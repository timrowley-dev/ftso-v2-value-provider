import { Logger } from "@nestjs/common";
import ccxt, { Exchange } from "ccxt";
import { readFileSync } from "fs";
import { FeedId, FeedValueData } from "../dto/provider-requests.dto";
import { BaseDataFeed } from "./base-feed";
import { retry } from "src/utils/retry";
import axios, { AxiosResponse } from "axios";
import * as dotenv from "dotenv";
dotenv.config();

type networks = "local-test" | "from-env" | "coston2" | "coston" | "songbird";

enum FeedCategory {
  None = 0,
  Crypto = 1,
  FX = 2,
  Commodity = 3,
  Stock = 4,
}

const CONFIG_PREFIX = "src/config/";
const RETRY_BACKOFF_MS = 10_000;

interface FeedConfig {
  feed: FeedId;
  sources: {
    exchange: string;
    symbol: string;
  }[];
}

interface PriceInfo {
  price: number;
  time: number;
  exchange: string;
}

interface PredictionResponse {
  prediction: number | null;
  seconds_remaining: number;
}

const usdtToUsdFeedId: FeedId = { category: FeedCategory.Crypto.valueOf(), name: "USDT/USD" };

export class PredictorFeed implements BaseDataFeed {
  private readonly logger = new Logger(PredictorFeed.name);
  protected initialized = false;
  private config: FeedConfig[];

  private readonly exchangeByName: Map<string, Exchange> = new Map();

  /** Symbol -> exchange -> price */
  private readonly prices: Map<string, Map<string, PriceInfo>> = new Map();

  async start() {
    this.config = this.loadConfig();
    const exchangeToSymbols = new Map<string, Set<string>>();

    for (const feed of this.config) {
      for (const source of feed.sources) {
        const symbols = exchangeToSymbols.get(source.exchange) || new Set();
        symbols.add(source.symbol);
        exchangeToSymbols.set(source.exchange, symbols);
      }
    }

    this.logger.log(`Connecting to exchanges: ${JSON.stringify(Array.from(exchangeToSymbols.keys()))}`);
    const loadExchanges = [];
    for (const exchangeName of exchangeToSymbols.keys()) {
      try {
        const exchange: Exchange = new ccxt.pro[exchangeName]({ newUpdates: true });
        this.exchangeByName.set(exchangeName, exchange);
        loadExchanges.push([exchangeName, retry(async () => exchange.loadMarkets(), 2, RETRY_BACKOFF_MS, this.logger)]);
      } catch (e) {
        this.logger.warn(`Failed to initialize exchange ${exchangeName}, ignoring: ${e}`);
        exchangeToSymbols.delete(exchangeName);
      }
    }

    for (const [exchangeName, loadExchange] of loadExchanges) {
      try {
        await loadExchange;
        this.logger.log(`Exchange ${exchangeName} initialized`);
      } catch (e) {
        this.logger.warn(`Failed to load markets for ${exchangeName}, ignoring: ${e}`);
        exchangeToSymbols.delete(exchangeName);
      }
    }
    this.initialized = true;

    this.logger.log(`Initialization done, watching trades...`);
    void this.watchTrades(exchangeToSymbols);
  }

  async getValues(feeds: FeedId[], votingRoundId: number): Promise<FeedValueData[]> {
    const results: FeedValueData[] = [];
    for (const feed of feeds) {
      const value = await this.getValue(feed, votingRoundId);
      results.push(value);
    }
    return results;
  }

  async getValue(feed: FeedId, votingRoundId: number): Promise<FeedValueData> {
    let price: number;

    if ([].includes(feed.name) || votingRoundId === 0) {
      price = await this.getFeedPrice(feed, votingRoundId);
      this.logger.log(`CCXT (ONLY) PRICE: [${feed.name}] ${price}`);
    } else {
      const ccxtPrice = await this.getFeedPrice(feed, votingRoundId);
      let predictorPrice: number | null = null;

      if (process.env.PREDICTOR_ENABLED === "true") {
        predictorPrice = await this.getFeedPricePredictor(feed, votingRoundId);
      }

      price = predictorPrice || ccxtPrice;
      this.logger.log(
        `[${feed.name}] Using ${predictorPrice ? 'predictor' : 'CCXT'} price: ${price} ` +
        `(CCXT: ${ccxtPrice}, Predictor: ${predictorPrice || 'N/A'})`
      );
    }

    return {
      feed: feed,
      value: price,
    };
  }

  private async watchTrades(exchangeToSymbols: Map<string, Set<string>>) {
    for (const [exchangeName, symbols] of exchangeToSymbols) {
      const exchange = this.exchangeByName.get(exchangeName);
      if (exchange === undefined) continue;

      const marketIds: string[] = [];
      for (const symbol of symbols) {
        const market = exchange.markets[symbol];
        if (market === undefined) {
          this.logger.warn(`Market not found for ${symbol} on ${exchangeName}`);
          continue;
        }
        marketIds.push(market.id);
      }
      void this.watch(exchange, marketIds, exchangeName);
    }
  }

  private async watch(exchange: Exchange, marketIds: string[], exchangeName: string) {
    this.logger.log(`Watching trades for ${marketIds} on exchange ${exchangeName}`);

    // eslint-disable-next-line no-constant-condition
    while (true) {
      try {
        const trades = await retry(async () => exchange.watchTradesForSymbols(marketIds, null, 100), RETRY_BACKOFF_MS);
        trades.forEach(trade => {
          const prices = this.prices.get(trade.symbol) || new Map<string, PriceInfo>();
          prices.set(exchangeName, { price: trade.price, time: trade.timestamp, exchange: exchangeName });
          this.prices.set(trade.symbol, prices);
        });
      } catch (e) {
        this.logger.error(`Failed to watch trades for ${exchangeName}: ${e}`);
        return;
      }
    }
  }

  private async getFeedPrice(feedId: FeedId, votingRoundId: number): Promise<number> {
    const config = this.config.find(config => feedsEqual(config.feed, feedId));
    if (!config) {
      this.logger.warn(`No config found for ${JSON.stringify(feedId)}`);
      return undefined;
    }

    let usdtToUsd = undefined;
    const prices: PriceInfo[] = [];
    const now = Date.now();

    for (const source of config.sources) {
      const info = this.prices.get(source.symbol)?.get(source.exchange);
      if (info === undefined) continue;

      let price = info.price;
      if (source.symbol.endsWith("USDT")) {
        if (usdtToUsd === undefined) usdtToUsd = await this.getFeedPrice(usdtToUsdFeedId, votingRoundId);
        price = price * usdtToUsd;
      }

      prices.push({
        price: price,
        time: info.time,
        exchange: info.exchange
      });
    }

    if (prices.length === 0) {
      this.logger.warn(`No prices found for ${JSON.stringify(feedId)}`);
      return undefined;
    }

    return this.getEnhancedPrice(prices);
  }

  private getEnhancedPrice(prices: PriceInfo[]): number {
    if (prices.length === 0) {
      throw new Error("Price list cannot be empty.");
    }

    // Step 1: Remove stale prices (older than 5 minutes)
    const now = Date.now();
    const MAX_STALENESS = 5 * 60 * 1000;
    let filteredPrices = prices.filter(p => (now - p.time) <= MAX_STALENESS);

    if (filteredPrices.length === 0) {
      const mostRecentTime = Math.max(...prices.map(p => p.time));
      filteredPrices = prices.filter(p => p.time === mostRecentTime);
    }

    // Step 2: Remove outliers using IQR method
    const values = filteredPrices.map(p => p.price).sort((a, b) => a - b);
    const q1 = this.getQuantile(values, 0.25);
    const q3 = this.getQuantile(values, 0.75);
    const iqr = q3 - q1;
    const lowerBound = q1 - 1.5 * iqr;
    const upperBound = q3 + 1.5 * iqr;

    filteredPrices = filteredPrices.filter(p => 
      p.price >= lowerBound && p.price <= upperBound
    );

    this.logger.warn(`[Enhanced] Removed ${prices.length - filteredPrices.length} outliers. Range: ${lowerBound}-${upperBound}`);

    // Step 3: Calculate confidence-weighted scores
    const lambda = process.env.MEDIAN_DECAY ? parseFloat(process.env.MEDIAN_DECAY) : 0.00005;
    const priceScores = filteredPrices.map(data => {
      const timeWeight = Math.exp(-lambda * (now - data.time));
      const exchangeWeight = this.getExchangeReliability(data.exchange);
      
      const mean = this.calculateMean(filteredPrices.map(p => p.price));
      const std = this.calculateStandardDeviation(filteredPrices.map(p => p.price), mean);
      const deviationWeight = std === 0 ? 1 : Math.exp(-Math.pow(data.price - mean, 2) / (2 * Math.pow(std, 2)));

      const totalWeight = timeWeight * exchangeWeight * deviationWeight;

      return {
        price: data.price,
        weight: totalWeight,
        exchange: data.exchange,
        staleness: now - data.time
      };
    });

    priceScores.sort((a, b) => a.price - b.price);

    // Log detailed price information
    this.logger.log("Enhanced weighted prices:");
    priceScores.forEach(({ price, weight, exchange, staleness }) => {
      this.logger.log(
        `Price: ${price}, Weight: ${weight.toFixed(4)}, ` +
        `Exchange: ${exchange}, Staleness: ${staleness}ms`
      );
    });

    // Calculate weighted median
    let cumulativeWeight = 0;
    const totalWeight = priceScores.reduce((sum, { weight }) => sum + weight, 0);

    for (const score of priceScores) {
      cumulativeWeight += score.weight / totalWeight;
      if (cumulativeWeight >= 0.5) {
        this.logger.log(`Final enhanced weighted median: ${score.price}`);
        return score.price;
      }
    }

    this.logger.warn("Unable to calculate enhanced weighted median");
    return undefined;
  }

  // Helper methods needed for enhanced price calculation
  private getQuantile(sortedValues: number[], q: number): number {
    const pos = (sortedValues.length - 1) * q;
    const base = Math.floor(pos);
    const rest = pos - base;
    if (sortedValues[base + 1] !== undefined) {
      return sortedValues[base] + rest * (sortedValues[base + 1] - sortedValues[base]);
    } else {
      return sortedValues[base];
    }
  }

  private calculateMean(values: number[]): number {
    return values.reduce((sum, val) => sum + val, 0) / values.length;
  }

  private calculateStandardDeviation(values: number[], mean: number): number {
    const squareDiffs = values.map(value => Math.pow(value - mean, 2));
    return Math.sqrt(squareDiffs.reduce((sum, diff) => sum + diff, 0) / values.length);
  }

  private getExchangeReliability(exchange: string): number {
    const reliabilityScores = {
      'binance': 1.0,
      'coinbase': 0.95,
      'kraken': 0.9,
      'default': 0.8
    };
    return reliabilityScores[exchange] || reliabilityScores.default;
  }

  private async getFeedPricePredictor(feedId: FeedId, votingRound: number): Promise<number> {
    const baseSymbol = feedId.name.split("/")[0];
    const axiosURL = `http://${process.env.PREDICTOR_HOST}:${process.env.PREDICTOR_PORT}/GetPrediction/${baseSymbol}/${votingRound}/2000`;
    this.logger.debug(`axios URL ${axiosURL}`);
    try {
      const request: AxiosResponse<PredictionResponse> = await axios.get(axiosURL, { timeout: 15000 });
      if (request && request.data) {
        const prediction = request.data.prediction;
        if (prediction == 0) return null;
        this.logger.debug(`Price from pred: ${prediction}`);
        return prediction;
      }
    } catch (error) {
      if (axios.isAxiosError(error) && error.code === "ECONNABORTED") {
        this.logger.warn("Predictor request timed out after 15 seconds");
      } else {
        this.logger.error(error);
      }
      return null;
    }
    this.logger.debug(`Price from pred was null`);
    return null;
  }

  private loadConfig() {
    const network = process.env.NETWORK as networks;
    let configPath: string;
    switch (network) {
      case "local-test":
        configPath = CONFIG_PREFIX + "test-feeds.json";
        break;
      default:
        configPath = CONFIG_PREFIX + "feeds.json";
    }

    try {
      const jsonString = readFileSync(configPath, "utf-8");
      const config: FeedConfig[] = JSON.parse(jsonString);

      if (config.find(feed => feedsEqual(feed.feed, usdtToUsdFeedId)) === undefined) {
        throw new Error("Must provide USDT feed sources, as it is used for USD conversion.");
      }

      this.logger.log(`Supported feeds: ${JSON.stringify(config.map(f => f.feed))}`);

      return config;
    } catch (err) {
      this.logger.error("Error parsing JSON config:", err);
      throw err;
    }
  }
}

function feedsEqual(a: FeedId, b: FeedId): boolean {
  return a.category === b.category && a.name === b.name;
}