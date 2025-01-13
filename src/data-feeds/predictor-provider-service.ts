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
const PRICE_CALCULATION_METHOD = process.env.PRICE_CALCULATION_METHOD || "weighted-median"; // 'average' or 'weighted-median'
const lambda = process.env.MEDIAN_DECAY ? parseFloat(process.env.MEDIAN_DECAY) : 0.00005;

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
  volume: number;
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
        // Try to initialize with CCXT Pro first, fall back to regular CCXT
        let exchange: Exchange;
        try {
          exchange = new ccxt.pro[exchangeName]({ newUpdates: true });
          this.logger.debug(`Using CCXT Pro for ${exchangeName}`);
        } catch (e) {
          // If Pro initialization fails, try regular CCXT
          exchange = new ccxt[exchangeName]({ newUpdates: true });
          this.logger.debug(`Falling back to regular CCXT for ${exchangeName}`);
        }

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
        `[${feed.name}] Using ${predictorPrice ? "predictor" : "CCXT"} price: ${price} ` +
          `(CCXT: ${ccxtPrice}, Predictor: ${predictorPrice || "N/A"})`
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

    // Check if exchange supports websocket
    if (!("watchTradesForSymbols" in exchange)) {
      this.logger.debug(`Exchange ${exchangeName} doesn't support websocket, using REST polling`);
      void this.pollTrades(exchange, marketIds, exchangeName);
      return;
    }

    // eslint-disable-next-line no-constant-condition
    while (true) {
      try {
        const trades = await retry(async () => exchange.watchTradesForSymbols(marketIds, null, 100), RETRY_BACKOFF_MS);
        trades.forEach(trade => {
          const prices = this.prices.get(trade.symbol) || new Map<string, PriceInfo>();
          prices.set(exchangeName, {
            price: trade.price,
            time: trade.timestamp,
            exchange: exchangeName,
            volume: trade.amount || 0,
          });
          this.prices.set(trade.symbol, prices);
        });
      } catch (e) {
        this.logger.error(`Failed to watch trades for ${exchangeName}: ${e}`);
        return;
      }
    }
  }

  private async pollTrades(exchange: Exchange, marketIds: string[], exchangeName: string) {
    const POLL_INTERVAL = 10000; // 10 seconds

    // eslint-disable-next-line no-constant-condition
    while (true) {
      try {
        for (const marketId of marketIds) {
          const trades = await retry(async () => exchange.fetchTrades(marketId), RETRY_BACKOFF_MS);
          if (trades.length > 0) {
            const lastTrade = trades[trades.length - 1];
            const prices = this.prices.get(lastTrade.symbol) || new Map<string, PriceInfo>();
            prices.set(exchangeName, {
              price: lastTrade.price,
              time: lastTrade.timestamp,
              exchange: exchangeName,
              volume: lastTrade.amount || 0,
            });
            this.prices.set(lastTrade.symbol, prices);
          }
        }
      } catch (e) {
        this.logger.warn(`Failed to poll trades for ${exchangeName}: ${e}`);
      }

      await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL));
    }
  }

  private async getFeedPrice(feedId: FeedId, votingRoundId: number): Promise<number> {
    // Add fallback price for USDX
    if (feedId.name === "USDX/USD") {
      return 0.9999;
    }

    const config = this.config.find(config => feedsEqual(config.feed, feedId));
    if (!config) {
      this.logger.warn(`No config found for ${JSON.stringify(feedId)}`);
      return undefined;
    }

    const priceInfos: PriceInfo[] = [];
    let usdtToUsd = undefined;

    for (const source of config.sources) {
      const info = this.prices.get(source.symbol)?.get(source.exchange);
      if (info === undefined) continue;

      if (source.symbol.endsWith("USDT")) {
        if (usdtToUsd === undefined) usdtToUsd = await this.getFeedPrice(usdtToUsdFeedId, votingRoundId);
        priceInfos.push({
          price: info.price * usdtToUsd,
          time: info.time,
          exchange: info.exchange,
          volume: info.volume,
        });
      } else {
        priceInfos.push(info);
      }
    }

    if (priceInfos.length === 0) {
      this.logger.warn(`No prices found for ${JSON.stringify(feedId)}`);
      return undefined;
    }

    // Apply outlier removal
    const filteredPrices = this.removeOutliers(priceInfos);

    // Use selected calculation method
    if (PRICE_CALCULATION_METHOD === "weighted-median") {
      return this.weightedMedian(filteredPrices);
    } else {
      return filteredPrices.reduce((a, b) => a + b.price, 0) / filteredPrices.length;
    }
  }

  private weightedMedian(prices: PriceInfo[]): number {
    if (prices.length === 0) {
      throw new Error("Price list cannot be empty.");
    }

    // If there's only one price, return it directly
    if (prices.length === 1) {
      this.logger.debug(`Single price available, using: ${prices[0].price} from ${prices[0].exchange}`);
      return prices[0].price;
    }

    prices.sort((a, b) => a.time - b.time);
    const now = Date.now();

    // Calculate total volume
    const totalVolume = prices.reduce((sum, data) => sum + data.volume, 0);

    // Calculate combined weights using both time and volume
    const weights = prices.map(data => {
      const timeDifference = now - data.time;
      const timeWeight = Math.exp(-lambda * timeDifference);
      const volumeWeight = totalVolume > 0 ? data.volume / totalVolume : 1 / prices.length;

      // Combine weights (you can adjust the balance between time and volume)
      return timeWeight * 0.7 + volumeWeight * 0.3; // 70% time, 30% volume weight
    });

    // Normalize weights
    const weightSum = weights.reduce((sum, weight) => sum + weight, 0);
    const normalizedWeights = weights.map(weight => weight / weightSum);

    // Combine prices and weights
    const weightedPrices = prices.map((data, i) => ({
      price: data.price,
      weight: normalizedWeights[i],
      exchange: data.exchange,
      staleness: now - data.time,
    }));

    // Sort by price for median calculation
    weightedPrices.sort((a, b) => a.price - b.price);

    this.logger.debug("Weighted prices:");
    for (const { price, weight, exchange, staleness } of weightedPrices) {
      this.logger.debug(`Price: ${price}, weight: ${weight}, staleness ms: ${staleness}, exchange: ${exchange}`);
    }

    // Add debug logging
    this.logger.debug("Volume weights:");
    prices.forEach(data => {
      const volumeWeight = totalVolume > 0 ? data.volume / totalVolume : 1 / prices.length;
      this.logger.debug(`Exchange: ${data.exchange}, Volume: ${data.volume}, Weight: ${volumeWeight}`);
    });

    // Find weighted median
    let cumulativeWeight = 0;
    for (let i = 0; i < weightedPrices.length; i++) {
      cumulativeWeight += weightedPrices[i].weight;
      if (cumulativeWeight >= 0.5) {
        this.logger.debug(`Weighted median: ${weightedPrices[i].price}`);
        return weightedPrices[i].price;
      }
    }

    this.logger.warn("Unable to calculate weighted median");
    return undefined;
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

  private removeOutliers(prices: PriceInfo[], sigmas: number = 2): PriceInfo[] {
    // Need at least 3 prices for meaningful outlier detection
    if (prices.length < 3) return prices;

    // Calculate mean
    const mean = prices.reduce((sum, p) => sum + p.price, 0) / prices.length;

    // Calculate standard deviation
    const squaredDiffs = prices.map(p => Math.pow(p.price - mean, 2));
    const variance = squaredDiffs.reduce((sum, diff) => sum + diff, 0) / prices.length;
    const stdDev = Math.sqrt(variance);

    // Filter out prices more than N standard deviations from mean
    const threshold = sigmas * stdDev;
    const filteredPrices = prices.filter(p => Math.abs(p.price - mean) <= threshold);

    if (filteredPrices.length < prices.length) {
      this.logger.debug(`Removed ${prices.length - filteredPrices.length} outliers using ${sigmas}-sigma rule`);
      this.logger.debug(`Original prices: ${prices.map(p => `${p.price} (${p.exchange})`).join(", ")}`);
      this.logger.debug(`Filtered prices: ${filteredPrices.map(p => `${p.price} (${p.exchange})`).join(", ")}`);
    }

    return filteredPrices;
  }
}

function feedsEqual(a: FeedId, b: FeedId): boolean {
  return a.category === b.category && a.name === b.name;
}
