import { Logger } from "@nestjs/common";
import ccxt, { Exchange, Ticker } from "ccxt";
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
  source?: "trade" | "spot";
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
    void this.initWatchTrades(exchangeToSymbols);
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

  private async initWatchTrades(exchangeToSymbols: Map<string, Set<string>>) {
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
    this.logger.log(`Watching ${marketIds} on exchange ${exchangeName}`);

    // Get the correct market symbol format for each exchange
    const formattedMarketIds = marketIds.map(marketId => {
      const market = exchange.markets[marketId];
      return market ? market.id : marketId;
    });

    // Always start polling for spot prices if supported
    if (exchange.has["fetchTicker"]) {
      void this.pollTrades(exchange, formattedMarketIds, exchangeName);
    } else {
      this.logger.warn(`Exchange ${exchangeName} does not support fetchTicker, skipping spot price polling`);
    }

    // Optionally collect trades as backup if websocket is available
    if ("watchTradesForSymbols" in exchange) {
      void this.watchTrades(exchange, marketIds, exchangeName);
    }
  }

  private async watchTrades(exchange: Exchange, marketIds: string[], exchangeName: string) {
    const isRunning = true;
    while (isRunning) {
      try {
        const trades = await retry(async () => exchange.watchTradesForSymbols(marketIds, null, 100), RETRY_BACKOFF_MS);
        trades.forEach(trade => {
          const prices = this.prices.get(trade.symbol) || new Map<string, PriceInfo>();
          // Only update if we don't have a recent spot price
          const existingPrice = prices.get(exchangeName);
          if (!existingPrice || existingPrice.source !== "spot" || Date.now() - existingPrice.time > 60000) {
            prices.set(exchangeName, {
              price: trade.price,
              time: trade.timestamp || Date.now(),
              exchange: exchangeName,
              volume: trade.amount || 0,
              source: "trade",
            });
            this.prices.set(trade.symbol, prices);
          }
        });
      } catch (e) {
        this.logger.error(`Failed to watch trades for ${exchangeName}: ${e}`);
        await new Promise(resolve => setTimeout(resolve, RETRY_BACKOFF_MS));
      }
    }
  }

  private async pollTrades(exchange: Exchange, marketIds: string[], exchangeName: string) {
    const POLL_INTERVAL = 10000; // 10 seconds
    const isRunning = true;
    this.logger.log(`Starting spot price polling for ${exchangeName}`);

    while (isRunning) {
      try {
        // Try batch fetching first
        if (exchange.has["fetchTickers"]) {
          try {
            const tickers = await exchange.fetchTickers(marketIds);
            for (const [symbol, ticker] of Object.entries(tickers)) {
              if (ticker && (ticker.last || ticker.close)) {
                this.updateSpotPrice(symbol, ticker, exchangeName);
              }
            }
            continue; // Skip individual fetches if batch succeeded
          } catch (e) {
            this.logger.debug(
              `Batch ticker fetch failed for ${exchangeName}, falling back to individual: ${e.message}`
            );
          }
        }

        // Fall back to individual fetches
        for (const marketId of marketIds) {
          try {
            this.logger.debug(`Fetching spot price for ${marketId} on ${exchangeName}`);
            const ticker = await exchange.fetchTicker(marketId);
            if (ticker && (ticker.last || ticker.close)) {
              this.updateSpotPrice(marketId, ticker, exchangeName);
            }
          } catch (e) {
            this.logger.warn(`Failed to fetch ticker for ${marketId} on ${exchangeName}: ${e.message}`);
          }
        }
      } catch (e) {
        this.logger.warn(`Failed to poll data for ${exchangeName}: ${e}`);
      }

      await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL));
    }
  }

  private updateSpotPrice(symbol: string, ticker: Ticker, exchangeName: string) {
    const price = ticker.last || ticker.close;
    const prices = this.prices.get(symbol) || new Map<string, PriceInfo>();

    prices.set(exchangeName, {
      price: price,
      time: ticker.timestamp || Date.now(),
      exchange: exchangeName,
      volume: ticker.baseVolume || 0,
      source: "spot", // Explicitly mark as spot price
    });

    this.prices.set(symbol, prices);
    this.logger.debug(
      `[${exchangeName}] ${symbol} Spot price updated: ${price} ` + `(volume: ${ticker.baseVolume || 0})`
    );
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
    const activeExchanges = new Set<string>();
    const inactiveExchanges = new Map<string, string>(); // Track inactive exchanges and reasons

    for (const source of config.sources) {
      const prices = this.prices.get(source.symbol);
      if (!prices) {
        inactiveExchanges.set(source.exchange, "No price data received");
        continue;
      }

      const info = prices.get(source.exchange);
      if (info === undefined) {
        inactiveExchanges.set(source.exchange, "Exchange initialized but no recent trades");
        continue;
      }

      // Adjust stale data check based on source - prioritize spot data with shorter window
      const staleness = info.source === "spot" ? 30 * 1000 : 5 * 60 * 1000; // 30s for spot, 5m for trades
      if (Date.now() - info.time > staleness) {
        inactiveExchanges.set(
          source.exchange,
          `Stale ${info.source} data (${Math.round((Date.now() - info.time) / 1000)}s old)`
        );
        continue;
      }

      activeExchanges.add(source.exchange);

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

    // Enhanced logging
    this.logger.log(
      `${feedId.name} - Exchange status:
       Active (${activeExchanges.size}): ${Array.from(activeExchanges).join(", ")}
       Inactive (${inactiveExchanges.size}): ${Array.from(inactiveExchanges.entries())
         .map(([exchange, reason]) => `${exchange} (${reason})`)
         .join(", ")}`
    );

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

    if (prices.length === 1) {
      this.logger.debug(`Single price available, using: ${prices[0].price} from ${prices[0].exchange}`);
      return prices[0].price;
    }

    // First pass: Remove extreme outliers (e.g. >10% from trimmed mean)
    const trimmedPrices = [...prices].sort((a, b) => a.price - b.price);
    const trimAmount = Math.floor(prices.length * 0.1); // 10% trim
    const trimmedMean =
      trimmedPrices.slice(trimAmount, -trimAmount).reduce((sum, p) => sum + p.price, 0) /
      (prices.length - 2 * trimAmount);

    prices = prices.filter(p => {
      const deviation = Math.abs(p.price - trimmedMean) / trimmedMean;
      if (deviation > 0.1) {
        // 10% threshold
        this.logger.debug(
          `Removing extreme outlier: ${p.exchange} (${p.price}, ${(deviation * 100).toFixed(2)}% from mean)`
        );
        return false;
      }
      return true;
    });

    // Sort remaining prices
    prices.sort((a, b) => a.price - b.price);
    const now = Date.now();

    // Calculate weights with adjusted formula
    const totalVolume = prices.reduce((sum, data) => sum + data.volume, 0);
    const weights = prices.map(data => {
      const timeDifference = now - data.time;
      // Exponential decay with more aggressive time penalty
      const timeWeight = Math.exp(-lambda * Math.pow(timeDifference, 1.2));

      // Volume weight with diminishing returns
      const volumeWeight = Math.log1p(data.volume) / Math.log1p(totalVolume);

      // Add source reliability weight
      const sourceWeight = data.source === "spot" ? 1.0 : 0.8;

      // Combine weights with adjusted ratios
      return timeWeight * 0.7 + volumeWeight * 0.2 + sourceWeight * 0.1;
    });

    // Normalize weights
    const weightSum = weights.reduce((sum, w) => sum + w, 0);
    const normalizedWeights = weights.map(w => w / weightSum);

    this.logger.log("Final normalized weights:");
    prices.forEach((price, i) => {
      this.logger.log(
        `  ${price.exchange}: ${normalizedWeights[i].toFixed(4)} ` +
          `(price: ${price.price}, source: ${price.source || "websocket"})`
      );
    });

    // Calculate cumulative weights and find median
    let cumulativeWeight = 0;
    const midpoint = 0.5;

    for (let i = 0; i < prices.length; i++) {
      const prevWeight = cumulativeWeight;
      cumulativeWeight += normalizedWeights[i];

      this.logger.log(`Cumulative weight after ${prices[i].exchange}: ${cumulativeWeight.toFixed(4)}`);

      if (prevWeight <= midpoint && cumulativeWeight >= midpoint) {
        // If we're between two prices, interpolate
        if (i < prices.length - 1) {
          const leftPrice = prices[i].price;
          const rightPrice = prices[i + 1].price;
          const fraction = (midpoint - prevWeight) / normalizedWeights[i];
          const weightedPrice = leftPrice + (rightPrice - leftPrice) * fraction;

          this.logger.log(`Interpolating between:`);
          this.logger.log(`  ${prices[i].exchange}: ${leftPrice}`);
          this.logger.log(`  ${prices[i + 1].exchange}: ${rightPrice}`);
          this.logger.log(`  Fraction: ${fraction.toFixed(4)}`);
          this.logger.log(`  Final weighted median: ${weightedPrice}`);

          return weightedPrice;
        }

        this.logger.log(`Using exact price from ${prices[i].exchange}: ${prices[i].price}`);
        return prices[i].price;
      }
    }

    // Fallback to middle price if something goes wrong
    const fallbackPrice = prices[Math.floor(prices.length / 2)].price;
    this.logger.log(`Using fallback middle price: ${fallbackPrice}`);
    return fallbackPrice;
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
