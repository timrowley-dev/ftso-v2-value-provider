import { Logger } from "@nestjs/common";
import ccxt, { Exchange } from "ccxt";
import { readFileSync } from "fs";
import { FeedId, FeedValueData } from "../dto/provider-requests.dto";
import { BaseDataFeed } from "./base-feed";
import { retry } from "src/utils/retry";
import axios, { AxiosResponse } from "axios";
import * as dotenv from "dotenv";
dotenv.config();

// Environment variables interface
interface EnvConfig {
  NETWORK: networks;
  PREDICTOR_HOST: string;
  PREDICTOR_PORT: string;
  PREDICTOR_ENABLED: string;
}

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
const PREDICTOR_TIMEOUT_MS = 15_000;
const PREDICTOR_MAX_RETRIES = 3;

const CCXT_ONLY_SYMBOLS = ["SHIB/USD", "BONK/USD", "LINK/USD", "WIF/USD", "PEPE/USD", "ETH/USD"];

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

/**
 * PredictorFeed class implements BaseDataFeed interface and provides price feed functionality
 * combining CCXT exchange data with predictor service data
 */
export class PredictorFeed implements BaseDataFeed {
  private readonly logger = new Logger(PredictorFeed.name);
  protected initialized = false;
  private config: FeedConfig[];

  private readonly exchangeByName: Map<string, Exchange> = new Map();

  /** Symbol -> exchange -> price */
  private readonly prices: Map<string, Map<string, PriceInfo>> = new Map();

  /**
   * Initializes the feed service by loading config and connecting to exchanges
   */
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

  /**
   * Gets values for multiple feeds
   * @param feeds Array of FeedId to get values for
   * @param votingRoundId Current voting round ID
   */
  async getValues(feeds: FeedId[], votingRoundId: number): Promise<FeedValueData[]> {
    const promises = feeds.map(feed => this.getValue(feed, votingRoundId));
    return Promise.all(promises);
  }

  /**
   * Gets value for a single feed
   * @param feed FeedId to get value for
   * @param votingRoundId Current voting round ID
   */
  async getValue(feed: FeedId, votingRoundId: number): Promise<FeedValueData> {
    let price: number, ccxtPrice: number, predictorPrice: number;
    
    // Always get CCXT price first
    ccxtPrice = await this.getFeedPrice(feed, votingRoundId);
    if (ccxtPrice === undefined) {
        throw new Error(`Unable to get CCXT price for ${feed.name}`);
    }
    price = ccxtPrice;

    // Only get predictor price if enabled AND not in CCXT_ONLY_SYMBOLS
    if (process.env.PREDICTOR_ENABLED === "true" && !CCXT_ONLY_SYMBOLS.includes(feed.name)) {
        try {
            predictorPrice = await this.getFeedPricePredictor(feed, votingRoundId);
            if (predictorPrice) {
                this.logger.log(`Using predictor price for ${feed.name}: ${predictorPrice}`);
                price = predictorPrice;
            }
        } catch (error) {
            this.logger.warn(`Failed to get predictor price for ${feed.name}, using CCXT price: ${error.message}`);
        }
    }

    this.logger.log(`Final price for [${feed.name}]: ${price} (Source: ${predictorPrice ? 'Predictor' : 'CCXT'})`);

    return {
        feed: feed,
        value: price,
    };
  }

  /**
   * Watches trades for specified exchanges and symbols
   * @param exchangeToSymbols Map of exchange names to sets of symbols
   */
  private async watchTrades(exchangeToSymbols: Map<string, Set<string>>) {
    for (const [exchangeName, symbols] of exchangeToSymbols) {
      const exchange = this.exchangeByName.get(exchangeName);
      if (exchange === undefined) {
        this.logger.warn(`Exchange ${exchangeName} not found, skipping`);
        continue;
      }

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

  /**
   * Watches trades for a specific exchange
   * @param exchange CCXT exchange instance
   * @param marketIds Array of market IDs to watch
   * @param exchangeName Name of the exchange
   */
  private async watch(exchange: Exchange, marketIds: string[], exchangeName: string) {
    this.logger.log(`Watching trades for ${marketIds} on exchange ${exchangeName}`);

    // eslint-disable-next-line no-constant-condition
    while (true) {
      try {
        const trades = await retry(
          async () => exchange.watchTradesForSymbols(marketIds, null, 100),
          RETRY_BACKOFF_MS
        );
        trades.forEach(trade => {
          const prices = this.prices.get(trade.symbol) || new Map<string, PriceInfo>();
          prices.set(exchangeName, { price: trade.price, time: trade.timestamp, exchange: exchangeName });
          this.prices.set(trade.symbol, prices);
        });
      } catch (e) {
        this.logger.error(`Failed to watch trades for ${exchangeName}: ${e}`);
        // Add retry logic with exponential backoff
        await new Promise(resolve => setTimeout(resolve, RETRY_BACKOFF_MS));
        this.logger.log(`Attempting to reconnect to ${exchangeName}...`);
        continue;
      }
    }
  }

  /**
   * Gets price from predictor service with retry logic
   * @param feedId Feed identifier
   * @param votingRound Voting round ID
   */
  private async getFeedPricePredictor(feedId: FeedId, votingRound: number): Promise<number> {
    if (!feedId?.name) {
      this.logger.error('Invalid feedId provided to getFeedPricePredictor');
      return null;
    }

    if (!this.validatePredictorEnvVars()) {
      return null;
    }

    const baseSymbol = feedId.name.split("/")[0];
    const axiosURL = `http://${process.env.PREDICTOR_HOST}:${process.env.PREDICTOR_PORT}/GetPrediction/${baseSymbol}/${votingRound}/2000`;
    
    return retry(
      async () => {
        try {
          const request: AxiosResponse<PredictionResponse> = await axios.get(axiosURL, { 
            timeout: PREDICTOR_TIMEOUT_MS 
          });
          
          if (request?.data?.prediction) {
            const prediction = request.data.prediction / 100000;
            if (prediction === 0) return null;
            this.logger.debug(`Price from predictor: ${prediction}`);
            return prediction;
          }
          return null;
        } catch (error) {
          if (axios.isAxiosError(error) && error.code === "ECONNABORTED") {
            this.logger.warn("Predictor request timed out");
          } else {
            this.logger.error(`Predictor request failed: ${error.message}`);
          }
          throw error; // Throw error to trigger retry
        }
      },
      PREDICTOR_MAX_RETRIES,
      RETRY_BACKOFF_MS,
      this.logger
    );
  }

  /**
   * Validates predictor service environment variables
   */
  private validatePredictorEnvVars(): boolean {
    const requiredVars = ['PREDICTOR_HOST', 'PREDICTOR_PORT'];
    const missingVars = requiredVars.filter(varName => !process.env[varName]);
    
    if (missingVars.length > 0) {
      this.logger.error(`Missing required environment variables: ${missingVars.join(', ')}`);
      return false;
    }
    return true;
  }

  private async getFeedPrice(feedId: FeedId, votingRoundId: number): Promise<number> {
    const config = this.config.find(config => feedsEqual(config.feed, feedId));
    if (!config) {
      this.logger.warn(`No config found for ${JSON.stringify(feedId)}`);
      return undefined;
    }

    const prices: number[] = [];

    let usdtToUsd = undefined;

    for (const source of config.sources) {
      const info = this.prices.get(source.symbol)?.get(source.exchange);
      if (info === undefined) continue;

      if (source.symbol.endsWith("USDT")) {
        if (usdtToUsd === undefined) usdtToUsd = await this.getFeedPrice(usdtToUsdFeedId, votingRoundId);
        prices.push(info.price * usdtToUsd);
      } else {
        prices.push(info.price);
      }
    }

    if (prices.length === 0) {
      this.logger.warn(`No prices found for ${JSON.stringify(feedId)}`);
      return undefined;
    }

    const result = prices.reduce((a, b) => a + b, 0) / prices.length;
    return result;
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

/**
 * Compares two FeedIds for equality
 * @param a First FeedId
 * @param b Second FeedId
 */
function feedsEqual(a: FeedId, b: FeedId): boolean {
  return a.category === b.category && a.name === b.name;
}
