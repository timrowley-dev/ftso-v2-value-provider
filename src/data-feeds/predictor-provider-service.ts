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
const PREFERRED_CURRENCY_PAIRS = process.env.PREFERRED_CURRENCY_PAIRS || "usdt"; // 'usdt', 'usdc', or 'both'

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
  quoteAsset?: "USDT" | "USDC";
}

interface PredictionResponse {
  [symbol: string]: {
    symbol: string;
    model: string;
    weighted_predicted_price: number;
    average_coefficient: number;
    trend: "UP" | "DOWN";
    exchange_count: number;
    total_samples: number;
    exchange_predictions: {
      [exchange: string]: {
        price: number;
        used_fallback: boolean;
        fallback_reason: string;
      };
    };
    analysis_timestamp: number;
    used_fallback: boolean;
    fallback_reason: string;
    converted_to_usd: boolean;
  };
}

const usdtToUsdFeedId: FeedId = { category: FeedCategory.Crypto.valueOf(), name: "USDT/USD" };
const usdcToUsdFeedId: FeedId = { category: FeedCategory.Crypto.valueOf(), name: "USDC/USD" };

export class PredictorFeed implements BaseDataFeed {
  private readonly logger = new Logger(PredictorFeed.name);
  protected initialized = false;
  private config: FeedConfig[];

  private readonly exchangeByName: Map<string, Exchange> = new Map();

  /** Symbol -> exchange -> price */
  private readonly prices: Map<string, Map<string, PriceInfo>> = new Map();

  // Track symbols per round
  private loggedSymbolsPerRound = new Map<number, Set<string>>();

  // Add at class level
  private readonly stablecoinRateCache = new Map<string, { rate: number; timestamp: number }>();
  private readonly CACHE_TTL_MS = 10000; // Increase to 10 seconds

  async start() {
    this.config = this.loadConfig();
    const exchangeToSymbols = new Map<string, Set<string>>();
    this.logger.log(`Predictor Config: HOST: ${process.env.PREDICTOR_HOST} PORT: ${process.env.PREDICTOR_PORT}`);

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
    const excludedFeedNames = [];

    const ccxtPrice = await this.getFeedPrice(feed, votingRoundId);

    // Early return for excluded feeds or voting round 0
    if (excludedFeedNames.includes(feed.name) || votingRoundId === 0) {
      return {
        feed: feed,
        value: ccxtPrice,
      };
    }

    // If predictor is enabled, fetch price asynchronously with timeout
    let predictorPrice: number | null = null;
    if (process.env.PREDICTOR_ENABLED === "true") {
      try {
        // Wrap predictor call in Promise.race with timeout
        const timeoutMs = 10000; // 10 second timeout
        predictorPrice = await Promise.race([
          this.getFeedPricePredictor(feed),
          new Promise<null>((_, reject) => setTimeout(() => reject(new Error("Predictor timeout")), timeoutMs)),
        ]);
      } catch (error) {
        this.logger.warn(`Predictor failed or timed out for ${feed.name}: ${error.message}`);
        predictorPrice = null;
      }
    }

    // Use predictor price if available, otherwise fall back to CCXT price
    const price = predictorPrice || ccxtPrice;

    this.logger.log(
      `[${feed.name}] Using ${predictorPrice ? "predictor" : "CCXT"} price: ${price} ` +
        `(CCXT: ${ccxtPrice}, Predictor: ${predictorPrice || "N/A"})`
    );

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
              quoteAsset: trade.symbol.endsWith("USDT") ? "USDT" : trade.symbol.endsWith("USDC") ? "USDC" : undefined,
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
      source: "spot",
      quoteAsset: symbol.endsWith("USDT") ? "USDT" : symbol.endsWith("USDC") ? "USDC" : undefined,
    });

    this.prices.set(symbol, prices);
    // this.logger.debug(
    //   `[${exchangeName}] ${symbol} Spot price updated: ${price} ` +
    //     `(volume: ${ticker.baseVolume || 0}, quote: ${symbol.endsWith("USDT") ? "USDT" : symbol.endsWith("USDC") ? "USDC" : "other"})`
    // );
  }

  private async getFeedPrice(
    feedId: FeedId,
    votingRoundId: number,
    skipStablecoinConversion: boolean = false,
    skipLogging: boolean = false
  ): Promise<number> {
    try {
      const symbol = feedId.name;
      const isStablecoin = symbol === "USDT/USD" || symbol === "USDC/USD";

      // Check cache first for stablecoins
      if (isStablecoin && !skipStablecoinConversion) {
        const cached = this.stablecoinRateCache.get(symbol);
        if (cached && Date.now() - cached.timestamp < this.CACHE_TTL_MS) {
          return cached.rate;
        }
      }

      if (!isStablecoin && !skipLogging) {
        this.logger.log(`\n${"=".repeat(50)}`);
        this.logger.log(`Starting price calculation for ${symbol}`);
        this.logger.log(`${"-".repeat(50)}`);
      }

      const config = this.config.find(config => feedsEqual(config.feed, feedId));
      if (!config) {
        this.logger.warn(`No config found for ${JSON.stringify(feedId)}`);
        return undefined;
      }

      // Track exchange statuses
      const exchangeStatuses = {
        total: config.sources.length,
        active: 0,
        stale: 0,
        noData: 0,
        invalidPrice: 0,
      };

      // Get raw prices and group by quote asset
      const pricesByQuote: { [quote: string]: PriceInfo[] } = {
        USDT: [],
        USDC: [],
        USD: [],
      };

      // Collect prices and group by quote asset
      for (const source of config.sources) {
        const prices = this.prices.get(source.symbol);
        if (!prices) {
          exchangeStatuses.noData++;
          this.logger.debug(`${source.exchange}: No price data found`);
          continue;
        }

        const info = prices.get(source.exchange);
        if (!info) {
          exchangeStatuses.noData++;
          this.logger.debug(`${source.exchange}: No exchange data found`);
          continue;
        }

        const staleness = info.source === "spot" ? 30 * 1000 : 5 * 60 * 1000;
        if (Date.now() - info.time > staleness) {
          exchangeStatuses.stale++;
          this.logger.debug(`${source.exchange}: Stale data (${Math.round((Date.now() - info.time) / 1000)}s old)`);
          continue;
        }

        if (!info.price || info.price <= 0) {
          exchangeStatuses.invalidPrice++;
          this.logger.debug(`${source.exchange}: Invalid price (${info.price})`);
          continue;
        }

        exchangeStatuses.active++;
        const quoteAsset = info.quoteAsset || "USD";
        pricesByQuote[quoteAsset].push(info);
      }

      // Log exchange status summary
      if (!skipLogging) {
        const statusMsg = [
          `Exchange Status for ${symbol}:`,
          `  Active: ${exchangeStatuses.active}/${exchangeStatuses.total}`,
          `  Stale: ${exchangeStatuses.stale}`,
          `  No Data: ${exchangeStatuses.noData}`,
          `  Invalid Price: ${exchangeStatuses.invalidPrice}`,
        ].join("\n");

        if (exchangeStatuses.active < exchangeStatuses.total * 0.5) {
          this.logger.warn(statusMsg);
        } else {
          this.logger.debug(statusMsg);
        }
      }

      // Get conversion rates once per quote asset
      const conversionRates: { [quote: string]: number } = {};
      if (!skipStablecoinConversion && !isStablecoin) {
        if (pricesByQuote.USDT.length > 0) {
          conversionRates.USDT = await this.getFeedPrice(usdtToUsdFeedId, votingRoundId, true, true);
        }
        if (pricesByQuote.USDC.length > 0) {
          conversionRates.USDC = await this.getFeedPrice(usdcToUsdFeedId, votingRoundId, true, true);
        }
        conversionRates.USD = 1;
      }

      // Convert all prices to USD
      const allPrices: PriceInfo[] = [];
      for (const [quote, prices] of Object.entries(pricesByQuote)) {
        if (prices.length === 0) continue;

        const convertedPrices = prices.map(info => ({
          ...info,
          price: info.price * (skipStablecoinConversion ? 1 : conversionRates[quote] || 1),
        }));
        allPrices.push(...convertedPrices);
      }

      // Apply outlier removal
      const filteredPrices = this.removeOutliers(allPrices, 3.0, 0.5, false);

      // Calculate final price
      const result =
        PRICE_CALCULATION_METHOD === "weighted-median"
          ? this.weightedMedian(filteredPrices, symbol)
          : filteredPrices.reduce((a, b) => a + b.price, 0) / filteredPrices.length;

      if (isStablecoin && !skipStablecoinConversion) {
        this.stablecoinRateCache.set(symbol, {
          rate: result,
          timestamp: Date.now(),
        });
      }

      if (!isStablecoin && !skipLogging) {
        this.logger.log(`Final price for ${symbol}: ${result}`);
        this.logger.log(`${"=".repeat(50)}\n`);
      }

      return result;
    } catch (error) {
      this.logger.error(`Error getting price for ${feedId.name}: ${error.message}`);
      return 0; // or other appropriate default/fallback value
    }
  }

  private weightedMedian(prices: PriceInfo[], symbol: string): number {
    if (prices.length === 0) {
      this.logger.warn(`No valid prices available for ${symbol}`);
      return 0; // or throw a specific error if you prefer
    }

    if (prices.length === 1) {
      this.logger.debug(
        `[${symbol || "UNKNOWN"}] Single price available, using: ${prices[0].price} from ${prices[0].exchange}`
      );
      return prices[0].price;
    }

    // Sort by price first
    prices.sort((a, b) => a.price - b.price);
    const now = Date.now();

    // Improve log formatting with proper indentation
    this.logger.debug(
      `[${symbol || "UNKNOWN"}] Processing ${prices.length} prices:\n    ` +
        prices.map(p => `${p.exchange}: ${p.price} (${p.source}, vol: ${p.volume})`).join("\n    ")
    );

    // Calculate weights
    const totalVolume = prices.reduce((sum, data) => sum + data.volume, 0);
    this.logger.debug(`Total volume: ${totalVolume}`);

    // Calculate and log weights in a single block
    const weights = prices.map(data => {
      const timeDifference = now - data.time;
      const timeWeight = Math.exp(-lambda * timeDifference);
      const volumeWeight = totalVolume > 0 ? data.volume / totalVolume : 1 / prices.length;
      const combinedWeight = timeWeight * 0.999 + volumeWeight * 0.001;

      this.logger.debug(
        `${data.exchange.padEnd(10)}: ` +
          `price=${data.price.toFixed(6)} ` +
          `weight=${combinedWeight.toFixed(4)} ` +
          `(time=${timeWeight.toFixed(4)}, vol=${volumeWeight.toFixed(4)})`
      );

      return combinedWeight;
    });

    // Normalize weights
    const weightSum = weights.reduce((sum, w) => sum + w, 0);
    const normalizedWeights = weights.map(w => w / weightSum);

    this.logger.debug("Final normalized weights:");
    prices.forEach((price, i) => {
      this.logger.debug(
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

      this.logger.debug(`Cumulative weight after ${prices[i].exchange}: ${cumulativeWeight.toFixed(4)}`);

      if (prevWeight <= midpoint && cumulativeWeight >= midpoint) {
        // If we're between two prices, interpolate
        if (i < prices.length - 1) {
          const leftPrice = prices[i].price;
          const rightPrice = prices[i + 1].price;
          const fraction = (midpoint - prevWeight) / normalizedWeights[i];
          const weightedPrice = leftPrice + (rightPrice - leftPrice) * fraction;

          // Update interpolation logging with proper indentation
          this.logger.debug(`    Interpolating between:`);
          this.logger.debug(`      ${prices[i].exchange}: ${leftPrice}`);
          this.logger.debug(`      ${prices[i + 1].exchange}: ${rightPrice}`);
          this.logger.debug(`      Fraction: ${fraction.toFixed(4)}`);
          this.logger.debug(`      Final weighted median: ${weightedPrice}`);

          // Update final result logging
          this.logger.debug(
            `    Selected median between:\n` +
              `      ${prices[i].exchange}: ${prices[i].price}\n` +
              `      ${prices[i + 1].exchange}: ${prices[i + 1].price}\n` +
              `    Final weighted median: ${weightedPrice} (interpolation: ${fraction.toFixed(4)})`
          );

          return weightedPrice;
        }

        this.logger.debug(`Using exact price from ${prices[i].exchange}: ${prices[i].price}`);
        return prices[i].price;
      }
    }

    // Fallback to middle price if something goes wrong
    const fallbackPrice = prices[Math.floor(prices.length / 2)].price;
    this.logger.debug(`Using fallback middle price: ${fallbackPrice}`);
    return fallbackPrice;
  }

  private async getFeedPricePredictor(feedId: FeedId): Promise<number> {
    const url = process.env.PREDICTOR_HOST || "http://localhost";
    const port = process.env.PREDICTOR_PORT || 8000;
    const baseSymbol = feedId.name.split("/")[0];
    const model = process.env.PREDICTOR_MODEL || "linear";
    const lookback = process.env.PREDICTOR_LOOKBACK || 60;
    const convertToUsd = process.env.PREDICTOR_CONVERT_TO_USD || true;
    const baseURL = `${url}${port ? `:${port}` : ""}/analyze`;

    this.logger.log(`PREDICTOR URL: ${baseURL}`);

    try {
      const request: AxiosResponse<PredictionResponse> = await axios.get(baseURL, {
        params: {
          symbols: baseSymbol,
          model: model,
          lookback: lookback,
          convert_to_usd: convertToUsd,
        },
        timeout: 15000,
      });
      if (request && request.data) {
        const prediction = request.data[baseSymbol];
        const price = prediction.weighted_predicted_price;
        if (price == 0 || price == null) return null;

        if (prediction.used_fallback) {
          this.logger.warn(`${baseSymbol} used fallback: ${prediction.fallback_reason}`);
        }
        this.logger.log(
          `baseSymbol: ${baseSymbol}, prediction: ${price}, model: ${model}, lookback: ${lookback}, convertToUsd: ${convertToUsd}, baseURL: ${baseURL}`
        );
        return price;
      }
    } catch (error) {
      if (axios.isAxiosError(error) && error.code === "ECONNABORTED") {
        this.logger.warn("Predictor request timed out after 15 seconds");
      } else {
        this.logger.error(error);
      }
      return null;
    }
    this.logger.debug(`Price from predictor was null`);
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
      let config: FeedConfig[] = JSON.parse(jsonString);

      // Filter sources based on PREFERRED_CURRENCY_PAIRS
      config = config.map(feed => ({
        ...feed,
        sources: feed.sources.filter(source => {
          // Don't filter the stablecoin feeds themselves
          if (feed.feed.name === "USDT/USD" || feed.feed.name === "USDC/USD") return true;

          if (PREFERRED_CURRENCY_PAIRS === "both") return true;
          if (PREFERRED_CURRENCY_PAIRS === "usdt") return source.symbol.endsWith("USDT");
          if (PREFERRED_CURRENCY_PAIRS === "usdc") return source.symbol.endsWith("USDC");
          return true;
        }),
      }));

      // Always validate both stablecoin feeds exist since we need them for conversion
      if (config.find(feed => feedsEqual(feed.feed, usdtToUsdFeedId)) === undefined) {
        throw new Error("Must provide USDT feed sources, as it is used for USD conversion.");
      }
      if (config.find(feed => feedsEqual(feed.feed, usdcToUsdFeedId)) === undefined) {
        throw new Error("Must provide USDC feed sources, as it is used for USD conversion.");
      }

      this.logger.log(`Supported feeds: ${JSON.stringify(config.map(f => f.feed))}`);
      return config;
    } catch (err) {
      this.logger.error("Error parsing JSON config:", err);
      throw err;
    }
  }

  private removeOutliers(
    prices: PriceInfo[],
    madThreshold: number = 3.0,
    minPercentThreshold: number = 0.5,
    skipLogging: boolean = false
  ): PriceInfo[] {
    if (prices.length < 3) return prices;

    const sortedPrices = [...prices].sort((a, b) => a.price - b.price);
    const median = sortedPrices[Math.floor(sortedPrices.length / 2)].price;

    // Calculate MAD without scaling factor
    const absoluteDeviations = prices.map(p => Math.abs(p.price - median));
    const mad = absoluteDeviations.sort((a, b) => a - b)[Math.floor(absoluteDeviations.length / 2)];

    // Calculate thresholds
    const madBasedThreshold = madThreshold * mad;
    const minThresholdDecimal = minPercentThreshold / 100;
    const minThreshold = median * minThresholdDecimal;
    const threshold = Math.max(madBasedThreshold, minThreshold);

    // Calculate the percentage threshold for logging
    const percentageThreshold = (threshold / median) * 100;

    if (!skipLogging) {
      this.logger.log(
        `Outlier detection: median price ${median.toFixed(4)}, threshold ±${percentageThreshold.toFixed(2)}% (minimum: ±${minPercentThreshold.toFixed(2)}%)`
      );
    }

    // Create a formatted price list with the correct threshold
    const priceList = prices.map(p => ({
      status: Math.abs(p.price - median) > threshold ? "OUTLIER" : "KEPT",
      exchange: p.exchange,
      price: p.price,
      deviationPercent: (Math.abs(p.price - median) / median) * 100,
    }));

    if (!skipLogging) {
      // Update debug logging with proper indentation
      this.logger.debug(`    MAD: ${mad}`);
      this.logger.debug(`    MAD-based threshold: ${madBasedThreshold}`);
      this.logger.debug(`    Min percent threshold (decimal): ${minThresholdDecimal}`);
      this.logger.debug(`    Minimum threshold (${minPercentThreshold}%): ${minThreshold}`);
      this.logger.debug(`    Final threshold: ${threshold}`);
      this.logger.debug(
        `    Acceptable range: ${(median - threshold).toFixed(8)} to ${(median + threshold).toFixed(8)}`
      );

      // Update price deviation logging
      this.logger.log(`    Price deviations from median:`);
      let currentStatus = "";
      priceList.forEach(p => {
        if (p.status !== currentStatus) {
          currentStatus = p.status;
          this.logger.log(`      ${currentStatus}:`);
        }
        this.logger.log(
          `        ${p.exchange.padEnd(10)} price ${p.price.toFixed(8)} (${p.deviationPercent.toFixed(2)}% from median)`
        );
      });
    }

    // Filter out prices more than threshold from median
    const filteredPrices = prices.filter(p => Math.abs(p.price - median) <= threshold);

    if (!skipLogging && filteredPrices.length < prices.length) {
      this.logger.log(
        `Summary: Removed ${prices.length - filteredPrices.length} of ${prices.length} prices ` +
          `(${(((prices.length - filteredPrices.length) / prices.length) * 100).toFixed(1)}%)`
      );
    }

    if (!skipLogging) {
      this.logger.log(`Price deviations from median:`);
      this.logger.log(`  KEPT:`);

      // Log kept prices
      priceList
        .filter(p => p.status === "KEPT")
        .forEach(p => {
          this.logger.log(
            `    ${p.exchange.padEnd(10)} price ${p.price.toFixed(8)} (${p.deviationPercent.toFixed(2)}% from median)`
          );
        });

      // Log outliers in white using warn level
      const outliers = priceList.filter(p => p.status === "OUTLIER");
      if (outliers.length > 0) {
        this.logger.warn(`  OUTLIER:`);
        outliers.forEach(p => {
          this.logger.warn(
            `    ${p.exchange.padEnd(10)} price ${p.price.toFixed(8)} (${p.deviationPercent.toFixed(2)}% from median)`
          );
        });
      }
    }

    return filteredPrices;
  }
}

function feedsEqual(a: FeedId, b: FeedId): boolean {
  return a.category === b.category && a.name === b.name;
}
