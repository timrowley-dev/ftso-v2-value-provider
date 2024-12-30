import { Logger } from "@nestjs/common";
import ccxt, { Exchange, Trade } from "ccxt";
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

enum PricingMethod {
  WEIGHTED = 'weighted',
  ENHANCED = 'enhanced',
  // Add more methods here as needed
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
  type?: 'USDT' | 'USDC' | 'USD';
}

interface PredictionResponse {
  prediction: number | null;
  seconds_remaining: number;
}

const usdtToUsdFeedId: FeedId = { category: FeedCategory.Crypto.valueOf(), name: "USDT/USD" };
const usdcToUsdFeedId: FeedId = { category: FeedCategory.Crypto.valueOf(), name: "USDC/USD" };

const pricingMethod = (process.env.PRICING_METHOD || PricingMethod.WEIGHTED) as PricingMethod;

export class PredictorFeed implements BaseDataFeed {
  private readonly logger = new Logger(PredictorFeed.name);
  protected initialized = false;
  private config: FeedConfig[];
  private lastReportedRound: number = -1;

  private readonly exchangeByName: Map<string, Exchange> = new Map();

  /** Symbol -> exchange -> price */
  private readonly prices: Map<string, Map<string, PriceInfo>> = new Map();

  private readonly reconnectAttempts = new Map<string, number>();
  private readonly MAX_RECONNECT_ATTEMPTS = 5;
  private readonly RECONNECT_RESET_TIME = 30 * 60 * 1000; // 30 minutes

  private readonly exchangeReliabilityScores = new Map<string, number>([
    ['binance', 1.0],
    ['coinbase', 0.95], 
    ['kraken', 0.9]
  ]);

  private readonly predictorRateLimit = new Map<string, number>();
  private readonly MIN_PREDICTOR_INTERVAL = 1000; // 1 second

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
        const exchange: Exchange = new ccxt[exchangeName]({ newUpdates: true });
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
    
    // Modify the connection status logging
    setInterval(() => {
        // Count exchanges that have provided prices for any symbol
        const activeExchanges = new Set<string>();
        
        // Iterate through all prices
        this.prices.forEach((exchangePrices, symbol) => {
            exchangePrices.forEach((priceInfo, exchange) => {
                // Only count exchanges with recent prices (last 5 minutes)
                if (Date.now() - priceInfo.time < 5 * 60 * 1000) {
                    activeExchanges.add(exchange);
                }
            });
        });

        const totalExchanges = this.exchangeByName.size;
        this.logger.log(
            `Active exchanges: ${activeExchanges.size}/${totalExchanges}\n` +
            `Active exchanges: ${Array.from(activeExchanges).join(', ')}`
        );

        // Log inactive exchanges
        const inactiveExchanges = Array.from(this.exchangeByName.keys())
            .filter(exchange => !activeExchanges.has(exchange));
        if (inactiveExchanges.length > 0) {
            this.logger.warn(
                `Inactive exchanges: ${inactiveExchanges.join(', ')}`
            );
        }

        // Attempt to reconnect inactive exchanges
        inactiveExchanges.forEach(exchangeName => {
            const exchange = this.exchangeByName.get(exchangeName);
            if (exchange) {
                this.logger.log(`Attempting to reconnect to ${exchangeName}`);
                void this.reconnectExchange(exchange, exchangeName);
            }
        });
    }, 60000); // Every minute
    
    void this.watchTrades(exchangeToSymbols);

    setInterval(() => this.cleanupOldPrices(), 60 * 60 * 1000); // Run hourly
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
    this.logExchangeSourcesOnce(votingRoundId);

    let price: number;
    let priceSource: 'CCXT' | 'Predictor';
    
    // Get the number of configured sources for this feed
    const feedConfig = this.config.find(config => feedsEqual(config.feed, feed));
    const configuredSources = feedConfig?.sources.length || 0;
    
    // Get detailed source information
    const sourceDetails = feedConfig?.sources.map(source => {
        const hasPrice = this.prices.get(source.symbol)?.get(source.exchange) !== undefined;
        return {
            exchange: source.exchange,
            symbol: source.symbol,
            hasPrice,
            lastPrice: this.prices.get(source.symbol)?.get(source.exchange)?.price,
            lastUpdate: this.prices.get(source.symbol)?.get(source.exchange)?.time
        };
    }) || [];

    const activeSources = sourceDetails.filter(s => s.hasPrice).length;
    const sourcesInfo = `[${feed.name}][${activeSources}/${configuredSources} sources]`;

    // Log detailed source status
    this.logger.debug(`Source status for ${feed.name}:`);
    sourceDetails.forEach(source => {
        const timeAgo = source.lastUpdate ? `${((Date.now() - source.lastUpdate)/1000).toFixed(1)}s ago` : 'never';
        this.logger.debug(
            `- ${source.exchange} (${source.symbol}): ` +
            `${source.hasPrice ? 'active' : 'inactive'}, ` +
            `last price: ${source.lastPrice || 'none'}, ` +
            `updated: ${timeAgo}`
        );
    });

    if ([].includes(feed.name) || votingRoundId === 0) {
      price = await this.getFeedPrice(feed, votingRoundId);
      this.logger.log(`CCXT (ONLY) PRICE: ${sourcesInfo} ${price}`);
    } else {
      const ccxtPrice = await this.getFeedPrice(feed, votingRoundId);
      let predictorPrice: number | null = null;

      if (process.env.PREDICTOR_ENABLED === "true") {
        predictorPrice = await this.getFeedPricePredictor(feed, votingRoundId);
      }

      price = predictorPrice || ccxtPrice;
      priceSource = predictorPrice ? 'Predictor' : 'CCXT';
      
      this.logger.log(
        `${sourcesInfo} Using ${predictorPrice ? 'predictor' : 'CCXT'} price: ${price} ` +
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

      // Watch each market separately
      for (const marketId of marketIds) {
        void this.watchSingleMarket(exchange, marketId, exchangeName);
      }
    }
  }

  private async watchSingleMarket(exchange: Exchange, marketId: string, exchangeName: string) {
    let consecutiveErrors = 0;
    const MAX_CONSECUTIVE_ERRORS = 5;
    
    while (true) {
      try {
        const trades: Trade[] = await retry(
          async () => exchange.watchTrades(marketId),
          RETRY_BACKOFF_MS
        );
        
        // Reset error counter on success
        consecutiveErrors = 0;
        
        // Handle single trade or array of trades
        const tradesArray = Array.isArray(trades) ? trades : [trades];
        
        tradesArray.forEach(trade => {
          const prices = this.prices.get(trade.symbol) || new Map<string, PriceInfo>();
          prices.set(exchangeName, { 
            price: trade.price, 
            time: trade.timestamp, 
            exchange: exchangeName 
          });
          this.prices.set(trade.symbol, prices);
        });
      } catch (e) {
        consecutiveErrors++;
        
        this.logger.error(
          `Failed to watch trades for ${marketId} on ${exchangeName}: ${e}. ` +
          `Consecutive errors: ${consecutiveErrors}`
        );

        if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
          this.logger.error(`Too many consecutive errors, attempting reconnection...`);
          await this.reconnectExchange(exchange, exchangeName);
          consecutiveErrors = 0;
        }
        
        await new Promise(resolve => setTimeout(resolve, RETRY_BACKOFF_MS));
      }
    }
  }

  private async getFeedPrice(feedId: FeedId, votingRoundId: number, isConversionRate: boolean = false): Promise<number> {
    const config = this.config.find(config => feedsEqual(config.feed, feedId));
    if (!config) {
      this.logger.warn(`No config found for ${JSON.stringify(feedId)}`);
      return undefined;
    }

    let usdtToUsd = undefined;
    let usdcToUsd = undefined;
    const prices: PriceInfo[] = [];
    const now = Date.now();

    const sourceStats = {
      configured: config.sources.length,
      noInfo: 0,
      stalePrice: 0,
      validPrices: 0
    };

    if (!isConversionRate) {
      const hasUsdtPairs = config.sources.some(source => source.symbol.endsWith("USDT"));
      const hasUsdcPairs = config.sources.some(source => source.symbol.endsWith("USDC"));
      
      if (hasUsdtPairs) {
        usdtToUsd = await this.getFeedPrice(usdtToUsdFeedId, votingRoundId, true);
      }
      if (hasUsdcPairs) {
        usdcToUsd = await this.getFeedPrice(usdcToUsdFeedId, votingRoundId, true);
      }
    }

    for (const source of config.sources) {
      const info = this.prices.get(source.symbol)?.get(source.exchange);
      if (info === undefined) {
        sourceStats.noInfo++;
        this.logger.debug(`No price info for ${source.symbol} from ${source.exchange}`);
        continue;
      }

      if (now - info.time > 5 * 60 * 1000) {
        sourceStats.stalePrice++;
        this.logger.debug(`Stale price for ${source.symbol} from ${source.exchange}: ${(now - info.time)/1000}s old`);
        continue;
      }

      const isUsdtPair = source.symbol.endsWith("USDT");
      const isUsdcPair = source.symbol.endsWith("USDC");
      const isUsdPair = source.symbol.endsWith("USD");

      let price = info.price;

      if (!isConversionRate) {
        if (isUsdtPair && usdtToUsd) {
          price = price * usdtToUsd;
        } else if (isUsdcPair && usdcToUsd) {
          price = price * usdcToUsd;
        } else if (isUsdPair) {
          price = price; // Direct USD price
        }
      }

      prices.push({
        price: price,
        time: info.time,
        exchange: info.exchange,
        type: isUsdtPair ? 'USDT' : (isUsdcPair ? 'USDC' : 'USD')
      });
      sourceStats.validPrices++;
    }

    this.logger.debug(`Source statistics for ${feedId.name}:
      Configured: ${sourceStats.configured}
      No Info: ${sourceStats.noInfo}
      Stale: ${sourceStats.stalePrice}
      Valid: ${sourceStats.validPrices}
    `);

    if (prices.length === 0) {
      this.logger.warn(`No valid prices found for ${JSON.stringify(feedId)}`);
      return undefined;
    }

    const usdtCount = prices.filter(p => p.type === 'USDT').length;
    const usdcCount = prices.filter(p => p.type === 'USDC').length;
    const usdCount = prices.filter(p => p.type === 'USD').length;
    
    this.logger.debug(
      `Price sources distribution for ${feedId.name}: ` +
      `USDT: ${usdtCount}, USDC: ${usdcCount}, USD: ${usdCount}`
    );

    switch (pricingMethod) {
      case PricingMethod.ENHANCED:
        return this.getEnhancedPrice(prices);
      case PricingMethod.WEIGHTED:
        return this.weightedMedian(prices);
      default:
        this.logger.warn(`Unknown pricing method ${pricingMethod}, falling back to weighted median`);
        return this.weightedMedian(prices);
    }
  }

  private async calculatePriceStatistics(prices: PriceInfo[]) {
    const now = Date.now();
    const MAX_STALENESS = 5 * 60 * 1000; // 5 minutes

    // Filter stale prices
    let validPrices = prices.filter(p => (now - p.time) <= MAX_STALENESS);
    
    if (validPrices.length === 0) {
      const mostRecentTime = Math.max(...prices.map(p => p.time));
      validPrices = prices.filter(p => p.time === mostRecentTime);
      this.logger.warn(
        `All prices were stale. Using most recent prices from ${new Date(mostRecentTime).toISOString()}. ` +
        `${validPrices.length} prices selected`
      );
    }

    // Calculate statistics
    const values = validPrices.map(p => p.price);
    const mean = this.calculateMean(values);
    const std = this.calculateStandardDeviation(values, mean);

    return {
      validPrices,
      mean,
      std,
      originalCount: prices.length
    };
  }

  private getEnhancedPrice(prices: PriceInfo[]): number {
    if (prices.length === 0) {
        throw new Error("Price list cannot be empty.");
    }

    this.logger.debug(`Starting enhanced price calculation with ${prices.length} prices`);
    const now = Date.now();

    // Step 1: Initial filtering of stale prices
    const MAX_STALENESS = 5 * 60 * 1000; // 5 minutes
    let filteredPrices = prices.filter(p => (now - p.time) <= MAX_STALENESS);

    if (filteredPrices.length === 0) {
        const mostRecentTime = Math.max(...prices.map(p => p.time));
        filteredPrices = prices.filter(p => p.time === mostRecentTime);
        this.logger.warn(
            `All prices were stale. Using most recent prices (${mostRecentTime}). ` +
            `${filteredPrices.length} prices selected`
        );
    }

    // Step 2: Calculate basic statistics
    const values = filteredPrices.map(p => p.price);
    const mean = this.calculateMean(values);
    const std = this.calculateStandardDeviation(values, mean);
    const cv = std / mean; // Coefficient of variation

    // Step 3: Dynamic outlier detection
    const outlierThreshold = Math.min(3, Math.max(1.5, 2 * cv)); // Adaptive threshold
    const q1 = this.getQuantile(values.sort((a, b) => a - b), 0.25);
    const q3 = this.getQuantile(values.sort((a, b) => a - b), 0.75);
    const iqr = q3 - q1;
    const lowerBound = q1 - outlierThreshold * iqr;
    const upperBound = q3 + outlierThreshold * iqr;

    const pricesBeforeOutlierRemoval = filteredPrices.length;
    filteredPrices = filteredPrices.filter(p => p.price >= lowerBound && p.price <= upperBound);

    // Step 4: Calculate sophisticated weights
    const priceScores = filteredPrices.map(data => {
        // Time decay weight
        const timeWeight = Math.exp(-0.00005 * (now - data.time));

        // Exchange reliability weight
        const exchangeWeight = this.getExchangeReliability(data.exchange);

        // Price stability weight (based on how close to mean)
        const stabilityWeight = std === 0 ? 1 : 
            Math.exp(-Math.pow(data.price - mean, 2) / (2 * Math.pow(std, 2)));

        // Trend alignment weight
        const trendWeight = this.calculateTrendWeight(data, filteredPrices, now);

        // Cross-exchange correlation weight
        const correlationWeight = this.calculateCorrelationWeight(data, filteredPrices);

        // Combine weights with dynamic importance
        const totalWeight = (
            timeWeight * 0.25 +
            exchangeWeight * 0.20 +
            stabilityWeight * 0.25 +
            trendWeight * 0.15 +
            correlationWeight * 0.15
        );

        return {
            price: data.price,
            weight: totalWeight,
            exchange: data.exchange,
            components: {
                timeWeight,
                exchangeWeight,
                stabilityWeight,
                trendWeight,
                correlationWeight
            }
        };
    });

    // Log detailed analysis
    this.logEnhancedPriceAnalysis(priceScores, {
        mean,
        std,
        cv,
        outlierThreshold,
        removedCount: pricesBeforeOutlierRemoval - filteredPrices.length
    });

    // Step 5: Calculate final price using weighted median
    priceScores.sort((a, b) => a.price - b.price);
    const totalWeight = priceScores.reduce((sum, { weight }) => sum + weight, 0);
    let cumulativeWeight = 0;

    for (const score of priceScores) {
        cumulativeWeight += score.weight / totalWeight;
        if (cumulativeWeight >= 0.5) {
            return score.price;
        }
    }

    return mean; // Fallback to simple mean if weighted median fails
  }

  private calculateTrendWeight(
    current: PriceInfo,
    allPrices: PriceInfo[],
    now: number
  ): number {
    // Calculate short-term price trend
    const recentPrices = allPrices
        .filter(p => p.exchange === current.exchange)
        .sort((a, b) => b.time - a.time)
        .slice(0, 3);

    if (recentPrices.length < 2) return 1;

    const trend = recentPrices.reduce((acc, price, i, arr) => {
        if (i === 0) return 0;
        return acc + (price.price - arr[i-1].price);
    }, 0) / (recentPrices.length - 1);

    // Higher weight if price follows the general trend
    return Math.exp(-Math.abs(trend) * 0.1);
  }

  private calculateCorrelationWeight(
    current: PriceInfo,
    allPrices: PriceInfo[]
  ): number {
    const otherExchangePrices = allPrices
        .filter(p => p.exchange !== current.exchange)
        .map(p => p.price);

    if (otherExchangePrices.length === 0) return 1;

    const avgOtherPrice = this.calculateMean(otherExchangePrices);
    const priceDiff = Math.abs(current.price - avgOtherPrice) / avgOtherPrice;

    // Exponential decay based on difference from other exchanges
    return Math.exp(-priceDiff * 2);
  }

  private logEnhancedPriceAnalysis(
    priceScores: Array<{
        price: number;
        weight: number;
        exchange: string;
        components: {
            timeWeight: number;
            exchangeWeight: number;
            stabilityWeight: number;
            trendWeight: number;
            correlationWeight: number;
        };
    }>,
    stats: {
        mean: number;
        std: number;
        cv: number;
        outlierThreshold: number;
        removedCount: number;
    }
  ): void {
    this.logger.debug(`Enhanced Price Analysis:
    Statistics:
    - Mean: ${stats.mean.toFixed(8)}
    - Std Dev: ${stats.std.toFixed(8)}
    - CV: ${stats.cv.toFixed(4)}
    - Outlier Threshold: ${stats.outlierThreshold.toFixed(2)}
    - Removed Outliers: ${stats.removedCount}

    Price Weights:`);

    priceScores.forEach(score => {
        this.logger.debug(`
        ${score.exchange}: ${score.price.toFixed(8)}
        - Final Weight: ${score.weight.toFixed(4)}
        - Time: ${score.components.timeWeight.toFixed(4)}
        - Exchange: ${score.components.exchangeWeight.toFixed(4)}
        - Stability: ${score.components.stabilityWeight.toFixed(4)}
        - Trend: ${score.components.trendWeight.toFixed(4)}
        - Correlation: ${score.components.correlationWeight.toFixed(4)}`);
    });
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
    return this.exchangeReliabilityScores.get(exchange) || 0.8;
  }

  private async getFeedPricePredictor(feedId: FeedId, votingRound: number): Promise<number> {
    const baseSymbol = feedId.name.split("/")[0];
    
    // Check rate limit
    const lastCall = this.predictorRateLimit.get(baseSymbol) || 0;
    const now = Date.now();
    if (now - lastCall < this.MIN_PREDICTOR_INTERVAL) {
      return null;
    }
    
    this.predictorRateLimit.set(baseSymbol, now);
    
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
      this.validateConfig(config);

      this.logger.log(`Supported feeds: ${JSON.stringify(config.map(f => f.feed))}`);

      return config;
    } catch (err) {
      this.logger.error("Error parsing JSON config:", err);
      throw err;
    }
  }

  private validateConfig(config: FeedConfig[]): void {
    if (!Array.isArray(config)) {
      throw new Error('Config must be an array');
    }

    const requiredFeeds = [usdtToUsdFeedId, usdcToUsdFeedId];
    for (const required of requiredFeeds) {
      if (!config.find(feed => feedsEqual(feed.feed, required))) {
        throw new Error(`Missing required feed configuration: ${required.name}`);
      }
    }

    config.forEach(feedConfig => {
      if (!feedConfig.sources || !Array.isArray(feedConfig.sources)) {
        throw new Error(`Invalid sources for feed ${feedConfig.feed.name}`);
      }
      if (feedConfig.sources.length === 0) {
        throw new Error(`No sources configured for feed ${feedConfig.feed.name}`);
      }
    });
  }

  private weightedMedian(prices: PriceInfo[]): number {
    if (prices.length === 0) {
        throw new Error("Price list cannot be empty.");
    }

    const now = Date.now();

    // Configuration (could be moved to env variables)
    const MAX_STALENESS = 5 * 60 * 1000; // 5 minutes
    const lambda = process.env.MEDIAN_DECAY ? parseFloat(process.env.MEDIAN_DECAY) : 0.00005;
    
    // Filter out extremely stale prices
    let validPrices = prices.filter(p => (now - p.time) <= MAX_STALENESS);
    
    // Fallback to most recent prices if all are stale
    if (validPrices.length === 0) {
        const mostRecentTime = Math.max(...prices.map(p => p.time));
        validPrices = prices.filter(p => p.time === mostRecentTime);
        this.logger.warn(
            `All prices were stale. Using most recent prices from ${new Date(mostRecentTime).toISOString()}. ` +
            `${validPrices.length} prices selected`
        );
    }

    // Calculate basic statistics for outlier detection
    validPrices.sort((a, b) => a.price - b.price);
    const q1 = validPrices[Math.floor(validPrices.length * 0.25)].price;
    const q3 = validPrices[Math.floor(validPrices.length * 0.75)].price;
    const iqr = q3 - q1;
    const lowerBound = q1 - 1.5 * iqr;
    const upperBound = q3 + 1.5 * iqr;

    // Remove outliers
    const pricesWithoutOutliers = validPrices.filter(p => 
        p.price >= lowerBound && p.price <= upperBound
    );

    // If too many prices were filtered, log warning and use original set
    if (pricesWithoutOutliers.length < validPrices.length * 0.5) {
        this.logger.warn(
            `Too many prices filtered as outliers (${validPrices.length - pricesWithoutOutliers.length}). ` +
            `Using original price set.`
        );
    } else {
        validPrices = pricesWithoutOutliers;
    }

    // Calculate exponential weights
    const weights = validPrices.map(data => {
        const timeDifference = now - data.time;
        return Math.exp(-lambda * timeDifference);
    });

    // Normalize weights
    const weightSum = weights.reduce((sum, weight) => sum + weight, 0);
    const normalizedWeights = weights.map(weight => weight / weightSum);

    // Combine prices and weights
    const weightedPrices = validPrices.map((data, i) => ({
        price: data.price,
        weight: normalizedWeights[i],
        exchange: data.exchange,
        staleness: Math.round((now - data.time) / 1000) // in seconds
    }));

    // Sort by price for median calculation
    weightedPrices.sort((a, b) => a.price - b.price);

    // Log detailed price information
    this.logger.debug("Weighted prices distribution:");
    weightedPrices.forEach(({ price, weight, exchange, staleness }) => {
        this.logger.debug(
            `${exchange}: $${price.toFixed(4)} | ` +
            `Weight: ${(weight * 100).toFixed(2)}% | ` +
            `Staleness: ${staleness}s`
        );
    });

    // Find weighted median
    let cumulativeWeight = 0;
    for (const wp of weightedPrices) {
        cumulativeWeight += wp.weight;
        if (cumulativeWeight >= 0.5) {
            this.logger.debug(`Selected median price: $${wp.price.toFixed(4)} from ${wp.exchange}`);
            return wp.price;
        }
    }

    // Fallback to arithmetic mean if median calculation fails
    const meanPrice = weightedPrices.reduce((sum, wp) => sum + wp.price * wp.weight, 0);
    this.logger.warn("Falling back to weighted mean price");
    return meanPrice;
  }

  // Add a method to handle reconnection
  private async reconnectExchange(exchange: Exchange, exchangeName: string) {
    try {
      await exchange.close();
      await new Promise(resolve => setTimeout(resolve, 1000));
      
      exchange.markets = {};
      await exchange.loadMarkets();
      
      this.logger.log(`Successfully reconnected to ${exchangeName}`);
      
      const symbols = Array.from(this.exchangeByName.keys())
        .filter(symbol => this.prices.has(symbol))
        .map(symbol => exchange.markets[symbol])
        .filter(market => market !== undefined)
        .map(market => market.id);

      if (symbols.length > 0) {
        for (const marketId of symbols) {
          void this.watchSingleMarket(exchange, marketId, exchangeName);
        }
      }
    } catch (e) {
      this.logger.error(`Failed to reconnect to ${exchangeName}: ${e}`);
    }
  }

  private logExchangeSourcesOnce(votingRoundId: number) {
    if (this.lastReportedRound !== votingRoundId) {
        this.logger.log(`Round ${votingRoundId}: Using ${pricingMethod.toUpperCase()} pricing method`);
        
        const now = Date.now();
        // Count configured and active sources across all feeds
        const exchangeSources = new Map<string, {
            USDT: { configured: number; active: number; details: Array<{symbol: string, lastUpdate: number, price: number}> };
            USDC: { configured: number; active: number; details: Array<{symbol: string, lastUpdate: number, price: number}> };
        }>();
        
        // Initialize counts
        this.config.forEach(feedConfig => {
            feedConfig.sources.forEach(source => {
                const exchange = source.exchange;
                if (!exchangeSources.has(exchange)) {
                    exchangeSources.set(exchange, {
                        USDT: { configured: 0, active: 0, details: [] },
                        USDC: { configured: 0, active: 0, details: [] }
                    });
                }
                
                const price = this.prices.get(source.symbol)?.get(exchange);
                const timeSinceUpdate = price ? (now - price.time) / 1000 : null; // in seconds

                if (source.symbol.endsWith('USDT')) {
                    exchangeSources.get(exchange).USDT.configured++;
                    exchangeSources.get(exchange).USDT.details.push({
                        symbol: source.symbol,
                        lastUpdate: timeSinceUpdate,
                        price: price?.price
                    });
                    if (price && (now - price.time) < 5 * 60 * 1000) {
                        exchangeSources.get(exchange).USDT.active++;
                    }
                } else if (source.symbol.endsWith('USDC')) {
                    exchangeSources.get(exchange).USDC.configured++;
                    exchangeSources.get(exchange).USDC.details.push({
                        symbol: source.symbol,
                        lastUpdate: timeSinceUpdate,
                        price: price?.price
                    });
                    if (price && (now - price.time) < 5 * 60 * 1000) {
                        exchangeSources.get(exchange).USDC.active++;
                    }
                }
            });
        });

        // Log exchange sources with active/configured counts and details
        this.logger.log('Exchange sources for this round:');
        exchangeSources.forEach((counts, exchange) => {
            if (counts.USDT.configured > 0 || counts.USDC.configured > 0) {
                this.logger.log(
                    `  ${exchange}: ` +
                    `USDT pairs: ${counts.USDT.active}/${counts.USDT.configured} active, ` +
                    `USDC pairs: ${counts.USDC.active}/${counts.USDC.configured} active`
                );

                // Log details for inactive pairs
                if (counts.USDT.details.length > 0) {
                    this.logger.debug(`  ${exchange} USDT pairs details:`);
                    counts.USDT.details.forEach(detail => {
                        const status = detail.lastUpdate === null ? 'No data' :
                            detail.lastUpdate > 300 ? `Stale (${Math.floor(detail.lastUpdate)}s old)` : 'Active';
                        this.logger.debug(`    ${detail.symbol}: ${status}, Last price: ${detail.price || 'N/A'}`);
                    });
                }

                if (counts.USDC.details.length > 0) {
                    this.logger.debug(`  ${exchange} USDC pairs details:`);
                    counts.USDC.details.forEach(detail => {
                        const status = detail.lastUpdate === null ? 'No data' :
                            detail.lastUpdate > 300 ? `Stale (${Math.floor(detail.lastUpdate)}s old)` : 'Active';
                        this.logger.debug(`    ${detail.symbol}: ${status}, Last price: ${detail.price || 'N/A'}`);
                    });
                }
            }
        });
        
        this.lastReportedRound = votingRoundId;
    }
  }

  private cleanupOldPrices() {
    const MAX_AGE = 24 * 60 * 60 * 1000; // 24 hours
    const now = Date.now();

    this.prices.forEach((exchangePrices, symbol) => {
      exchangePrices.forEach((priceInfo, exchange) => {
        if (now - priceInfo.time > MAX_AGE) {
          exchangePrices.delete(exchange);
        }
      });
      
      if (exchangePrices.size === 0) {
        this.prices.delete(symbol);
      }
    });
  }
}

function feedsEqual(a: FeedId, b: FeedId): boolean {
  return a.category === b.category && a.name === b.name;
}
