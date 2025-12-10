// =============================================================================
// services/polygon-service.js - Polygon.io Historical Data Service
// =============================================================================
//
// Polygon.io provides forex data with much lower latency than Dukascopy
// (often just 1-2 minutes delay vs 30-45 minutes for Dukascopy)
//
// Free tier: 5 API calls/minute, 2 years of historical data
// =============================================================================

const axios = require('axios');

// Polygon ticker format: C:EURUSD (C: prefix for currencies)
const POLYGON_TICKERS = {
    'EURUSD': 'C:EURUSD', 'GBPUSD': 'C:GBPUSD', 'USDJPY': 'C:USDJPY',
    'USDCHF': 'C:USDCHF', 'AUDUSD': 'C:AUDUSD', 'USDCAD': 'C:USDCAD',
    'NZDUSD': 'C:NZDUSD', 'EURGBP': 'C:EURGBP', 'EURJPY': 'C:EURJPY',
    'GBPJPY': 'C:GBPJPY', 'EURCHF': 'C:EURCHF', 'GBPCHF': 'C:GBPCHF',
    'AUDJPY': 'C:AUDJPY', 'EURAUD': 'C:EURAUD', 'EURCAD': 'C:EURCAD',
    'GBPAUD': 'C:GBPAUD', 'GBPCAD': 'C:GBPCAD', 'AUDCAD': 'C:AUDCAD',
    'AUDNZD': 'C:AUDNZD', 'NZDJPY': 'C:NZDJPY', 'CADJPY': 'C:CADJPY',
    'CHFJPY': 'C:CHFJPY', 'EURNZD': 'C:EURNZD',
    // Metals use different format
    'XAUUSD': 'C:XAUUSD', 'XAGUSD': 'C:XAGUSD',
};

// Timeframe mapping: our format -> Polygon format
const TIMEFRAME_MAP = {
    'M1':  { multiplier: 1,  timespan: 'minute' },
    'M5':  { multiplier: 5,  timespan: 'minute' },
    'M15': { multiplier: 15, timespan: 'minute' },
    'M30': { multiplier: 30, timespan: 'minute' },
    'H1':  { multiplier: 1,  timespan: 'hour' },
    'H4':  { multiplier: 4,  timespan: 'hour' },
    'D1':  { multiplier: 1,  timespan: 'day' },
};

class PolygonService {
    constructor(apiKey = null) {
        this.apiKey = apiKey || process.env.POLYGON_API_KEY;
        this.baseUrl = 'https://api.polygon.io';
        
        // Rate limiting: 5 calls/minute for free tier
        this.lastRequestTime = 0;
        this.minRequestInterval = 12000; // 12 seconds between requests (5/min)
        
        this.stats = {
            requestsMade: 0,
            candlesFetched: 0,
            errors: 0,
            rateLimitHits: 0,
        };
    }

    // =========================================================================
    // RATE LIMITING
    // =========================================================================

    async rateLimit() {
        const now = Date.now();
        const timeSinceLastRequest = now - this.lastRequestTime;
        
        if (timeSinceLastRequest < this.minRequestInterval) {
            const waitTime = this.minRequestInterval - timeSinceLastRequest;
            await new Promise(resolve => setTimeout(resolve, waitTime));
        }
        
        this.lastRequestTime = Date.now();
    }

    // =========================================================================
    // FETCH CANDLES FROM POLYGON
    // =========================================================================

    /**
     * Fetch candles from Polygon.io
     * @param {string} symbol - e.g., 'EURUSD'
     * @param {string} timeframe - e.g., 'M1', 'H1', 'D1'
     * @param {Date} from - Start date
     * @param {Date} to - End date
     * @returns {Array} - Array of candle objects
     */
    async fetchCandles(symbol, timeframe, from, to) {
        const ticker = POLYGON_TICKERS[symbol];
        const tfConfig = TIMEFRAME_MAP[timeframe];
        
        if (!ticker) {
            throw new Error(`Symbol ${symbol} not supported by Polygon`);
        }
        if (!tfConfig) {
            throw new Error(`Timeframe ${timeframe} not supported`);
        }

        await this.rateLimit();
        this.stats.requestsMade++;

        // Format dates as YYYY-MM-DD for Polygon API
        const fromStr = from.toISOString().split('T')[0];
        const toStr = to.toISOString().split('T')[0];

        const url = `${this.baseUrl}/v2/aggs/ticker/${ticker}/range/${tfConfig.multiplier}/${tfConfig.timespan}/${fromStr}/${toStr}`;

        try {
            const response = await axios.get(url, {
                params: {
                    apiKey: this.apiKey,
                    adjusted: true,
                    sort: 'asc',
                    limit: 50000, // Max results
                },
                timeout: 30000,
            });

            if (response.data.status === 'ERROR') {
                throw new Error(response.data.error || 'Polygon API error');
            }

            const results = response.data.results || [];
            this.stats.candlesFetched += results.length;

            // Convert Polygon format to our format
            return results.map(bar => ({
                symbol,
                timeframe,
                timestamp: new Date(bar.t), // bar.t is Unix timestamp in ms
                open: bar.o,
                high: bar.h,
                low: bar.l,
                close: bar.c,
                volume: bar.v || 0,
            }));

        } catch (error) {
            this.stats.errors++;
            
            // Check for rate limiting
            if (error.response?.status === 429) {
                this.stats.rateLimitHits++;
                console.error(`⚠️ Polygon rate limited. Waiting 60s...`);
                await new Promise(resolve => setTimeout(resolve, 60000));
                throw new Error('rate_limited');
            }
            
            // Check for auth errors
            if (error.response?.status === 401 || error.response?.status === 403) {
                throw new Error(`Polygon auth error: Check API key`);
            }

            throw new Error(`Polygon error: ${error.message}`);
        }
    }

    /**
     * Fetch with automatic pagination for large date ranges
     */
    async fetchCandlesWithPagination(symbol, timeframe, from, to) {
        const allCandles = [];
        const maxDaysPerRequest = timeframe === 'M1' ? 1 : (timeframe === 'M5' ? 3 : 30);
        
        let currentFrom = new Date(from);
        
        while (currentFrom < to) {
            const chunkTo = new Date(Math.min(
                currentFrom.getTime() + maxDaysPerRequest * 24 * 60 * 60 * 1000,
                to.getTime()
            ));
            
            try {
                const candles = await this.fetchCandles(symbol, timeframe, currentFrom, chunkTo);
                allCandles.push(...candles);
                
                console.log(`   📊 Polygon: ${currentFrom.toISOString().slice(0,10)} → ${chunkTo.toISOString().slice(0,10)}: ${candles.length} candles`);
                
            } catch (error) {
                if (error.message === 'rate_limited') {
                    // Retry this chunk after waiting
                    continue;
                }
                console.error(`   ❌ Polygon chunk error: ${error.message}`);
            }
            
            currentFrom = chunkTo;
        }
        
        return allCandles;
    }

    /**
     * Check if Polygon has recent data (to determine delay)
     */
    async getDataDelay(symbol = 'EURUSD') {
        try {
            const now = new Date();
            const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000);
            
            const candles = await this.fetchCandles(symbol, 'M1', oneHourAgo, now);
            
            if (candles.length === 0) {
                return { delayMinutes: 999, lastCandle: null };
            }
            
            const lastCandle = candles[candles.length - 1];
            const delayMinutes = Math.floor((now.getTime() - new Date(lastCandle.timestamp).getTime()) / 60000);
            
            return { delayMinutes, lastCandle: lastCandle.timestamp };
            
        } catch (error) {
            return { delayMinutes: 999, error: error.message };
        }
    }

    /**
     * Check if symbol is supported
     */
    isSymbolSupported(symbol) {
        return !!POLYGON_TICKERS[symbol];
    }

    /**
     * Get stats
     */
    getStats() {
        return { ...this.stats };
    }
}

module.exports = { PolygonService, POLYGON_TICKERS };