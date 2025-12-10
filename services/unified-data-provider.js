// =============================================================================
// services/unified-data-provider.js - Multi-Source Historical Data Provider
// =============================================================================
//
// Strategy:
// 1. Try Polygon FIRST (faster, lower delay ~1-5 min)
// 2. Fall back to Dukascopy if Polygon fails or has no data
// 3. Combine results if one source has gaps
//
// This gives you the best of both worlds:
// - Polygon's speed for recent data
// - Dukascopy's reliability for older data
// =============================================================================

const { PolygonService } = require('./polygon-service');
const { getHistoricalRates } = require('dukascopy-node');

const DUKASCOPY_INSTRUMENTS = {
    'EURUSD': 'eurusd', 'GBPUSD': 'gbpusd', 'USDJPY': 'usdjpy',
    'USDCHF': 'usdchf', 'AUDUSD': 'audusd', 'USDCAD': 'usdcad',
    'NZDUSD': 'nzdusd', 'EURGBP': 'eurgbp', 'EURJPY': 'eurjpy',
    'GBPJPY': 'gbpjpy', 'EURCHF': 'eurchf', 'GBPCHF': 'gbpchf',
    'AUDJPY': 'audjpy', 'EURAUD': 'euraud', 'EURCAD': 'eurcad',
    'GBPAUD': 'gbpaud', 'GBPCAD': 'gbpcad', 'AUDCAD': 'audcad',
    'AUDNZD': 'audnzd', 'NZDJPY': 'nzdjpy', 'CADJPY': 'cadjpy',
    'XAUUSD': 'xauusd', 'XAGUSD': 'xagusd',
};

const TIMEFRAME_MAP = {
    'M1': 'm1', 'M5': 'm5', 'M15': 'm15', 'M30': 'm30',
    'H1': 'h1', 'H4': 'h4', 'D1': 'd1'
};

class UnifiedDataProvider {
    constructor(options = {}) {
        this.polygonApiKey = options.polygonApiKey || process.env.POLYGON_API_KEY;
        this.polygon = new PolygonService(this.polygonApiKey);
        
        // Strategy: 'polygon-first', 'dukascopy-first', 'polygon-only', 'dukascopy-only'
        this.strategy = options.strategy || 'polygon-first';
        
        this.stats = {
            polygonRequests: 0,
            polygonSuccess: 0,
            polygonFailed: 0,
            dukascopyRequests: 0,
            dukascopySuccess: 0,
            dukascopyFailed: 0,
            combinedResults: 0,
        };
    }

    // =========================================================================
    // MAIN FETCH METHOD
    // =========================================================================

    /**
     * Fetch candles using the configured strategy
     */
    async fetchCandles(symbol, timeframe, from, to) {
        console.log(`   🔄 Fetching ${symbol} ${timeframe} via ${this.strategy}`);
        
        switch (this.strategy) {
            case 'polygon-first':
                return this.fetchPolygonFirst(symbol, timeframe, from, to);
            case 'dukascopy-first':
                return this.fetchDukascopyFirst(symbol, timeframe, from, to);
            case 'polygon-only':
                return this.fetchFromPolygon(symbol, timeframe, from, to);
            case 'dukascopy-only':
                return this.fetchFromDukascopy(symbol, timeframe, from, to);
            default:
                return this.fetchPolygonFirst(symbol, timeframe, from, to);
        }
    }

    // =========================================================================
    // POLYGON-FIRST STRATEGY
    // =========================================================================

    async fetchPolygonFirst(symbol, timeframe, from, to) {
        // Step 1: Try Polygon first
        let polygonCandles = [];
        let polygonError = null;
        
        try {
            this.stats.polygonRequests++;
            polygonCandles = await this.fetchFromPolygon(symbol, timeframe, from, to);
            this.stats.polygonSuccess++;
            
            console.log(`   ✅ Polygon: ${polygonCandles.length} candles`);
            
        } catch (error) {
            polygonError = error.message;
            this.stats.polygonFailed++;
            console.log(`   ⚠️ Polygon failed: ${error.message}`);
        }

        // Step 2: Check if we have enough data from Polygon
        const expectedCandles = this.calculateExpectedCandles(timeframe, from, to);
        const coverage = polygonCandles.length / expectedCandles;
        
        // If Polygon gave us good coverage (>80%), use it
        if (coverage > 0.8) {
            return polygonCandles;
        }

        // Step 3: Try Dukascopy as fallback/supplement
        let dukascopyCandles = [];
        
        try {
            this.stats.dukascopyRequests++;
            dukascopyCandles = await this.fetchFromDukascopy(symbol, timeframe, from, to);
            this.stats.dukascopySuccess++;
            
            console.log(`   ✅ Dukascopy: ${dukascopyCandles.length} candles`);
            
        } catch (error) {
            this.stats.dukascopyFailed++;
            console.log(`   ⚠️ Dukascopy failed: ${error.message}`);
        }

        // Step 4: Combine results (prefer Polygon for overlapping timestamps)
        if (polygonCandles.length > 0 && dukascopyCandles.length > 0) {
            this.stats.combinedResults++;
            return this.mergeCandles(polygonCandles, dukascopyCandles);
        }

        // Return whichever has more data
        return polygonCandles.length >= dukascopyCandles.length ? polygonCandles : dukascopyCandles;
    }

    // =========================================================================
    // DUKASCOPY-FIRST STRATEGY
    // =========================================================================

    async fetchDukascopyFirst(symbol, timeframe, from, to) {
        // Step 1: Try Dukascopy first
        let dukascopyCandles = [];
        
        try {
            this.stats.dukascopyRequests++;
            dukascopyCandles = await this.fetchFromDukascopy(symbol, timeframe, from, to);
            this.stats.dukascopySuccess++;
            
            console.log(`   ✅ Dukascopy: ${dukascopyCandles.length} candles`);
            
        } catch (error) {
            this.stats.dukascopyFailed++;
            console.log(`   ⚠️ Dukascopy failed: ${error.message}`);
        }

        // Check coverage
        const expectedCandles = this.calculateExpectedCandles(timeframe, from, to);
        const coverage = dukascopyCandles.length / expectedCandles;

        if (coverage > 0.8) {
            return dukascopyCandles;
        }

        // Step 2: Try Polygon as fallback
        let polygonCandles = [];
        
        try {
            this.stats.polygonRequests++;
            polygonCandles = await this.fetchFromPolygon(symbol, timeframe, from, to);
            this.stats.polygonSuccess++;
            
            console.log(`   ✅ Polygon fallback: ${polygonCandles.length} candles`);
            
        } catch (error) {
            this.stats.polygonFailed++;
            console.log(`   ⚠️ Polygon fallback failed: ${error.message}`);
        }

        // Combine - prefer Dukascopy for overlaps
        if (dukascopyCandles.length > 0 && polygonCandles.length > 0) {
            this.stats.combinedResults++;
            return this.mergeCandles(dukascopyCandles, polygonCandles);
        }

        return dukascopyCandles.length >= polygonCandles.length ? dukascopyCandles : polygonCandles;
    }

    // =========================================================================
    // INDIVIDUAL SOURCE FETCHERS
    // =========================================================================

    async fetchFromPolygon(symbol, timeframe, from, to) {
        if (!this.polygon.isSymbolSupported(symbol)) {
            throw new Error(`${symbol} not supported by Polygon`);
        }
        
        return await this.polygon.fetchCandles(symbol, timeframe, from, to);
    }

    async fetchFromDukascopy(symbol, timeframe, from, to) {
        const instrument = DUKASCOPY_INSTRUMENTS[symbol];
        const tf = TIMEFRAME_MAP[timeframe];
        
        if (!instrument) {
            throw new Error(`${symbol} not supported by Dukascopy`);
        }

        try {
            const data = await getHistoricalRates({
                instrument,
                dates: { from, to },
                timeframe: tf,
                format: 'json',
                priceType: 'bid',
                volumes: true,
            });

            return data.map(candle => ({
                symbol,
                timeframe,
                timestamp: new Date(candle.timestamp),
                open: candle.open,
                high: candle.high,
                low: candle.low,
                close: candle.close,
                volume: candle.volume || 0,
            }));
            
        } catch (error) {
            throw new Error(`Dukascopy: ${error.message}`);
        }
    }

    // =========================================================================
    // HELPERS
    // =========================================================================

    /**
     * Calculate expected number of candles for a time range
     */
    calculateExpectedCandles(timeframe, from, to) {
        const tfMinutes = {
            'M1': 1, 'M5': 5, 'M15': 15, 'M30': 30,
            'H1': 60, 'H4': 240, 'D1': 1440
        };
        
        const minutes = tfMinutes[timeframe] || 1;
        const durationMs = to.getTime() - from.getTime();
        const durationMinutes = durationMs / (60 * 1000);
        
        // Account for market hours (forex ~5 days/week, ~21 hours/day)
        const tradingRatio = 0.625; // Roughly 5/8 of the time
        
        return Math.floor((durationMinutes / minutes) * tradingRatio);
    }

    /**
     * Merge candles from two sources
     * Primary source takes precedence for overlapping timestamps
     */
    mergeCandles(primary, secondary) {
        const candleMap = new Map();
        
        // Add secondary first
        for (const candle of secondary) {
            const key = new Date(candle.timestamp).getTime();
            candleMap.set(key, candle);
        }
        
        // Primary overwrites
        for (const candle of primary) {
            const key = new Date(candle.timestamp).getTime();
            candleMap.set(key, candle);
        }
        
        // Sort by timestamp
        const merged = Array.from(candleMap.values());
        merged.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
        
        console.log(`   🔀 Merged: ${merged.length} unique candles`);
        return merged;
    }

    /**
     * Get data delay for each source
     */
    async checkDelays(symbol = 'EURUSD') {
        const results = {};
        
        // Check Polygon delay
        try {
            const polygonDelay = await this.polygon.getDataDelay(symbol);
            results.polygon = polygonDelay;
        } catch (e) {
            results.polygon = { error: e.message };
        }
        
        // Check Dukascopy delay
        try {
            const now = new Date();
            const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000);
            const candles = await this.fetchFromDukascopy(symbol, 'M1', oneHourAgo, now);
            
            if (candles.length > 0) {
                const last = candles[candles.length - 1];
                results.dukascopy = {
                    delayMinutes: Math.floor((now.getTime() - new Date(last.timestamp).getTime()) / 60000),
                    lastCandle: last.timestamp
                };
            } else {
                results.dukascopy = { delayMinutes: 999, lastCandle: null };
            }
        } catch (e) {
            results.dukascopy = { error: e.message };
        }
        
        return results;
    }

    getStats() {
        return {
            ...this.stats,
            polygon: this.polygon.getStats(),
        };
    }
}

module.exports = { UnifiedDataProvider };