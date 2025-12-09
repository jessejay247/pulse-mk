// =============================================================================
// services/dukascopy-service.js - Dukascopy Historical Data Service
// =============================================================================

const { getHistoricalRates } = require('dukascopy-node');
const database = require('../database');

// Dukascopy instrument mappings
const DUKASCOPY_INSTRUMENTS = {
    // Major Forex Pairs
    'EURUSD': 'eurusd',
    'GBPUSD': 'gbpusd',
    'USDJPY': 'usdjpy',
    'USDCHF': 'usdchf',
    'AUDUSD': 'audusd',
    'USDCAD': 'usdcad',
    'NZDUSD': 'nzdusd',
    
    // Minor/Cross Pairs
    'EURGBP': 'eurgbp',
    'EURJPY': 'eurjpy',
    'GBPJPY': 'gbpjpy',
    'EURCHF': 'eurchf',
    'GBPCHF': 'gbpchf',
    'AUDJPY': 'audjpy',
    'EURAUD': 'euraud',
    'EURCAD': 'eurcad',
    'GBPAUD': 'gbpaud',
    'GBPCAD': 'gbpcad',
    'AUDCAD': 'audcad',
    'AUDNZD': 'audnzd',
    'NZDJPY': 'nzdjpy',
    'CADJPY': 'cadjpy',
    'CHFJPY': 'chfjpy',
    'EURNZD': 'eurnzd',
    
    // Metals
    'XAUUSD': 'xauusd',
    'XAGUSD': 'xagusd'
};

// Timeframe mappings
const TIMEFRAME_MAP = {
    'M1': 'm1',
    'M5': 'm5',
    'M15': 'm15',
    'M30': 'm30',
    'H1': 'h1',
    'H4': 'h4',
    'D1': 'd1',
    'W1': 'w1',
    'MN': 'mn1'
};

// Timeframe durations in milliseconds
const TIMEFRAME_MS = {
    'M1': 60 * 1000,
    'M5': 5 * 60 * 1000,
    'M15': 15 * 60 * 1000,
    'M30': 30 * 60 * 1000,
    'H1': 60 * 60 * 1000,
    'H4': 4 * 60 * 60 * 1000,
    'D1': 24 * 60 * 60 * 1000,
    'W1': 7 * 24 * 60 * 60 * 1000
};

class DukascopyService {
    constructor() {
        this.isRunning = false;
        this.stats = {
            fetched: 0,
            inserted: 0,
            errors: 0
        };
    }

    /**
     * Fetch historical data from Dukascopy
     */
    async fetchCandles(symbol, timeframe, fromDate, toDate) {
        const instrument = DUKASCOPY_INSTRUMENTS[symbol];
        const tf = TIMEFRAME_MAP[timeframe];

        if (!instrument) {
            throw new Error(`Unknown symbol: ${symbol}`);
        }
        if (!tf) {
            throw new Error(`Unknown timeframe: ${timeframe}`);
        }

        try {
            const data = await getHistoricalRates({
                instrument: instrument,
                dates: {
                    from: fromDate,
                    to: toDate
                },
                timeframe: tf,
                format: 'json',
                priceType: 'bid', // Use bid prices
                volumes: true
            });

            this.stats.fetched += data.length;
            return data.map(candle => ({
                symbol,
                timeframe,
                timestamp: new Date(candle.timestamp),
                open: candle.open,
                high: candle.high,
                low: candle.low,
                close: candle.close,
                volume: candle.volume || 0
            }));
        } catch (error) {
            this.stats.errors++;
            console.error(`❌ Error fetching ${symbol} ${timeframe}:`, error.message);
            throw error;
        }
    }

    /**
     * Save candles to database in batches
     */
    async saveCandles(candles, batchSize = 500) {
        if (!candles || candles.length === 0) return 0;

        let inserted = 0;

        for (let i = 0; i < candles.length; i += batchSize) {
            const batch = candles.slice(i, i + batchSize);
            
            try {
                const placeholders = batch.map(() => '(?, ?, ?, ?, ?, ?, ?, ?, ?)').join(',');
                const values = batch.flatMap(c => [
                    c.symbol,
                    c.timeframe,
                    c.timestamp,
                    c.open,
                    c.high,
                    c.low,
                    c.close,
                    c.volume,
                    0 // spread
                ]);

                await database.pool.execute(`
                    INSERT IGNORE INTO pulse_market_data 
                    (symbol, timeframe, timestamp, open, high, low, close, volume, spread)
                    VALUES ${placeholders}
                `, values);

                inserted += batch.length;
            } catch (error) {
                console.error('❌ Batch insert error:', error.message);
                // Try individual inserts
                for (const candle of batch) {
                    try {
                        await database.pool.execute(`
                            INSERT IGNORE INTO pulse_market_data 
                            (symbol, timeframe, timestamp, open, high, low, close, volume, spread)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        `, [
                            candle.symbol, candle.timeframe, candle.timestamp,
                            candle.open, candle.high, candle.low, candle.close,
                            candle.volume, 0
                        ]);
                        inserted++;
                    } catch (e) {
                        // Skip duplicates
                    }
                }
            }
        }

        this.stats.inserted += inserted;
        return inserted;
    }

    /**
     * Fetch and save data for a symbol/timeframe combination
     */
    async fetchAndSave(symbol, timeframe, fromDate, toDate) {
        console.log(`📥 Fetching ${symbol} ${timeframe} from ${fromDate.toISOString().split('T')[0]} to ${toDate.toISOString().split('T')[0]}`);
        
        try {
            const candles = await this.fetchCandles(symbol, timeframe, fromDate, toDate);
            
            if (candles.length > 0) {
                const inserted = await this.saveCandles(candles);
                console.log(`   ✅ Fetched ${candles.length} candles, inserted ${inserted}`);
                return inserted;
            } else {
                console.log(`   ⚠️  No data returned`);
                return 0;
            }
        } catch (error) {
            console.error(`   ❌ Error: ${error.message}`);
            return 0;
        }
    }

    /**
     * Get the latest candle timestamp for a symbol/timeframe
     */
    async getLatestTimestamp(symbol, timeframe) {
        try {
            const [rows] = await database.pool.execute(`
                SELECT MAX(timestamp) as latest
                FROM pulse_market_data
                WHERE symbol = ? AND timeframe = ?
            `, [symbol, timeframe]);

            return rows[0]?.latest || null;
        } catch (error) {
            console.error('Error getting latest timestamp:', error);
            return null;
        }
    }

    /**
     * Get the oldest candle timestamp for a symbol/timeframe
     */
    async getOldestTimestamp(symbol, timeframe) {
        try {
            const [rows] = await database.pool.execute(`
                SELECT MIN(timestamp) as oldest
                FROM pulse_market_data
                WHERE symbol = ? AND timeframe = ?
            `, [symbol, timeframe]);

            return rows[0]?.oldest || null;
        } catch (error) {
            console.error('Error getting oldest timestamp:', error);
            return null;
        }
    }

    /**
     * Detect gaps in data
     */
    async detectGaps(symbol, timeframe, fromDate, toDate) {
        const gaps = [];
        const tfMs = TIMEFRAME_MS[timeframe];
        
        if (!tfMs) return gaps;

        try {
            const [rows] = await database.pool.execute(`
                SELECT timestamp
                FROM pulse_market_data
                WHERE symbol = ? AND timeframe = ? 
                AND timestamp BETWEEN ? AND ?
                ORDER BY timestamp ASC
            `, [symbol, timeframe, fromDate, toDate]);

            if (rows.length < 2) return gaps;

            for (let i = 1; i < rows.length; i++) {
                const prev = new Date(rows[i - 1].timestamp).getTime();
                const curr = new Date(rows[i].timestamp).getTime();
                const expectedNext = prev + tfMs;

                // Allow some tolerance (market closed hours)
                // For forex, skip weekends
                if (curr - expectedNext > tfMs * 2) {
                    // Check if this gap spans a weekend
                    const gapStart = new Date(expectedNext);
                    const gapEnd = new Date(curr);
                    
                    // Skip weekend gaps for forex
                    if (!this.isWeekendGap(gapStart, gapEnd)) {
                        gaps.push({
                            from: gapStart,
                            to: gapEnd,
                            missingCandles: Math.floor((curr - expectedNext) / tfMs)
                        });
                    }
                }
            }
        } catch (error) {
            console.error('Error detecting gaps:', error);
        }

        return gaps;
    }

    /**
     * Check if a gap is just a weekend
     */
    isWeekendGap(from, to) {
        const fromDay = from.getUTCDay();
        const toDay = to.getUTCDay();
        const diffDays = (to - from) / (24 * 60 * 60 * 1000);

        // If gap is less than 3 days and spans Sat/Sun, it's a weekend
        if (diffDays <= 3) {
            // Friday 22:00 to Sunday 21:00 is normal weekend gap
            if (fromDay === 5 || fromDay === 6 || toDay === 0 || toDay === 1) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get available symbols
     */
    getSymbols() {
        return Object.keys(DUKASCOPY_INSTRUMENTS);
    }

    /**
     * Get forex symbols only (no metals)
     */
    getForexSymbols() {
        return Object.keys(DUKASCOPY_INSTRUMENTS).filter(s => !s.startsWith('XA'));
    }

    /**
     * Get metal symbols only
     */
    getMetalSymbols() {
        return Object.keys(DUKASCOPY_INSTRUMENTS).filter(s => s.startsWith('XA'));
    }

    /**
     * Reset stats
     */
    resetStats() {
        this.stats = { fetched: 0, inserted: 0, errors: 0 };
    }

    /**
     * Get stats
     */
    getStats() {
        return { ...this.stats };
    }
}

module.exports = new DukascopyService();
module.exports.DUKASCOPY_INSTRUMENTS = DUKASCOPY_INSTRUMENTS;
module.exports.TIMEFRAME_MAP = TIMEFRAME_MAP;
module.exports.TIMEFRAME_MS = TIMEFRAME_MS;