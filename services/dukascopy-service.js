// =============================================================================
// services/dukascopy-service.js - Dukascopy Historical Data Service (FIXED)
// =============================================================================
// 
// Changes:
// - Uses ON DUPLICATE KEY UPDATE instead of INSERT IGNORE
// - Updates incomplete candles with correct OHLC values
// =============================================================================

const { getHistoricalRates } = require('dukascopy-node');
const database = require('../database');

const DUKASCOPY_INSTRUMENTS = {
    'EURUSD': 'eurusd', 'GBPUSD': 'gbpusd', 'USDJPY': 'usdjpy',
    'USDCHF': 'usdchf', 'AUDUSD': 'audusd', 'USDCAD': 'usdcad',
    'NZDUSD': 'nzdusd', 'EURGBP': 'eurgbp', 'EURJPY': 'eurjpy',
    'GBPJPY': 'gbpjpy', 'EURCHF': 'eurchf', 'GBPCHF': 'gbpchf',
    'AUDJPY': 'audjpy', 'EURAUD': 'euraud', 'EURCAD': 'eurcad',
    'GBPAUD': 'gbpaud', 'GBPCAD': 'gbpcad', 'AUDCAD': 'audcad',
    'AUDNZD': 'audnzd', 'NZDJPY': 'nzdjpy', 'CADJPY': 'cadjpy',
    'CHFJPY': 'chfjpy', 'EURNZD': 'eurnzd',
    'XAUUSD': 'xauusd', 'XAGUSD': 'xagusd'
};

const TIMEFRAME_MAP = {
    'M1': 'm1', 'M5': 'm5', 'M15': 'm15', 'M30': 'm30',
    'H1': 'h1', 'H4': 'h4', 'D1': 'd1', 'W1': 'w1', 'MN': 'mn1'
};

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
        this.stats = { fetched: 0, inserted: 0, updated: 0, errors: 0 };
    }

    async fetchCandles(symbol, timeframe, fromDate, toDate) {
        const instrument = DUKASCOPY_INSTRUMENTS[symbol];
        const tf = TIMEFRAME_MAP[timeframe];

        if (!instrument) throw new Error(`Unknown symbol: ${symbol}`);
        if (!tf) throw new Error(`Unknown timeframe: ${timeframe}`);

        try {
            const data = await getHistoricalRates({
                instrument,
                dates: { from: fromDate, to: toDate },
                timeframe: tf,
                format: 'json',
                priceType: 'bid',
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
     * Save candles - UPDATED to use ON DUPLICATE KEY UPDATE
     * This ensures incomplete candles get corrected with proper OHLC data
     */
    async saveCandles(candles, batchSize = 500) {
        if (!candles || candles.length === 0) return 0;

        let inserted = 0;
        let updated = 0;

        for (let i = 0; i < candles.length; i += batchSize) {
            const batch = candles.slice(i, i + batchSize);
            
            try {
                const placeholders = batch.map(() => '(?, ?, ?, ?, ?, ?, ?, ?, ?)').join(',');
                const values = batch.flatMap(c => [
                    c.symbol, c.timeframe, c.timestamp,
                    c.open, c.high, c.low, c.close, c.volume, 0
                ]);

                // ✅ FIX: Use ON DUPLICATE KEY UPDATE to fix incomplete candles
                // This replaces the old OHLC values with correct historical data
                const [result] = await database.pool.execute(`
                    INSERT INTO pulse_market_data 
                    (symbol, timeframe, timestamp, open, high, low, close, volume, spread)
                    VALUES ${placeholders}
                    ON DUPLICATE KEY UPDATE
                        open = VALUES(open),
                        high = VALUES(high),
                        low = VALUES(low),
                        close = VALUES(close),
                        volume = VALUES(volume)
                `, values);

                // affectedRows: 1 = inserted, 2 = updated existing row
                inserted += result.affectedRows;
            } catch (error) {
                console.error('❌ Batch insert error:', error.message);
                // Individual fallback
                for (const candle of batch) {
                    try {
                        const [result] = await database.pool.execute(`
                            INSERT INTO pulse_market_data 
                            (symbol, timeframe, timestamp, open, high, low, close, volume, spread)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON DUPLICATE KEY UPDATE
                                open = VALUES(open),
                                high = VALUES(high),
                                low = VALUES(low),
                                close = VALUES(close),
                                volume = VALUES(volume)
                        `, [
                            candle.symbol, candle.timeframe, candle.timestamp,
                            candle.open, candle.high, candle.low, candle.close,
                            candle.volume, 0
                        ]);
                        inserted += result.affectedRows;
                    } catch (e) {
                        // Skip errors
                    }
                }
            }
        }

        this.stats.inserted += inserted;
        return inserted;
    }

    /**
     * Fetch and save - now with option to force update existing
     */
    async fetchAndSave(symbol, timeframe, fromDate, toDate, options = {}) {
        const { forceUpdate = true } = options; // Default to updating existing

        console.log(`📥 Fetching ${symbol} ${timeframe} from ${fromDate.toISOString().split('T')[0]} to ${toDate.toISOString().split('T')[0]}`);
        
        try {
            const candles = await this.fetchCandles(symbol, timeframe, fromDate, toDate);
            
            if (candles.length > 0) {
                const result = await this.saveCandles(candles);
                console.log(`   ✅ Fetched ${candles.length} candles, processed ${result} rows`);
                return result;
            } else {
                console.log(`   ⚠️  No data returned`);
                return 0;
            }
        } catch (error) {
            console.error(`   ❌ Error: ${error.message}`);
            return 0;
        }
    }

    async getLatestTimestamp(symbol, timeframe) {
        try {
            const [rows] = await database.pool.execute(`
                SELECT MAX(timestamp) as latest
                FROM pulse_market_data
                WHERE symbol = ? AND timeframe = ?
            `, [symbol, timeframe]);
            return rows[0]?.latest || null;
        } catch (error) {
            return null;
        }
    }

    async getOldestTimestamp(symbol, timeframe) {
        try {
            const [rows] = await database.pool.execute(`
                SELECT MIN(timestamp) as oldest
                FROM pulse_market_data
                WHERE symbol = ? AND timeframe = ?
            `, [symbol, timeframe]);
            return rows[0]?.oldest || null;
        } catch (error) {
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

                if (curr - expectedNext > tfMs * 2) {
                    const gapStart = new Date(expectedNext);
                    const gapEnd = new Date(curr);
                    
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
     * Detect incomplete/suspicious candles (same OHLC values)
     */
    async detectIncompleteCandles(symbol, timeframe, fromDate, toDate) {
        try {
            const [rows] = await database.pool.execute(`
                SELECT id, timestamp, open, high, low, close
                FROM pulse_market_data
                WHERE symbol = ? AND timeframe = ?
                AND timestamp BETWEEN ? AND ?
                AND open = high AND high = low AND low = close
            `, [symbol, timeframe, fromDate, toDate]);

            return rows.map(r => ({
                id: r.id,
                timestamp: r.timestamp,
                price: r.close
            }));
        } catch (error) {
            console.error('Error detecting incomplete candles:', error);
            return [];
        }
    }

    isWeekendGap(from, to) {
        const fromDay = from.getUTCDay();
        const toDay = to.getUTCDay();
        const diffDays = (to - from) / (24 * 60 * 60 * 1000);

        if (diffDays <= 3) {
            if (fromDay === 5 || fromDay === 6 || toDay === 0 || toDay === 1) {
                return true;
            }
        }
        return false;
    }

    getSymbols() { return Object.keys(DUKASCOPY_INSTRUMENTS); }
    getForexSymbols() { return Object.keys(DUKASCOPY_INSTRUMENTS).filter(s => !s.startsWith('XA')); }
    getMetalSymbols() { return Object.keys(DUKASCOPY_INSTRUMENTS).filter(s => s.startsWith('XA')); }
    resetStats() { this.stats = { fetched: 0, inserted: 0, updated: 0, errors: 0 }; }
    getStats() { return { ...this.stats }; }
}

module.exports = new DukascopyService();
module.exports.DUKASCOPY_INSTRUMENTS = DUKASCOPY_INSTRUMENTS;
module.exports.TIMEFRAME_MAP = TIMEFRAME_MAP;
module.exports.TIMEFRAME_MS = TIMEFRAME_MS;