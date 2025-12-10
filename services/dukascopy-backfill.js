// =============================================================================
// services/dukascopy-backfill.js - Dukascopy Backfill (DELETE + INSERT)
// =============================================================================
//
// Uses DELETE + INSERT instead of ON DUPLICATE KEY UPDATE for reliability
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
    'XAUUSD': 'xauusd', 'XAGUSD': 'xagusd',
};

const TIMEFRAME_MAP = {
    'M1': 'm1', 'M5': 'm5', 'M15': 'm15', 'M30': 'm30',
    'H1': 'h1', 'H4': 'h4', 'D1': 'd1',
};

class DukascopyBackfill {
    constructor() {
        this.lastRequestTime = 0;
        this.minRequestInterval = 2000;
        
        this.chunkSizes = {
            'M1': 1, 'M5': 7, 'M15': 14, 'M30': 30,
            'H1': 60, 'H4': 180, 'D1': 365,
        };
        
        this.stats = {
            requestsMade: 0,
            candlesFetched: 0,
            candlesDeleted: 0,
            candlesInserted: 0,
            errors: 0,
        };
    }

    // =========================================================================
    // MAIN BACKFILL - DELETE + INSERT
    // =========================================================================

    async fetchAndSave(symbol, timeframe, from, to) {
        const instrument = DUKASCOPY_INSTRUMENTS[symbol];
        if (!instrument) throw new Error(`Unknown symbol: ${symbol}`);
        
        const tf = TIMEFRAME_MAP[timeframe];
        if (!tf) throw new Error(`Unknown timeframe: ${timeframe}`);
        
        console.log(`📥 Backfilling ${symbol} ${timeframe}: ${from.toISOString().split('T')[0]} to ${to.toISOString().split('T')[0]}`);
        
        const chunks = this.splitIntoChunks(from, to, timeframe);
        let totalInserted = 0;
        
        for (const chunk of chunks) {
            try {
                await this.rateLimit();
                
                // 1. Fetch from Dukascopy
                const candles = await this.fetchCandles(symbol, timeframe, chunk.from, chunk.to);
                
                if (candles.length > 0) {
                    // 2. DELETE existing candles in this range
                    const deleted = await this.deleteRange(symbol, timeframe, chunk.from, chunk.to);
                    
                    // 3. INSERT fresh candles
                    const inserted = await this.insertCandles(candles);
                    totalInserted += inserted;
                    
                    console.log(`   ✅ Chunk ${chunk.from.toISOString().split('T')[0]}: deleted ${deleted}, inserted ${inserted}`);
                }
                
            } catch (error) {
                this.stats.errors++;
                console.error(`   ❌ Chunk error: ${error.message}`);
                
                if (error.message.includes('rate limit')) {
                    await this.sleep(10000);
                }
            }
        }
        
        // Rebuild higher timeframes
        if (totalInserted > 0 && timeframe === 'M1') {
            await this.rebuildHigherTimeframes(symbol, from, to);
        }
        
        return totalInserted;
    }

    // =========================================================================
    // DELETE EXISTING CANDLES
    // =========================================================================

    async deleteRange(symbol, timeframe, from, to) {
        try {
            const [result] = await database.pool.execute(`
                DELETE FROM pulse_market_data
                WHERE symbol = ? AND timeframe = ?
                AND timestamp >= ? AND timestamp < ?
            `, [symbol, timeframe, from, to]);
            
            this.stats.candlesDeleted += result.affectedRows;
            return result.affectedRows;
        } catch (error) {
            console.error(`Delete error: ${error.message}`);
            return 0;
        }
    }

    // =========================================================================
    // FETCH FROM DUKASCOPY
    // =========================================================================

    async fetchCandles(symbol, timeframe, from, to) {
        const instrument = DUKASCOPY_INSTRUMENTS[symbol];
        const tf = TIMEFRAME_MAP[timeframe];
        
        this.stats.requestsMade++;
        
        try {
            const data = await getHistoricalRates({
                instrument,
                dates: { from, to },
                timeframe: tf,
                format: 'json',
                priceType: 'bid',
                volumes: true,
            });
            
            this.stats.candlesFetched += data.length;
            
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
            console.error(`❌ Dukascopy fetch error: ${error.message}`);
            throw error;
        }
    }

    // =========================================================================
    // INSERT FRESH CANDLES (plain INSERT, not upsert)
    // =========================================================================

    async insertCandles(candles) {
        if (candles.length === 0) return 0;
        
        const batchSize = 500;
        let inserted = 0;
        
        for (let i = 0; i < candles.length; i += batchSize) {
            const batch = candles.slice(i, i + batchSize);
            
            const placeholders = batch.map(() => '(?, ?, ?, ?, ?, ?, ?, ?, 0)').join(',');
            const values = batch.flatMap(c => [
                c.symbol, c.timeframe, c.timestamp,
                c.open, c.high, c.low, c.close, c.volume
            ]);
            
            try {
                // Plain INSERT - we already deleted, so no duplicates
                const [result] = await database.pool.execute(`
                    INSERT INTO pulse_market_data 
                    (symbol, timeframe, timestamp, open, high, low, close, volume, spread)
                    VALUES ${placeholders}
                `, values);
                
                inserted += result.affectedRows;
                
            } catch (error) {
                // If batch fails (unlikely), try individual inserts
                if (error.code === 'ER_DUP_ENTRY') {
                    for (const c of batch) {
                        try {
                            await database.pool.execute(`
                                REPLACE INTO pulse_market_data 
                                (symbol, timeframe, timestamp, open, high, low, close, volume, spread)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
                            `, [c.symbol, c.timeframe, c.timestamp, c.open, c.high, c.low, c.close, c.volume]);
                            inserted++;
                        } catch (e) {
                            // Skip
                        }
                    }
                } else {
                    console.error('Insert error:', error.message);
                }
            }
        }
        
        this.stats.candlesInserted += inserted;
        return inserted;
    }

    // =========================================================================
    // FIX INCOMPLETE CANDLES SPECIFICALLY
    // =========================================================================

    /**
     * Find and fix candles where OHLC are all the same (incomplete)
     */
    async fixIncompleteCandles(symbol, timeframe, from, to) {
        console.log(`🔧 Fixing incomplete ${symbol} ${timeframe} candles...`);
        
        // 1. Find incomplete candles
        const [incomplete] = await database.pool.execute(`
            SELECT id, timestamp
            FROM pulse_market_data
            WHERE symbol = ? AND timeframe = ?
            AND timestamp >= ? AND timestamp < ?
            AND open = high AND high = low AND low = close
            ORDER BY timestamp
        `, [symbol, timeframe, from, to]);
        
        if (incomplete.length === 0) {
            console.log(`   ✅ No incomplete candles found`);
            return 0;
        }
        
        console.log(`   Found ${incomplete.length} incomplete candles`);
        
        // 2. Group into ranges for efficient fetching
        const ranges = this.groupTimestampsIntoRanges(
            incomplete.map(r => new Date(r.timestamp)),
            timeframe
        );
        
        console.log(`   Grouped into ${ranges.length} ranges`);
        
        let totalFixed = 0;
        
        // 3. For each range, DELETE and re-fetch
        for (const range of ranges) {
            await this.rateLimit();
            
            // Fetch fresh data
            const candles = await this.fetchCandles(symbol, timeframe, range.from, range.to);
            
            if (candles.length > 0) {
                // Delete the bad candles in this range
                await this.deleteRange(symbol, timeframe, range.from, range.to);
                
                // Insert fresh candles
                const inserted = await this.insertCandles(candles);
                totalFixed += inserted;
                
                console.log(`   ✅ Range ${range.from.toISOString().slice(0,16)}: fixed ${inserted} candles`);
            }
        }
        
        return totalFixed;
    }

    /**
     * Group timestamps into contiguous ranges
     */
    groupTimestampsIntoRanges(timestamps, timeframe) {
        if (timestamps.length === 0) return [];
        
        const tfMinutes = { M1: 1, M5: 5, M15: 15, M30: 30, H1: 60, H4: 240, D1: 1440 };
        const gapThreshold = (tfMinutes[timeframe] || 1) * 60 * 1000 * 3; // 3x timeframe
        
        const sorted = timestamps.map(t => t.getTime()).sort((a, b) => a - b);
        const ranges = [];
        
        let rangeStart = sorted[0];
        let rangeEnd = sorted[0];
        
        for (let i = 1; i < sorted.length; i++) {
            if (sorted[i] - rangeEnd > gapThreshold) {
                // New range
                ranges.push({
                    from: new Date(rangeStart - 60000), // 1 min before
                    to: new Date(rangeEnd + 60000),     // 1 min after
                });
                rangeStart = sorted[i];
            }
            rangeEnd = sorted[i];
        }
        
        // Last range
        ranges.push({
            from: new Date(rangeStart - 60000),
            to: new Date(rangeEnd + 60000),
        });
        
        return ranges;
    }

    // =========================================================================
    // REBUILD HIGHER TIMEFRAMES
    // =========================================================================

    async rebuildHigherTimeframes(symbol, from, to) {
        console.log(`   🔄 Rebuilding higher timeframes...`);
        
        const timeframes = ['M5', 'M15', 'M30', 'H1', 'H4', 'D1'];
        
        for (const tf of timeframes) {
            try {
                // Get M1 candles in range
                const [m1Candles] = await database.pool.execute(`
                    SELECT timestamp, open, high, low, close, volume
                    FROM pulse_market_data
                    WHERE symbol = ? AND timeframe = 'M1'
                    AND timestamp >= ? AND timestamp < ?
                    ORDER BY timestamp
                `, [symbol, from, to]);
                
                if (m1Candles.length === 0) continue;
                
                // Group by period
                const grouped = this.groupCandlesByPeriod(m1Candles, tf);
                
                // Delete and re-insert each period
                for (const [periodKey, candles] of grouped) {
                    const periodStart = new Date(parseInt(periodKey));
                    const aggregated = this.aggregateCandles(candles);
                    
                    if (aggregated) {
                        // Delete existing
                        await database.pool.execute(`
                            DELETE FROM pulse_market_data
                            WHERE symbol = ? AND timeframe = ? AND timestamp = ?
                        `, [symbol, tf, periodStart]);
                        
                        // Insert new
                        await database.pool.execute(`
                            INSERT INTO pulse_market_data
                            (symbol, timeframe, timestamp, open, high, low, close, volume, spread)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
                        `, [symbol, tf, periodStart, aggregated.open, aggregated.high, 
                            aggregated.low, aggregated.close, aggregated.volume]);
                    }
                }
                
            } catch (error) {
                console.error(`   ❌ Rebuild ${tf} error: ${error.message}`);
            }
        }
    }

    groupCandlesByPeriod(candles, timeframe) {
        const tfMinutes = { M5: 5, M15: 15, M30: 30, H1: 60, H4: 240, D1: 1440 };
        const periodMs = (tfMinutes[timeframe] || 60) * 60 * 1000;
        
        const grouped = new Map();
        
        for (const candle of candles) {
            const ts = new Date(candle.timestamp).getTime();
            const periodStart = Math.floor(ts / periodMs) * periodMs;
            
            if (!grouped.has(periodStart.toString())) {
                grouped.set(periodStart.toString(), []);
            }
            grouped.get(periodStart.toString()).push(candle);
        }
        
        return grouped;
    }

    aggregateCandles(candles) {
        if (candles.length === 0) return null;
        
        candles.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
        
        return {
            open: parseFloat(candles[0].open),
            high: Math.max(...candles.map(c => parseFloat(c.high))),
            low: Math.min(...candles.map(c => parseFloat(c.low))),
            close: parseFloat(candles[candles.length - 1].close),
            volume: candles.reduce((sum, c) => sum + parseFloat(c.volume || 0), 0),
        };
    }

    // =========================================================================
    // UTILITIES
    // =========================================================================

    splitIntoChunks(from, to, timeframe) {
        const chunks = [];
        const chunkDays = this.chunkSizes[timeframe] || 7;
        const chunkMs = chunkDays * 24 * 60 * 60 * 1000;
        
        let currentStart = new Date(from);
        
        while (currentStart < to) {
            const chunkEnd = new Date(Math.min(currentStart.getTime() + chunkMs, to.getTime()));
            chunks.push({ from: new Date(currentStart), to: chunkEnd });
            currentStart = new Date(chunkEnd);
        }
        
        return chunks;
    }

    async rateLimit() {
        const now = Date.now();
        const elapsed = now - this.lastRequestTime;
        
        if (elapsed < this.minRequestInterval) {
            await this.sleep(this.minRequestInterval - elapsed);
        }
        
        this.lastRequestTime = Date.now();
    }

    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    getAvailableSymbols() {
        return Object.keys(DUKASCOPY_INSTRUMENTS);
    }

    isSupported(symbol) {
        return DUKASCOPY_INSTRUMENTS.hasOwnProperty(symbol);
    }

    getStats() {
        return { ...this.stats };
    }

    resetStats() {
        this.stats = { requestsMade: 0, candlesFetched: 0, candlesDeleted: 0, candlesInserted: 0, errors: 0 };
    }
}

module.exports = { DukascopyBackfill, DUKASCOPY_INSTRUMENTS, TIMEFRAME_MAP };