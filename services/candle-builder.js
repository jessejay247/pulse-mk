// =============================================================================
// services/candle-builder.js - Candle Construction from Ticks + Aggregation
// =============================================================================
//
// Responsibilities:
// - Build M1 candles from raw ticks
// - Aggregate higher timeframes from M1 candles
// - Validate candle data
// - Detect and mark incomplete candles
// =============================================================================

const database = require('../database');
const { TickStore } = require('./tick-store');
const { isMarketOpenForSymbol } = require('../config/market-hours');

class CandleBuilder {
    constructor() {
        this.tickStore = new TickStore();
        
        // Timeframe configurations
        this.timeframeConfig = {
            M1:  { minutes: 1,    sourceTimeframe: null,   parent: 'M5' },
            M5:  { minutes: 5,    sourceTimeframe: 'M1',   parent: 'M15' },
            M15: { minutes: 15,   sourceTimeframe: 'M1',   parent: 'M30' },
            M30: { minutes: 30,   sourceTimeframe: 'M1',   parent: 'H1' },
            H1:  { minutes: 60,   sourceTimeframe: 'M1',   parent: 'H4' },
            H4:  { minutes: 240,  sourceTimeframe: 'H1',   parent: 'D1' },
            D1:  { minutes: 1440, sourceTimeframe: 'H1',   parent: null },
        };
        
        this.stats = {
            m1Built: 0,
            aggregated: 0,
            incomplete: 0,
            errors: 0,
        };
    }

    // =========================================================================
    // M1 CANDLE BUILDING (from ticks)
    // =========================================================================

    /**
     * Build a single M1 candle from ticks
     * This is the foundation - all other timeframes are built from M1
     */
    async buildM1FromTicks(symbol, minuteStart) {
        const symbolType = this.getSymbolType(symbol);
        
        // Check if market is open
        const marketStatus = isMarketOpenForSymbol(symbolType, minuteStart);
        if (!marketStatus.open) {
            return null; // Don't build candles when market is closed
        }
        
        // Get ticks for this minute
        const ticks = await this.tickStore.getTicksForMinute(symbol, minuteStart);
        
        if (ticks.length === 0) {
            // No ticks - mark as potential gap
            return { symbol, timestamp: minuteStart, hasGap: true };
        }
        
        // Build OHLCV from ticks
        const prices = ticks.map(t => parseFloat(t.price));
        const volumes = ticks.map(t => parseFloat(t.volume || 0));
        
        const candle = {
            symbol,
            timeframe: 'M1',
            timestamp: minuteStart,
            open: prices[0],
            high: Math.max(...prices),
            low: Math.min(...prices),
            close: prices[prices.length - 1],
            volume: volumes.reduce((a, b) => a + b, 0),
            tickCount: ticks.length,
            isComplete: ticks.length >= 2, // At least 2 ticks = likely complete
        };
        
        // Validate candle
        const validation = this.validateCandle(candle);
        if (!validation.valid) {
            console.warn(`⚠️ Invalid M1 candle ${symbol} @ ${minuteStart.toISOString()}: ${validation.reason}`);
            this.stats.incomplete++;
        }
        
        // Save to database
        await this.saveCandle(candle);
        this.stats.m1Built++;
        
        return candle;
    }

    /**
     * Build M1 candles for a time range
     */
    async buildM1Range(symbol, from, to) {
        const candles = [];
        let current = new Date(from);
        current.setUTCSeconds(0, 0);
        
        while (current < to) {
            const candle = await this.buildM1FromTicks(symbol, new Date(current));
            if (candle && !candle.hasGap) {
                candles.push(candle);
            }
            current.setUTCMinutes(current.getUTCMinutes() + 1);
        }
        
        return candles;
    }

    // =========================================================================
    // HIGHER TIMEFRAME AGGREGATION
    // =========================================================================

    /**
     * Build a higher timeframe candle from M1 or H1 candles
     */
    async buildFromM1(symbol, targetTimeframe) {
        const config = this.timeframeConfig[targetTimeframe];
        if (!config) {
            throw new Error(`Unknown timeframe: ${targetTimeframe}`);
        }
        
        const sourceTimeframe = config.sourceTimeframe;
        const periodMinutes = config.minutes;
        
        // Calculate current period boundaries
        const now = new Date();
        const periodStart = this.getPeriodStart(now, targetTimeframe);
        const periodEnd = new Date(periodStart.getTime() + periodMinutes * 60 * 1000);
        
        // Don't build if period hasn't ended yet (except D1)
        if (targetTimeframe !== 'D1' && now < periodEnd) {
            return null;
        }
        
        // Get source candles
        const sourceCandles = await this.getSourceCandles(
            symbol, sourceTimeframe, periodStart, periodEnd
        );
        
        if (sourceCandles.length === 0) {
            return { symbol, timeframe: targetTimeframe, timestamp: periodStart, hasGap: true };
        }
        
        // Aggregate into single candle
        const candle = this.aggregateCandles(symbol, targetTimeframe, periodStart, sourceCandles);
        
        // Calculate expected candle count
        const expectedCount = this.getExpectedCandleCount(sourceTimeframe, periodMinutes);
        candle.isComplete = sourceCandles.length >= expectedCount * 0.8; // 80% threshold
        
        if (!candle.isComplete) {
            this.stats.incomplete++;
        }
        
        // Save
        await this.saveCandle(candle);
        this.stats.aggregated++;
        
        return candle;
    }

    /**
     * Rebuild a specific timeframe candle
     */
    async rebuildCandle(symbol, timeframe, timestamp) {
        const config = this.timeframeConfig[timeframe];
        if (!config || !config.sourceTimeframe) {
            throw new Error(`Cannot rebuild ${timeframe} - no source timeframe`);
        }
        
        const periodStart = this.getPeriodStart(new Date(timestamp), timeframe);
        const periodEnd = new Date(periodStart.getTime() + config.minutes * 60 * 1000);
        
        // Get fresh source candles
        const sourceCandles = await this.getSourceCandles(
            symbol, config.sourceTimeframe, periodStart, periodEnd
        );
        
        if (sourceCandles.length === 0) {
            return null;
        }
        
        const candle = this.aggregateCandles(symbol, timeframe, periodStart, sourceCandles);
        await this.saveCandle(candle, true); // Force update
        
        return candle;
    }

    /**
     * Rebuild all affected higher timeframes when M1 is updated
     */
    async rebuildAffectedTimeframes(symbol, m1Timestamp) {
        const timeframes = ['M5', 'M15', 'M30', 'H1', 'H4', 'D1'];
        
        for (const tf of timeframes) {
            const periodStart = this.getPeriodStart(new Date(m1Timestamp), tf);
            await this.rebuildCandle(symbol, tf, periodStart);
        }
    }

    // =========================================================================
    // CANDLE AGGREGATION LOGIC
    // =========================================================================

    /**
     * Aggregate multiple source candles into one
     */
    aggregateCandles(symbol, timeframe, timestamp, sourceCandles) {
        if (sourceCandles.length === 0) {
            return null;
        }
        
        // Sort by timestamp
        sourceCandles.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
        
        const opens = sourceCandles.map(c => parseFloat(c.open));
        const highs = sourceCandles.map(c => parseFloat(c.high));
        const lows = sourceCandles.map(c => parseFloat(c.low));
        const closes = sourceCandles.map(c => parseFloat(c.close));
        const volumes = sourceCandles.map(c => parseFloat(c.volume || 0));
        
        return {
            symbol,
            timeframe,
            timestamp,
            open: opens[0],
            high: Math.max(...highs),
            low: Math.min(...lows),
            close: closes[closes.length - 1],
            volume: volumes.reduce((a, b) => a + b, 0),
            sourceCount: sourceCandles.length,
        };
    }

    // =========================================================================
    // DATABASE OPERATIONS
    // =========================================================================

    /**
     * Get source candles for aggregation
     */
    async getSourceCandles(symbol, timeframe, from, to) {
        const [rows] = await database.pool.execute(`
            SELECT timestamp, open, high, low, close, volume
            FROM pulse_market_data
            WHERE symbol = ? AND timeframe = ?
            AND timestamp >= ? AND timestamp < ?
            ORDER BY timestamp ASC
        `, [symbol, timeframe, from, to]);
        
        return rows;
    }

    /**
     * Save candle to database (upsert)
     */
    async saveCandle(candle, forceUpdate = false) {
        const query = forceUpdate ? `
            INSERT INTO pulse_market_data 
            (symbol, timeframe, timestamp, open, high, low, close, volume, spread)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
            ON DUPLICATE KEY UPDATE
                open = VALUES(open),
                high = VALUES(high),
                low = VALUES(low),
                close = VALUES(close),
                volume = VALUES(volume)
        ` : `
            INSERT INTO pulse_market_data 
            (symbol, timeframe, timestamp, open, high, low, close, volume, spread)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
            ON DUPLICATE KEY UPDATE
                high = GREATEST(high, VALUES(high)),
                low = LEAST(low, VALUES(low)),
                close = VALUES(close),
                volume = volume + VALUES(volume)
        `;
        
        try {
            await database.pool.execute(query, [
                candle.symbol,
                candle.timeframe,
                candle.timestamp,
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume || 0
            ]);
        } catch (error) {
            this.stats.errors++;
            throw error;
        }
    }

    /**
     * Get the latest candle for a symbol/timeframe
     */
    async getLatestCandle(symbol, timeframe) {
        const [rows] = await database.pool.execute(`
            SELECT timestamp, open, high, low, close, volume
            FROM pulse_market_data
            WHERE symbol = ? AND timeframe = ?
            ORDER BY timestamp DESC
            LIMIT 1
        `, [symbol, timeframe]);
        
        return rows[0] || null;
    }

    // =========================================================================
    // VALIDATION
    // =========================================================================

    /**
     * Validate a candle for consistency
     */
    validateCandle(candle) {
        // Check OHLC relationships
        if (candle.high < candle.low) {
            return { valid: false, reason: 'high < low' };
        }
        
        if (candle.high < candle.open || candle.high < candle.close) {
            return { valid: false, reason: 'high not highest' };
        }
        
        if (candle.low > candle.open || candle.low > candle.close) {
            return { valid: false, reason: 'low not lowest' };
        }
        
        // Check for zero/negative values
        if (candle.open <= 0 || candle.high <= 0 || candle.low <= 0 || candle.close <= 0) {
            return { valid: false, reason: 'non-positive price' };
        }
        
        // Check for identical OHLC (incomplete candle)
        if (candle.open === candle.high && 
            candle.high === candle.low && 
            candle.low === candle.close) {
            return { valid: false, reason: 'identical OHLC (likely incomplete)' };
        }
        
        // Check for suspicious range (too small)
        const range = candle.high - candle.low;
        const rangePercent = (range / candle.close) * 100;
        if (rangePercent < 0.0001 && candle.timeframe !== 'M1') {
            return { valid: false, reason: 'suspiciously small range' };
        }
        
        return { valid: true };
    }

    /**
     * Find incomplete candles (OHLC all same)
     */
    async findIncompleteCandles(symbol, timeframe, from, to) {
        const [rows] = await database.pool.execute(`
            SELECT id, timestamp, open, high, low, close
            FROM pulse_market_data
            WHERE symbol = ? AND timeframe = ?
            AND timestamp BETWEEN ? AND ?
            AND open = high AND high = low AND low = close
        `, [symbol, timeframe, from, to]);
        
        return rows;
    }

    // =========================================================================
    // PERIOD CALCULATIONS
    // =========================================================================

    /**
     * Get the start of a period for a given timeframe
     */
    getPeriodStart(date, timeframe) {
        const d = new Date(date);
        d.setUTCSeconds(0, 0);
        
        switch (timeframe) {
            case 'M1':
                // Already at minute start
                break;
            case 'M5':
                d.setUTCMinutes(Math.floor(d.getUTCMinutes() / 5) * 5);
                break;
            case 'M15':
                d.setUTCMinutes(Math.floor(d.getUTCMinutes() / 15) * 15);
                break;
            case 'M30':
                d.setUTCMinutes(Math.floor(d.getUTCMinutes() / 30) * 30);
                break;
            case 'H1':
                d.setUTCMinutes(0);
                break;
            case 'H4':
                d.setUTCMinutes(0);
                d.setUTCHours(Math.floor(d.getUTCHours() / 4) * 4);
                break;
            case 'D1':
                d.setUTCMinutes(0);
                d.setUTCHours(0);
                break;
        }
        
        return d;
    }

    /**
     * Get expected candle count for aggregation
     */
    getExpectedCandleCount(sourceTimeframe, targetMinutes) {
        const sourceMinutes = this.timeframeConfig[sourceTimeframe]?.minutes || 1;
        return Math.floor(targetMinutes / sourceMinutes);
    }

    // =========================================================================
    // UTILITIES
    // =========================================================================

    getSymbolType(symbol) {
        if (symbol.startsWith('XAU') || symbol.startsWith('XAG')) return 'metal';
        return 'forex';
    }

    getStats() {
        return { ...this.stats };
    }

    resetStats() {
        this.stats = { m1Built: 0, aggregated: 0, incomplete: 0, errors: 0 };
    }
}

module.exports = { CandleBuilder };