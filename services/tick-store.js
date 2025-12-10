// =============================================================================
// services/tick-store.js - Raw Tick Data Storage & Management
// =============================================================================
//
// Responsibilities:
// - Buffer incoming ticks in memory
// - Validate ticks (spike detection)
// - Batch insert to database
// - Provide ticks for candle building
// =============================================================================

const database = require('../database');
const { SpikeFilter } = require('./spike-filter');

class TickStore {
    constructor() {
        // In-memory tick buffer: Map<symbol, Array<tick>>
        this.buffer = new Map();
        this.maxBufferSize = 1000;  // Per symbol
        this.flushThreshold = 100;   // Flush when buffer reaches this
        
        this.spikeFilter = new SpikeFilter();
        
        this.stats = {
            ticksReceived: 0,
            ticksStored: 0,
            ticksRejected: 0,
            flushes: 0,
        };
    }

    // =========================================================================
    // TICK INGESTION
    // =========================================================================

    /**
     * Add a tick to the buffer (called from WebSocket handler)
     */
    async addTick(symbol, price, volume = 0, timestamp = new Date(), source = 'finnhub') {
        this.stats.ticksReceived++;
        
        // Validate price
        if (!price || isNaN(price) || price <= 0) {
            this.stats.ticksRejected++;
            return { accepted: false, reason: 'invalid_price' };
        }
        
        // Spike detection
        const symbolType = this.getSymbolType(symbol);
        const spikeCheck = this.spikeFilter.check(symbol, symbolType, price);
        
        if (spikeCheck.isSpike) {
            this.stats.ticksRejected++;
            console.warn(`⚠️ SPIKE REJECTED: ${symbol} ${spikeCheck.lastPrice?.toFixed(5)} → ${price.toFixed(5)} (${spikeCheck.reason})`);
            return { accepted: false, reason: 'spike', details: spikeCheck };
        }
        
        // Update spike filter's price tracking
        this.spikeFilter.updatePrice(symbol, price);
        
        // Add to buffer
        if (!this.buffer.has(symbol)) {
            this.buffer.set(symbol, []);
        }
        
        const tick = {
            symbol,
            price,
            volume,
            timestamp: new Date(timestamp),
            source,
            isValid: true,
        };
        
        const symbolBuffer = this.buffer.get(symbol);
        symbolBuffer.push(tick);
        
        // Trim if too large
        if (symbolBuffer.length > this.maxBufferSize) {
            symbolBuffer.shift();
        }
        
        // Auto-flush if threshold reached
        if (symbolBuffer.length >= this.flushThreshold) {
            await this.flushSymbol(symbol);
        }
        
        return { accepted: true, tick };
    }

    /**
     * Add multiple ticks at once (for REST API fallback)
     */
    async addTicks(symbol, ticks) {
        const results = [];
        for (const tick of ticks) {
            const result = await this.addTick(
                symbol,
                tick.price,
                tick.volume || 0,
                tick.timestamp || new Date(),
                tick.source || 'rest'
            );
            results.push(result);
        }
        return results;
    }

    // =========================================================================
    // BUFFER FLUSHING
    // =========================================================================

    /**
     * Flush buffered ticks for a symbol to database
     */
    async flushSymbol(symbol) {
        const buffer = this.buffer.get(symbol);
        if (!buffer || buffer.length === 0) return 0;
        
        // Take ticks from buffer
        const ticks = buffer.splice(0, buffer.length);
        
        try {
            const inserted = await this.batchInsertTicks(ticks);
            this.stats.ticksStored += inserted;
            this.stats.flushes++;
            return inserted;
        } catch (error) {
            console.error(`❌ Tick flush error (${symbol}):`, error.message);
            // Put ticks back on error
            buffer.unshift(...ticks.slice(-100)); // Keep last 100
            return 0;
        }
    }

    /**
     * Flush all symbol buffers
     */
    async flushAll() {
        let total = 0;
        for (const symbol of this.buffer.keys()) {
            total += await this.flushSymbol(symbol);
        }
        return total;
    }

    /**
     * Batch insert ticks to database
     */
    async batchInsertTicks(ticks) {
        if (ticks.length === 0) return 0;
        
        const batchSize = 500;
        let inserted = 0;
        
        for (let i = 0; i < ticks.length; i += batchSize) {
            const batch = ticks.slice(i, i + batchSize);
            
            const placeholders = batch.map(() => '(?, ?, ?, ?, ?, ?)').join(',');
            const values = batch.flatMap(t => [
                t.symbol, t.price, t.volume, t.timestamp, t.source, t.isValid ? 1 : 0
            ]);
            
            try {
                const [result] = await database.pool.execute(`
                    INSERT IGNORE INTO pulse_ticks 
                    (symbol, price, volume, timestamp, source, is_valid)
                    VALUES ${placeholders}
                `, values);
                
                inserted += result.affectedRows;
            } catch (error) {
                // Fall back to individual inserts
                for (const tick of batch) {
                    try {
                        await database.pool.execute(`
                            INSERT IGNORE INTO pulse_ticks 
                            (symbol, price, volume, timestamp, source, is_valid)
                            VALUES (?, ?, ?, ?, ?, ?)
                        `, [tick.symbol, tick.price, tick.volume, tick.timestamp, tick.source, tick.isValid ? 1 : 0]);
                        inserted++;
                    } catch (e) {
                        // Skip duplicates
                    }
                }
            }
        }
        
        return inserted;
    }

    // =========================================================================
    // TICK RETRIEVAL (for candle building)
    // =========================================================================

    /**
     * Get ticks for a specific minute (for M1 candle building)
     */
    async getTicksForMinute(symbol, minuteStart) {
        const minuteEnd = new Date(minuteStart.getTime() + 60000);
        
        // First check buffer
        const bufferedTicks = this.getBufferedTicksInRange(symbol, minuteStart, minuteEnd);
        
        // Then check database
        const [dbTicks] = await database.pool.execute(`
            SELECT price, volume, timestamp
            FROM pulse_ticks
            WHERE symbol = ? 
            AND timestamp >= ? AND timestamp < ?
            AND is_valid = 1
            ORDER BY timestamp ASC
        `, [symbol, minuteStart, minuteEnd]);
        
        // Merge and deduplicate
        const allTicks = [...dbTicks, ...bufferedTicks];
        const unique = this.deduplicateTicks(allTicks);
        
        return unique.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
    }

    /**
     * Get ticks from buffer within a time range
     */
    getBufferedTicksInRange(symbol, start, end) {
        const buffer = this.buffer.get(symbol) || [];
        return buffer.filter(t => {
            const ts = new Date(t.timestamp);
            return ts >= start && ts < end;
        });
    }

    /**
     * Get last N ticks for a symbol
     */
    async getRecentTicks(symbol, count = 100) {
        const [rows] = await database.pool.execute(`
            SELECT price, volume, timestamp, source
            FROM pulse_ticks
            WHERE symbol = ? AND is_valid = 1
            ORDER BY timestamp DESC
            LIMIT ?
        `, [symbol, count]);
        
        return rows.reverse();
    }

    /**
     * Get tick count for a time range
     */
    async getTickCount(symbol, from, to) {
        const [rows] = await database.pool.execute(`
            SELECT COUNT(*) as count
            FROM pulse_ticks
            WHERE symbol = ? 
            AND timestamp >= ? AND timestamp < ?
            AND is_valid = 1
        `, [symbol, from, to]);
        
        return rows[0].count;
    }

    // =========================================================================
    // TICK REFILL (when WebSocket misses data)
    // =========================================================================

    /**
     * Check if we have tick coverage for recent minutes
     * Returns list of missing minute intervals
     */
    async findMissingTicks(symbol, lookbackMinutes = 10) {
        const missing = [];
        const now = new Date();
        
        for (let i = 1; i <= lookbackMinutes; i++) {
            const minuteStart = new Date(now);
            minuteStart.setUTCMinutes(minuteStart.getUTCMinutes() - i, 0, 0);
            
            const count = await this.getTickCount(
                symbol,
                minuteStart,
                new Date(minuteStart.getTime() + 60000)
            );
            
            if (count === 0) {
                missing.push(minuteStart);
            }
        }
        
        return missing;
    }

    /**
     * Refill missing ticks from Finnhub REST API
     */
    async refillMissingTicks(symbol, finnhubClient) {
        const missing = await this.findMissingTicks(symbol, 5);
        
        if (missing.length === 0) return 0;
        
        let refilled = 0;
        
        try {
            // Get latest quote from Finnhub
            const quote = await finnhubClient.getQuote(symbol);
            
            if (quote && quote.price) {
                for (const minuteStart of missing) {
                    // Create synthetic tick at the middle of the minute
                    const tickTime = new Date(minuteStart.getTime() + 30000);
                    
                    await this.addTick(
                        symbol,
                        quote.price,
                        0,
                        tickTime,
                        'rest_refill'
                    );
                    refilled++;
                }
            }
        } catch (error) {
            console.error(`❌ Tick refill error (${symbol}):`, error.message);
        }
        
        return refilled;
    }

    // =========================================================================
    // UTILITIES
    // =========================================================================

    getSymbolType(symbol) {
        if (symbol.startsWith('XAU') || symbol.startsWith('XAG')) return 'metal';
        if (['BTC', 'ETH', 'XRP', 'SOL', 'ADA', 'DOGE'].some(c => symbol.startsWith(c))) return 'crypto';
        return 'forex';
    }

    deduplicateTicks(ticks) {
        const seen = new Set();
        return ticks.filter(tick => {
            const key = `${tick.price}-${new Date(tick.timestamp).getTime()}`;
            if (seen.has(key)) return false;
            seen.add(key);
            return true;
        });
    }

    /**
     * Process buffered ticks for a symbol (called by cron)
     */
    async processBufferedTicks(symbol) {
        return this.flushSymbol(symbol);
    }

    getStats() {
        const bufferSizes = {};
        for (const [symbol, buffer] of this.buffer) {
            bufferSizes[symbol] = buffer.length;
        }
        
        return {
            ...this.stats,
            bufferSizes,
            totalBuffered: Array.from(this.buffer.values()).reduce((sum, b) => sum + b.length, 0),
        };
    }

    clearBuffer(symbol = null) {
        if (symbol) {
            this.buffer.delete(symbol);
        } else {
            this.buffer.clear();
        }
    }
}

module.exports = { TickStore };