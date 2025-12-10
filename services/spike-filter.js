// =============================================================================
// services/spike-filter.js - Price Spike Detection & Filtering
// =============================================================================
//
// Responsibilities:
// - Detect suspicious price spikes
// - Maintain rolling price history
// - Calculate dynamic thresholds based on volatility
// - Log rejected spikes for analysis
// =============================================================================

const database = require('../database');

class SpikeFilter {
    constructor(thresholds = {}) {
        // Default thresholds (percentage change that triggers rejection)
        this.thresholds = {
            forex: { tick: 0.3, candle: 0.5 },     // 0.3% per tick, 0.5% per candle
            metal: { tick: 0.8, candle: 1.5 },     // Metals more volatile
            crypto: { tick: 3.0, candle: 5.0 },    // Crypto very volatile
            stock: { tick: 5.0, candle: 10.0 },    // Stocks can gap
            ...thresholds
        };
        
        // Last known prices: Map<symbol, { price, timestamp }>
        this.lastPrices = new Map();
        
        // Price history for volatility: Map<symbol, Array<{ price, timestamp }>>
        this.priceHistory = new Map();
        this.historyLength = 30;  // Keep last 30 prices
        
        // Recent volatility: Map<symbol, number>
        this.volatility = new Map();
        
        this.stats = {
            ticksChecked: 0,
            spikesRejected: 0,
            spikesBySymbol: {},
        };
    }

    // =========================================================================
    // PRICE LOADING (on startup)
    // =========================================================================

    /**
     * Load last known prices from database on startup
     */
    async loadLastPrices() {
        try {
            const [rows] = await database.pool.execute(`
                SELECT symbol, close, timestamp
                FROM pulse_market_data
                WHERE timeframe = 'M1'
                AND timestamp > DATE_SUB(NOW(), INTERVAL 1 HOUR)
                ORDER BY timestamp DESC
            `);
            
            const seen = new Set();
            for (const row of rows) {
                if (!seen.has(row.symbol)) {
                    this.lastPrices.set(row.symbol, {
                        price: parseFloat(row.close),
                        timestamp: new Date(row.timestamp)
                    });
                    seen.add(row.symbol);
                }
            }
            
            console.log(`📊 Loaded ${this.lastPrices.size} last prices for spike detection`);
        } catch (error) {
            console.error('⚠️ Could not load last prices:', error.message);
        }
    }

    // =========================================================================
    // SPIKE DETECTION
    // =========================================================================

    /**
     * Check if a tick price is a spike
     * Returns: { isSpike: boolean, reason?: string, changePercent?: number }
     */
    check(symbol, symbolType, newPrice) {
        this.stats.ticksChecked++;
        
        const lastData = this.lastPrices.get(symbol);
        
        // No previous price - accept but flag
        if (!lastData) {
            return { isSpike: false, reason: 'no_history' };
        }
        
        const lastPrice = lastData.price;
        const timeSinceLastMs = Date.now() - lastData.timestamp.getTime();
        
        // Calculate percentage change
        const changePercent = Math.abs((newPrice - lastPrice) / lastPrice) * 100;
        
        // Get threshold for this symbol type
        let threshold = this.thresholds[symbolType]?.tick || 0.5;
        
        // Adjust threshold based on time elapsed
        // If last price is stale (>5 min), be more lenient
        if (timeSinceLastMs > 5 * 60 * 1000) {
            threshold *= 2;
        }
        
        // Adjust threshold based on recent volatility
        const vol = this.volatility.get(symbol);
        if (vol && vol > threshold) {
            threshold = Math.min(vol * 1.5, threshold * 3);
        }
        
        // Check if spike
        if (changePercent > threshold) {
            this.recordSpike(symbol, lastPrice, newPrice, changePercent);
            
            return {
                isSpike: true,
                reason: `${changePercent.toFixed(3)}% exceeds ${threshold.toFixed(3)}% threshold`,
                lastPrice,
                newPrice,
                changePercent,
                threshold,
            };
        }
        
        return { isSpike: false, changePercent };
    }

    /**
     * Check a candle for spikes (used during aggregation)
     */
    checkCandle(symbol, symbolType, candle, previousClose) {
        if (!previousClose) return { isSpike: false };
        
        const threshold = this.thresholds[symbolType]?.candle || 1.0;
        
        // Check open vs previous close (gap)
        const gapPercent = Math.abs((candle.open - previousClose) / previousClose) * 100;
        if (gapPercent > threshold * 2) {
            return {
                isSpike: true,
                reason: `Gap of ${gapPercent.toFixed(3)}%`,
                field: 'open',
            };
        }
        
        // Check high and low extremes
        const highChange = Math.abs((candle.high - previousClose) / previousClose) * 100;
        const lowChange = Math.abs((candle.low - previousClose) / previousClose) * 100;
        
        if (highChange > threshold * 3) {
            return {
                isSpike: true,
                reason: `High spike of ${highChange.toFixed(3)}%`,
                field: 'high',
            };
        }
        
        if (lowChange > threshold * 3) {
            return {
                isSpike: true,
                reason: `Low spike of ${lowChange.toFixed(3)}%`,
                field: 'low',
            };
        }
        
        return { isSpike: false };
    }

    // =========================================================================
    // PRICE TRACKING
    // =========================================================================

    /**
     * Update price tracking after accepting a valid tick
     */
    updatePrice(symbol, price) {
        const now = new Date();
        
        // Update last price
        this.lastPrices.set(symbol, { price, timestamp: now });
        
        // Update history
        if (!this.priceHistory.has(symbol)) {
            this.priceHistory.set(symbol, []);
        }
        
        const history = this.priceHistory.get(symbol);
        history.push({ price, timestamp: now });
        
        // Trim history
        while (history.length > this.historyLength) {
            history.shift();
        }
        
        // Recalculate volatility
        this.updateVolatility(symbol);
    }

    /**
     * Update volatility calculation for a symbol
     */
    updateVolatility(symbol) {
        const history = this.priceHistory.get(symbol);
        if (!history || history.length < 5) return;
        
        // Calculate standard deviation of percentage changes
        const changes = [];
        for (let i = 1; i < history.length; i++) {
            const change = Math.abs((history[i].price - history[i-1].price) / history[i-1].price) * 100;
            changes.push(change);
        }
        
        const mean = changes.reduce((a, b) => a + b, 0) / changes.length;
        const variance = changes.reduce((sum, c) => sum + Math.pow(c - mean, 2), 0) / changes.length;
        const stdDev = Math.sqrt(variance);
        
        // Use 2 standard deviations as volatility measure
        this.volatility.set(symbol, mean + 2 * stdDev);
    }

    // =========================================================================
    // SPIKE LOGGING
    // =========================================================================

    /**
     * Record a rejected spike for analysis
     */
    recordSpike(symbol, lastPrice, newPrice, changePercent) {
        this.stats.spikesRejected++;
        this.stats.spikesBySymbol[symbol] = (this.stats.spikesBySymbol[symbol] || 0) + 1;
        
        // Log to database for later analysis
        this.logSpikeToDb(symbol, lastPrice, newPrice, changePercent).catch(() => {});
    }

    /**
     * Log spike to database
     */
    async logSpikeToDb(symbol, lastPrice, newPrice, changePercent) {
        try {
            await database.pool.execute(`
                INSERT INTO pulse_health_metrics 
                (metric_name, metric_value, symbol, recorded_at)
                VALUES ('spike_rejected', ?, ?, NOW())
            `, [changePercent, symbol]);
        } catch (error) {
            // Ignore logging errors
        }
    }

    /**
     * Get spike statistics for a symbol
     */
    async getSpikeStats(symbol, days = 7) {
        const [rows] = await database.pool.execute(`
            SELECT DATE(recorded_at) as date, 
                   COUNT(*) as spike_count,
                   AVG(metric_value) as avg_change
            FROM pulse_health_metrics
            WHERE metric_name = 'spike_rejected'
            AND symbol = ?
            AND recorded_at > DATE_SUB(NOW(), INTERVAL ? DAY)
            GROUP BY DATE(recorded_at)
            ORDER BY date DESC
        `, [symbol, days]);
        
        return rows;
    }

    // =========================================================================
    // UTILITIES
    // =========================================================================

    /**
     * Get current threshold for a symbol
     */
    getThreshold(symbol) {
        const type = this.getSymbolType(symbol);
        const base = this.thresholds[type]?.tick || 0.5;
        const vol = this.volatility.get(symbol);
        
        return {
            base,
            adjusted: vol ? Math.max(base, vol * 1.5) : base,
            volatility: vol || null,
        };
    }

    /**
     * Reset spike filter for a symbol (after confirmed gap fill)
     */
    resetSymbol(symbol, correctPrice) {
        this.lastPrices.set(symbol, {
            price: correctPrice,
            timestamp: new Date()
        });
        this.priceHistory.delete(symbol);
        this.volatility.delete(symbol);
    }

    getSymbolType(symbol) {
        if (symbol.startsWith('XAU') || symbol.startsWith('XAG')) return 'metal';
        if (['BTC', 'ETH', 'XRP', 'SOL', 'ADA', 'DOGE', 'DOT', 'LTC', 'AVAX', 'MATIC']
            .some(c => symbol.startsWith(c))) return 'crypto';
        return 'forex';
    }

    getStats() {
        return {
            ...this.stats,
            trackedSymbols: this.lastPrices.size,
            volatilityTracked: this.volatility.size,
        };
    }
}

module.exports = { SpikeFilter };