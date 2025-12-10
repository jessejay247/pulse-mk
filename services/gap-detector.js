// =============================================================================
// services/gap-detector.js - Data Gap Detection & Integrity Checking
// =============================================================================
//
// Responsibilities:
// - Detect missing candles in time series
// - Identify incomplete candles
// - Track data integrity metrics
// - Queue gaps for backfill
// =============================================================================

const database = require('../database');
const { isMarketOpenForSymbol, isForexMarketOpen } = require('../config/market-hours');

class GapDetector {
    constructor() {
        // Timeframe durations in milliseconds
        this.timeframeDurations = {
            M1:  60 * 1000,
            M5:  5 * 60 * 1000,
            M15: 15 * 60 * 1000,
            M30: 30 * 60 * 1000,
            H1:  60 * 60 * 1000,
            H4:  4 * 60 * 60 * 1000,
            D1:  24 * 60 * 60 * 1000,
        };
        
        this.stats = {
            checksPerformed: 0,
            gapsFound: 0,
            incompleteFound: 0,
        };
    }

    // =========================================================================
    // QUICK GAP DETECTION (for hourly checks)
    // =========================================================================

    /**
     * Quick check for gaps in the last N minutes
     */
    async detectRecentGaps(symbols, lookbackMinutes = 60) {
        const gaps = [];
        const now = new Date();
        const from = new Date(now.getTime() - lookbackMinutes * 60 * 1000);
        
        for (const symbol of symbols) {
            const symbolGaps = await this.detectGapsInRange(symbol, 'M1', from, now);
            gaps.push(...symbolGaps);
        }
        
        return gaps;
    }

    /**
     * Detect gaps in a specific time range
     */
    async detectGapsInRange(symbol, timeframe, from, to) {
        const gaps = [];
        const duration = this.timeframeDurations[timeframe];
        const symbolType = this.getSymbolType(symbol);
        
        // Get existing candles
        const [candles] = await database.pool.execute(`
            SELECT timestamp
            FROM pulse_market_data
            WHERE symbol = ? AND timeframe = ?
            AND timestamp >= ? AND timestamp < ?
            ORDER BY timestamp ASC
        `, [symbol, timeframe, from, to]);
        
        if (candles.length === 0) {
            // Check if market was even open
            if (this.wasMarketOpen(symbolType, from, to)) {
                gaps.push({
                    symbol,
                    timeframe,
                    from: new Date(from),
                    to: new Date(to),
                    type: 'full_gap',
                    expectedCandles: Math.floor((to - from) / duration),
                });
            }
            return gaps;
        }
        
        // Check for gaps between candles
        const timestamps = candles.map(c => new Date(c.timestamp).getTime());
        timestamps.sort((a, b) => a - b);
        
        // Check gap at start
        const firstExpected = this.alignToTimeframe(from, timeframe);
        if (timestamps[0] - firstExpected.getTime() > duration * 2) {
            const gapEnd = new Date(timestamps[0]);
            if (this.wasMarketOpen(symbolType, firstExpected, gapEnd)) {
                gaps.push({
                    symbol,
                    timeframe,
                    from: firstExpected,
                    to: gapEnd,
                    type: 'start_gap',
                    missingCandles: Math.floor((timestamps[0] - firstExpected.getTime()) / duration),
                });
            }
        }
        
        // Check gaps between candles
        for (let i = 1; i < timestamps.length; i++) {
            const expected = timestamps[i - 1] + duration;
            const actual = timestamps[i];
            const gapMs = actual - expected;
            
            // If gap > 2x the timeframe duration, it's a significant gap
            if (gapMs > duration * 2) {
                const gapStart = new Date(expected);
                const gapEnd = new Date(actual);
                
                // Only flag if market was open during this gap
                if (this.wasMarketOpen(symbolType, gapStart, gapEnd)) {
                    gaps.push({
                        symbol,
                        timeframe,
                        from: gapStart,
                        to: gapEnd,
                        type: 'mid_gap',
                        missingCandles: Math.floor(gapMs / duration),
                    });
                }
            }
        }
        
        // Check gap at end
        const lastExpected = this.alignToTimeframe(to, timeframe);
        const lastCandle = timestamps[timestamps.length - 1];
        if (lastExpected.getTime() - lastCandle > duration * 2) {
            const gapStart = new Date(lastCandle + duration);
            if (this.wasMarketOpen(symbolType, gapStart, lastExpected)) {
                gaps.push({
                    symbol,
                    timeframe,
                    from: gapStart,
                    to: lastExpected,
                    type: 'end_gap',
                    missingCandles: Math.floor((lastExpected.getTime() - lastCandle) / duration),
                });
            }
        }
        
        this.stats.gapsFound += gaps.length;
        return gaps;
    }

    // =========================================================================
    // FULL INTEGRITY CHECK (for daily verification)
    // =========================================================================

    /**
     * Full integrity check for a symbol/timeframe over N days
     */
    async fullIntegrityCheck(symbol, timeframe, days = 7) {
        this.stats.checksPerformed++;
        
        const to = new Date();
        const from = new Date(to.getTime() - days * 24 * 60 * 60 * 1000);
        
        // Detect gaps
        const gaps = await this.detectGapsInRange(symbol, timeframe, from, to);
        
        // Find incomplete candles
        const incomplete = await this.findIncompleteCandles(symbol, timeframe, from, to);
        
        // Calculate coverage
        const coverage = await this.calculateCoverage(symbol, timeframe, from, to);
        
        // Update integrity tracking table
        await this.updateIntegrityRecord(symbol, timeframe, {
            gaps: gaps.length,
            incomplete: incomplete.length,
            coverage,
        });
        
        return {
            symbol,
            timeframe,
            from,
            to,
            gaps,
            incomplete,
            coverage,
            isHealthy: gaps.length === 0 && incomplete.length === 0 && coverage >= 0.95,
        };
    }

    /**
     * Find candles with identical OHLC values (incomplete)
     */
    async findIncompleteCandles(symbol, timeframe, from, to) {
        const [rows] = await database.pool.execute(`
            SELECT id, timestamp, open, high, low, close
            FROM pulse_market_data
            WHERE symbol = ? AND timeframe = ?
            AND timestamp >= ? AND timestamp < ?
            AND open = high AND high = low AND low = close
            ORDER BY timestamp ASC
        `, [symbol, timeframe, from, to]);
        
        this.stats.incompleteFound += rows.length;
        return rows;
    }

    /**
     * Calculate data coverage percentage
     */
    async calculateCoverage(symbol, timeframe, from, to) {
        const duration = this.timeframeDurations[timeframe];
        const symbolType = this.getSymbolType(symbol);
        
        // Calculate expected candles (accounting for market hours)
        const expectedCandles = this.calculateExpectedCandles(symbolType, timeframe, from, to);
        
        // Count actual candles
        const [result] = await database.pool.execute(`
            SELECT COUNT(*) as count
            FROM pulse_market_data
            WHERE symbol = ? AND timeframe = ?
            AND timestamp >= ? AND timestamp < ?
        `, [symbol, timeframe, from, to]);
        
        const actualCandles = result[0].count;
        
        return expectedCandles > 0 ? actualCandles / expectedCandles : 1;
    }

    /**
     * Calculate expected candles for a time range (accounting for market hours)
     */
    calculateExpectedCandles(symbolType, timeframe, from, to) {
        const duration = this.timeframeDurations[timeframe];
        let expected = 0;
        let current = new Date(from);
        
        while (current < to) {
            const marketStatus = isMarketOpenForSymbol(symbolType, current);
            if (marketStatus.open) {
                expected++;
            }
            current = new Date(current.getTime() + duration);
        }
        
        return expected;
    }

    // =========================================================================
    // INTEGRITY TRACKING
    // =========================================================================

    /**
     * Update the integrity tracking record
     */
    async updateIntegrityRecord(symbol, timeframe, metrics) {
        const date = new Date().toISOString().split('T')[0];
        const status = metrics.gaps > 0 || metrics.incomplete > 0 ? 'gaps' : 'ok';
        
        await database.pool.execute(`
            INSERT INTO pulse_data_integrity 
            (symbol, timeframe, date, expected_candles, actual_candles, 
             missing_candles, incomplete_candles, last_checked, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, NOW(), ?)
            ON DUPLICATE KEY UPDATE
                missing_candles = VALUES(missing_candles),
                incomplete_candles = VALUES(incomplete_candles),
                last_checked = NOW(),
                status = VALUES(status)
        `, [
            symbol, timeframe, date,
            Math.round(metrics.coverage * 100),
            Math.round(metrics.coverage * 100),
            metrics.gaps,
            metrics.incomplete,
            status
        ]);
    }

    /**
     * Get integrity summary for all primary pairs
     */
    async getIntegritySummary(symbols, days = 7) {
        const [rows] = await database.pool.execute(`
            SELECT symbol, timeframe, 
                   SUM(missing_candles) as total_gaps,
                   SUM(incomplete_candles) as total_incomplete,
                   MIN(status) as worst_status
            FROM pulse_data_integrity
            WHERE symbol IN (${symbols.map(() => '?').join(',')})
            AND date >= DATE_SUB(CURDATE(), INTERVAL ? DAY)
            GROUP BY symbol, timeframe
        `, [...symbols, days]);
        
        return rows;
    }

    // =========================================================================
    // UTILITIES
    // =========================================================================

    /**
     * Check if market was open during a time range
     */
    wasMarketOpen(symbolType, from, to) {
        // Simple check - just check the midpoint
        const mid = new Date((new Date(from).getTime() + new Date(to).getTime()) / 2);
        const status = isMarketOpenForSymbol(symbolType, mid);
        return status.open;
    }

    /**
     * Align a timestamp to a timeframe boundary
     */
    alignToTimeframe(date, timeframe) {
        const d = new Date(date);
        d.setUTCSeconds(0, 0);
        
        switch (timeframe) {
            case 'M1':
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

    getSymbolType(symbol) {
        if (symbol.startsWith('XAU') || symbol.startsWith('XAG')) return 'metal';
        return 'forex';
    }

    getStats() {
        return { ...this.stats };
    }
}

module.exports = { GapDetector };