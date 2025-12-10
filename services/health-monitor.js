// =============================================================================
// services/health-monitor.js - System Health Monitoring
// =============================================================================
//
// Responsibilities:
// - Monitor data freshness
// - Track system metrics
// - Alert on issues
// - Generate health reports
// =============================================================================

const database = require('../database');

class HealthMonitor {
    constructor() {
        this.alertThresholds = {
            maxDataAge: 5 * 60 * 1000,      // Alert if data older than 5 minutes
            minTickRate: 10,                 // Minimum ticks per minute per pair
            maxGapsPerDay: 10,               // Maximum acceptable gaps per day
            maxIncompletePercent: 5,         // Maximum 5% incomplete candles
        };
        
        this.lastCheck = null;
        this.issues = [];
    }

    // =========================================================================
    // MAIN HEALTH CHECK
    // =========================================================================

    /**
     * Run comprehensive health check
     */
    async check(primaryPairs) {
        this.issues = [];
        const now = new Date();
        
        // 1. Check data freshness
        const freshness = await this.checkDataFreshness(primaryPairs);
        
        // 2. Check tick rate
        const tickRate = await this.checkTickRate(primaryPairs);
        
        // 3. Check gap counts
        const gapStatus = await this.checkGapStatus(primaryPairs);
        
        // 4. Check incomplete candles
        const incompleteStatus = await this.checkIncompleteCandles(primaryPairs);
        
        // 5. Check database connection
        const dbStatus = await this.checkDatabase();
        
        // 6. Check backfill queue
        const queueStatus = await this.checkBackfillQueue();
        
        // Compile results
        const health = {
            timestamp: now,
            overall: this.issues.length === 0 ? 'healthy' : 'degraded',
            issues: this.issues,
            metrics: {
                freshness,
                tickRate,
                gapStatus,
                incompleteStatus,
                dbStatus,
                queueStatus,
            },
        };
        
        // Log metrics to database
        await this.logMetrics(health.metrics);
        
        this.lastCheck = health;
        return health;
    }

    // =========================================================================
    // INDIVIDUAL CHECKS
    // =========================================================================

    /**
     * Check data freshness for each primary pair
     */
    async checkDataFreshness(pairs) {
        const results = {};
        const now = Date.now();
        
        for (const symbol of pairs) {
            try {
                const [rows] = await database.pool.execute(`
                    SELECT MAX(timestamp) as latest
                    FROM pulse_market_data
                    WHERE symbol = ? AND timeframe = 'M1'
                `, [symbol]);
                
                const latest = rows[0]?.latest;
                if (!latest) {
                    results[symbol] = { status: 'no_data', age: null };
                    this.issues.push(`${symbol}: No M1 data`);
                    continue;
                }
                
                const age = now - new Date(latest).getTime();
                results[symbol] = {
                    status: age > this.alertThresholds.maxDataAge ? 'stale' : 'fresh',
                    age,
                    latestTimestamp: latest,
                };
                
                if (age > this.alertThresholds.maxDataAge) {
                    this.issues.push(`${symbol}: Data is ${Math.round(age / 60000)} minutes old`);
                }
                
            } catch (error) {
                results[symbol] = { status: 'error', error: error.message };
            }
        }
        
        return results;
    }

    /**
     * Check tick rate (ticks per minute)
     */
    async checkTickRate(pairs) {
        const results = {};
        
        for (const symbol of pairs) {
            try {
                const [rows] = await database.pool.execute(`
                    SELECT COUNT(*) as count
                    FROM pulse_ticks
                    WHERE symbol = ?
                    AND timestamp > DATE_SUB(NOW(), INTERVAL 5 MINUTE)
                `, [symbol]);
                
                const ticksPerMinute = (rows[0]?.count || 0) / 5;
                results[symbol] = {
                    ticksPerMinute,
                    status: ticksPerMinute >= this.alertThresholds.minTickRate ? 'ok' : 'low',
                };
                
                if (ticksPerMinute < this.alertThresholds.minTickRate) {
                    this.issues.push(`${symbol}: Low tick rate (${ticksPerMinute.toFixed(1)}/min)`);
                }
                
            } catch (error) {
                results[symbol] = { status: 'error', error: error.message };
            }
        }
        
        return results;
    }

    /**
     * Check gap status from integrity table
     */
    async checkGapStatus(pairs) {
        try {
            const [rows] = await database.pool.execute(`
                SELECT symbol, SUM(missing_candles) as total_gaps
                FROM pulse_data_integrity
                WHERE symbol IN (${pairs.map(() => '?').join(',')})
                AND date >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
                GROUP BY symbol
            `, pairs);
            
            const results = {};
            for (const row of rows) {
                results[row.symbol] = {
                    gaps: row.total_gaps,
                    status: row.total_gaps > this.alertThresholds.maxGapsPerDay ? 'high' : 'ok',
                };
                
                if (row.total_gaps > this.alertThresholds.maxGapsPerDay) {
                    this.issues.push(`${row.symbol}: ${row.total_gaps} gaps in last 24h`);
                }
            }
            
            return results;
            
        } catch (error) {
            return { error: error.message };
        }
    }

    /**
     * Check incomplete candle percentage
     */
    async checkIncompleteCandles(pairs) {
        const results = {};
        
        for (const symbol of pairs) {
            try {
                const [totalRows] = await database.pool.execute(`
                    SELECT COUNT(*) as total
                    FROM pulse_market_data
                    WHERE symbol = ? AND timeframe = 'M1'
                    AND timestamp > DATE_SUB(NOW(), INTERVAL 24 HOUR)
                `, [symbol]);
                
                const [incompleteRows] = await database.pool.execute(`
                    SELECT COUNT(*) as incomplete
                    FROM pulse_market_data
                    WHERE symbol = ? AND timeframe = 'M1'
                    AND timestamp > DATE_SUB(NOW(), INTERVAL 24 HOUR)
                    AND open = high AND high = low AND low = close
                `, [symbol]);
                
                const total = totalRows[0]?.total || 0;
                const incomplete = incompleteRows[0]?.incomplete || 0;
                const percent = total > 0 ? (incomplete / total) * 100 : 0;
                
                results[symbol] = {
                    total,
                    incomplete,
                    percent,
                    status: percent > this.alertThresholds.maxIncompletePercent ? 'high' : 'ok',
                };
                
                if (percent > this.alertThresholds.maxIncompletePercent) {
                    this.issues.push(`${symbol}: ${percent.toFixed(1)}% incomplete candles`);
                }
                
            } catch (error) {
                results[symbol] = { status: 'error', error: error.message };
            }
        }
        
        return results;
    }

    /**
     * Check database connection health
     */
    async checkDatabase() {
        try {
            const start = Date.now();
            await database.pool.execute('SELECT 1');
            const latency = Date.now() - start;
            
            return {
                status: 'connected',
                latency,
            };
            
        } catch (error) {
            this.issues.push(`Database: ${error.message}`);
            return {
                status: 'error',
                error: error.message,
            };
        }
    }

    /**
     * Check backfill queue status
     */
    async checkBackfillQueue() {
        try {
            const [rows] = await database.pool.execute(`
                SELECT status, COUNT(*) as count
                FROM pulse_backfill_queue
                GROUP BY status
            `);
            
            const counts = {};
            for (const row of rows) {
                counts[row.status] = row.count;
            }
            
            const pending = counts.pending || 0;
            const failed = counts.failed || 0;
            
            if (pending > 50) {
                this.issues.push(`Backfill queue: ${pending} pending items`);
            }
            
            if (failed > 10) {
                this.issues.push(`Backfill queue: ${failed} failed items`);
            }
            
            return {
                pending,
                processing: counts.processing || 0,
                completed: counts.completed || 0,
                failed,
            };
            
        } catch (error) {
            return { status: 'error', error: error.message };
        }
    }

    // =========================================================================
    // METRIC LOGGING
    // =========================================================================

    /**
     * Log health metrics to database
     */
    async logMetrics(metrics) {
        const timestamp = new Date();
        const records = [];
        
        // Flatten metrics for logging
        if (metrics.freshness) {
            for (const [symbol, data] of Object.entries(metrics.freshness)) {
                if (data.age) {
                    records.push(['data_age_ms', data.age, symbol, null]);
                }
            }
        }
        
        if (metrics.tickRate) {
            for (const [symbol, data] of Object.entries(metrics.tickRate)) {
                if (data.ticksPerMinute !== undefined) {
                    records.push(['tick_rate', data.ticksPerMinute, symbol, null]);
                }
            }
        }
        
        if (metrics.dbStatus?.latency) {
            records.push(['db_latency_ms', metrics.dbStatus.latency, null, null]);
        }
        
        if (metrics.queueStatus?.pending !== undefined) {
            records.push(['backfill_queue_pending', metrics.queueStatus.pending, null, null]);
        }
        
        // Batch insert
        if (records.length > 0) {
            const placeholders = records.map(() => '(?, ?, ?, ?, NOW())').join(',');
            const values = records.flat();
            
            try {
                await database.pool.execute(`
                    INSERT INTO pulse_health_metrics 
                    (metric_name, metric_value, symbol, timeframe, recorded_at)
                    VALUES ${placeholders}
                `, values);
            } catch (error) {
                // Ignore logging errors
            }
        }
    }

    // =========================================================================
    // REPORTS
    // =========================================================================

    /**
     * Get health summary for API endpoint
     */
    getHealthSummary() {
        if (!this.lastCheck) {
            return { status: 'unknown', message: 'No health check performed yet' };
        }
        
        return {
            status: this.lastCheck.overall,
            timestamp: this.lastCheck.timestamp,
            issues: this.lastCheck.issues,
            issueCount: this.lastCheck.issues.length,
        };
    }

    /**
     * Get detailed health report
     */
    async getDetailedReport(pairs, days = 7) {
        // Get historical metrics
        const [metrics] = await database.pool.execute(`
            SELECT metric_name, AVG(metric_value) as avg_value,
                   MIN(metric_value) as min_value, MAX(metric_value) as max_value,
                   COUNT(*) as sample_count
            FROM pulse_health_metrics
            WHERE recorded_at > DATE_SUB(NOW(), INTERVAL ? DAY)
            GROUP BY metric_name
        `, [days]);
        
        // Get gap history
        const [gaps] = await database.pool.execute(`
            SELECT symbol, date, missing_candles, incomplete_candles, status
            FROM pulse_data_integrity
            WHERE symbol IN (${pairs.map(() => '?').join(',')})
            AND date > DATE_SUB(CURDATE(), INTERVAL ? DAY)
            ORDER BY date DESC
        `, [...pairs, days]);
        
        return {
            currentStatus: this.getHealthSummary(),
            historicalMetrics: metrics,
            gapHistory: gaps,
        };
    }
}

module.exports = { HealthMonitor };