// =============================================================================
// self-healing-engine.js - Main Self-Healing Forex Data Engine
// =============================================================================
// 
// Architecture:
// 1. Ticks → 1M candles → Higher timeframes (bottom-up rebuild)
// 2. Spike detection at tick level
// 3. Automatic gap detection and Dukascopy backfill
// 4. Priority system: Primary pairs processed first
// =============================================================================

require('dotenv').config();
const cron = require('node-cron');
const database = require('./database');
const { TickStore } = require('./services/tick-store');
const { CandleBuilder } = require('./services/candle-builder');
const { GapDetector } = require('./services/gap-detector');
const { DukascopyBackfill } = require('./services/dukascopy-backfill');
const { SpikeFilter } = require('./services/spike-filter');
const { HealthMonitor } = require('./services/health-monitor');

// =============================================================================
// CONFIGURATION
// =============================================================================

const CONFIG = {
    // Primary pairs - processed first, stricter monitoring
    primaryPairs: [
        'EURUSD', 'GBPUSD', 'USDJPY', 'XAUUSD', 'USDCHF',
        'AUDUSD', 'USDCAD', 'NZDUSD', 'EURGBP', 'EURJPY', 'GBPJPY'
    ],
    
    // Secondary pairs - processed after primary
    secondaryPairs: [
        'XAGUSD', 'EURCHF', 'GBPCHF', 'AUDJPY', 'EURAUD',
        'EURCAD', 'GBPAUD', 'GBPCAD', 'AUDCAD', 'AUDNZD',
        'NZDJPY', 'CADJPY'
    ],
    
    // Timeframes in build order
    timeframes: ['M1', 'M5', 'M15', 'M30', 'H1', 'H4', 'D1'],
    
    // Spike thresholds (% change that triggers rejection)
    spikeThresholds: {
        forex: { tick: 0.3, candle: 0.5 },   // 0.3% tick, 0.5% candle
        metal: { tick: 0.8, candle: 1.5 },   // More volatile
    },
    
    // Gap detection settings
    gapDetection: {
        maxGapMinutes: 5,        // Consider gap if > 5 min missing
        lookbackHours: 24,       // Check last 24 hours for gaps
        backfillBatchSize: 100,  // Candles per Dukascopy request
    },
    
    // Rate limits
    rateLimits: {
        finnhubPerSecond: 1,
        dukascopyPerMinute: 30,
    },
    
    // Tick retention
    tickRetentionHours: 48,  // Keep ticks for 48 hours
};

// =============================================================================
// MAIN ENGINE CLASS
// =============================================================================

class SelfHealingEngine {
    constructor() {
        this.tickStore = new TickStore();
        this.candleBuilder = new CandleBuilder();
        this.gapDetector = new GapDetector();
        this.dukascopyBackfill = new DukascopyBackfill();
        this.spikeFilter = new SpikeFilter(CONFIG.spikeThresholds);
        this.healthMonitor = new HealthMonitor();
        
        this.isRunning = false;
        this.stats = {
            ticksProcessed: 0,
            candlesBuilt: 0,
            spikesRejected: 0,
            gapsFixed: 0,
            lastHealthCheck: null,
        };
    }

    async init() {
        console.log('='.repeat(60));
        console.log('🚀 Self-Healing Forex Data Engine');
        console.log('='.repeat(60));
        
        await database.connect();
        await this.initTables();
        
        // Load last known prices for spike detection
        await this.spikeFilter.loadLastPrices();
        
        // Initial health check
        await this.runHealthCheck();
        
        // Schedule all cron jobs
        this.scheduleCronJobs();
        
        console.log(`✅ Engine initialized`);
        console.log(`📊 Primary pairs: ${CONFIG.primaryPairs.length}`);
        console.log(`📊 Secondary pairs: ${CONFIG.secondaryPairs.length}`);
        console.log('='.repeat(60));
        
        this.isRunning = true;
    }

    async initTables() {
        const queries = [
            // Ticks table - stores raw tick data
            `CREATE TABLE IF NOT EXISTS pulse_ticks (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                price DECIMAL(18,8) NOT NULL,
                volume DECIMAL(24,8) DEFAULT 0,
                timestamp DATETIME(3) NOT NULL,
                source ENUM('finnhub', 'dukascopy', 'interpolated') DEFAULT 'finnhub',
                is_valid TINYINT(1) DEFAULT 1,
                INDEX idx_symbol_ts (symbol, timestamp),
                INDEX idx_ts (timestamp),
                UNIQUE KEY unique_tick (symbol, timestamp, price)
            ) ENGINE=InnoDB`,
            
            // Data integrity tracking
            `CREATE TABLE IF NOT EXISTS pulse_data_integrity (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                timeframe VARCHAR(10) NOT NULL,
                date DATE NOT NULL,
                expected_candles INT DEFAULT 0,
                actual_candles INT DEFAULT 0,
                missing_candles INT DEFAULT 0,
                incomplete_candles INT DEFAULT 0,
                last_checked DATETIME,
                last_fixed DATETIME,
                status ENUM('ok', 'gaps', 'fixing', 'error') DEFAULT 'ok',
                UNIQUE KEY unique_integrity (symbol, timeframe, date),
                INDEX idx_status (status)
            ) ENGINE=InnoDB`,
            
            // Backfill queue
            `CREATE TABLE IF NOT EXISTS pulse_backfill_queue (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                timeframe VARCHAR(10) NOT NULL,
                gap_start DATETIME NOT NULL,
                gap_end DATETIME NOT NULL,
                priority TINYINT DEFAULT 5,
                status ENUM('pending', 'processing', 'completed', 'failed') DEFAULT 'pending',
                attempts INT DEFAULT 0,
                last_attempt DATETIME,
                error_message TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_status_priority (status, priority DESC),
                INDEX idx_symbol (symbol)
            ) ENGINE=InnoDB`,
            
            // Health metrics
            `CREATE TABLE IF NOT EXISTS pulse_health_metrics (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                metric_name VARCHAR(50) NOT NULL,
                metric_value DECIMAL(18,4),
                symbol VARCHAR(20),
                timeframe VARCHAR(10),
                recorded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_metric_ts (metric_name, recorded_at)
            ) ENGINE=InnoDB`,
        ];

        for (const query of queries) {
            try {
                await database.pool.execute(query);
            } catch (error) {
                if (!error.message.includes('already exists')) {
                    console.error('Table creation error:', error.message);
                }
            }
        }
        
        console.log('✅ Self-healing tables initialized');
    }

    // =========================================================================
    // CRON JOB SCHEDULING
    // =========================================================================

    scheduleCronJobs() {
        console.log('\n📅 Scheduling cron jobs...\n');

        // Every 10 seconds: Process tick buffer → 1M candles (primary pairs)
        cron.schedule('*/10 * * * * *', () => this.processTicks('primary'));
        console.log('   ✓ Tick processing (primary): every 10 seconds');

        // Every 30 seconds: Process tick buffer → 1M candles (secondary pairs)
        cron.schedule('*/30 * * * * *', () => this.processTicks('secondary'));
        console.log('   ✓ Tick processing (secondary): every 30 seconds');

        // Every minute: Build/verify 1M candles
        cron.schedule('* * * * *', () => this.buildM1Candles());
        console.log('   ✓ 1M candle builder: every minute');

        // Every 5 minutes: Build 5M candles + quick gap check
        cron.schedule('*/5 * * * *', () => this.buildHigherTF('M5'));
        console.log('   ✓ 5M candle builder: every 5 minutes');

        // Every 15 minutes: Build 15M candles
        cron.schedule('*/15 * * * *', () => this.buildHigherTF('M15'));
        console.log('   ✓ 15M candle builder: every 15 minutes');

        // Every 30 minutes: Build 30M candles
        cron.schedule('*/30 * * * *', () => this.buildHigherTF('M30'));
        console.log('   ✓ 30M candle builder: every 30 minutes');

        // Every hour: Build H1 candles + gap detection
        cron.schedule('0 * * * *', () => {
            this.buildHigherTF('H1');
            this.quickGapCheck();
        });
        console.log('   ✓ H1 candle builder + gap check: every hour');

        // Every 4 hours: Build H4 candles
        cron.schedule('0 */4 * * *', () => this.buildHigherTF('H4'));
        console.log('   ✓ H4 candle builder: every 4 hours');

        // Daily at 00:05 UTC: Build D1 candles
        cron.schedule('5 0 * * *', () => this.buildHigherTF('D1'));
        console.log('   ✓ D1 candle builder: daily at 00:05 UTC');

        // Every 5 minutes: Process backfill queue
        cron.schedule('*/5 * * * *', () => this.processBackfillQueue());
        console.log('   ✓ Backfill queue processor: every 5 minutes');

        // Daily at 02:00 UTC: Full integrity check + Dukascopy backfill
        cron.schedule('0 2 * * *', () => this.dailyIntegrityCheck());
        console.log('   ✓ Daily integrity check: 02:00 UTC');

        // Daily at 03:00 UTC: Cleanup old ticks
        cron.schedule('0 3 * * *', () => this.cleanupOldData());
        console.log('   ✓ Data cleanup: 03:00 UTC');

        // Every 10 minutes: Health check
        cron.schedule('*/10 * * * *', () => this.runHealthCheck());
        console.log('   ✓ Health check: every 10 minutes');

        console.log('');
    }

    // =========================================================================
    // TICK PROCESSING
    // =========================================================================

    async processTicks(tier = 'primary') {
        const pairs = tier === 'primary' ? CONFIG.primaryPairs : CONFIG.secondaryPairs;
        
        for (const symbol of pairs) {
            try {
                await this.tickStore.processBufferedTicks(symbol);
            } catch (error) {
                console.error(`❌ Tick processing error (${symbol}):`, error.message);
            }
        }
    }

    // =========================================================================
    // 1-MINUTE CANDLE BUILDING
    // =========================================================================

    async buildM1Candles() {
        const now = new Date();
        const minuteStart = new Date(now);
        minuteStart.setUTCSeconds(0, 0);
        minuteStart.setUTCMinutes(minuteStart.getUTCMinutes() - 1); // Previous minute
        
        const allPairs = [...CONFIG.primaryPairs, ...CONFIG.secondaryPairs];
        
        for (const symbol of allPairs) {
            try {
                await this.candleBuilder.buildM1FromTicks(symbol, minuteStart);
            } catch (error) {
                console.error(`❌ M1 build error (${symbol}):`, error.message);
            }
        }
    }

    // =========================================================================
    // HIGHER TIMEFRAME BUILDING
    // =========================================================================

    async buildHigherTF(timeframe) {
        const allPairs = [...CONFIG.primaryPairs, ...CONFIG.secondaryPairs];
        
        for (const symbol of allPairs) {
            try {
                await this.candleBuilder.buildFromM1(symbol, timeframe);
            } catch (error) {
                console.error(`❌ ${timeframe} build error (${symbol}):`, error.message);
            }
        }
        
        console.log(`✅ Built ${timeframe} candles for ${allPairs.length} pairs`);
    }

    // =========================================================================
    // GAP DETECTION & BACKFILL
    // =========================================================================

    async quickGapCheck() {
        // Quick check for recent gaps (last hour)
        const gaps = await this.gapDetector.detectRecentGaps(CONFIG.primaryPairs, 60);
        
        if (gaps.length > 0) {
            console.log(`⚠️ Found ${gaps.length} gaps, adding to backfill queue`);
            for (const gap of gaps) {
                await this.queueBackfill(gap, 10); // High priority
            }
        }
    }

    async dailyIntegrityCheck() {
        console.log('\n' + '='.repeat(60));
        console.log('🔍 Daily Integrity Check Started');
        console.log('='.repeat(60));
        
        const allPairs = [...CONFIG.primaryPairs, ...CONFIG.secondaryPairs];
        let totalGaps = 0;
        let totalIncomplete = 0;
        
        for (const symbol of allPairs) {
            for (const timeframe of ['M1', 'M5', 'H1']) {
                const result = await this.gapDetector.fullIntegrityCheck(symbol, timeframe, 7);
                
                if (result.gaps.length > 0) {
                    totalGaps += result.gaps.length;
                    for (const gap of result.gaps) {
                        await this.queueBackfill({ symbol, timeframe, ...gap }, 5);
                    }
                }
                
                if (result.incomplete.length > 0) {
                    totalIncomplete += result.incomplete.length;
                    await this.queueIncompletefix(symbol, timeframe, result.incomplete);
                }
            }
        }
        
        console.log(`📊 Found ${totalGaps} gaps, ${totalIncomplete} incomplete candles`);
        console.log('='.repeat(60) + '\n');
    }

    async queueBackfill(gap, priority = 5) {
        try {
            await database.pool.execute(`
                INSERT INTO pulse_backfill_queue 
                (symbol, timeframe, gap_start, gap_end, priority, status)
                VALUES (?, ?, ?, ?, ?, 'pending')
                ON DUPLICATE KEY UPDATE priority = GREATEST(priority, VALUES(priority))
            `, [gap.symbol, gap.timeframe || 'M1', gap.from, gap.to, priority]);
        } catch (error) {
            // Ignore duplicate errors
        }
    }

    async queueIncompletefix(symbol, timeframe, candles) {
        for (const candle of candles) {
            const from = new Date(candle.timestamp);
            const to = new Date(from.getTime() + 60000);
            await this.queueBackfill({ symbol, timeframe, from, to }, 8);
        }
    }

    async processBackfillQueue() {
        const [pending] = await database.pool.execute(`
            SELECT * FROM pulse_backfill_queue 
            WHERE status = 'pending' 
            ORDER BY priority DESC, created_at ASC 
            LIMIT 10
        `);
        
        if (pending.length === 0) return;
        
        console.log(`📥 Processing ${pending.length} backfill items`);
        
        for (const item of pending) {
            await this.processBackfillItem(item);
            await this.sleep(2000); // Rate limit Dukascopy
        }
    }

    async processBackfillItem(item) {
        try {
            await database.pool.execute(
                `UPDATE pulse_backfill_queue SET status = 'processing', last_attempt = NOW() WHERE id = ?`,
                [item.id]
            );
            
            const result = await this.dukascopyBackfill.fetchAndSave(
                item.symbol,
                item.timeframe,
                new Date(item.gap_start),
                new Date(item.gap_end)
            );
            
            await database.pool.execute(
                `UPDATE pulse_backfill_queue SET status = 'completed' WHERE id = ?`,
                [item.id]
            );
            
            this.stats.gapsFixed++;
            
        } catch (error) {
            const attempts = item.attempts + 1;
            const status = attempts >= 3 ? 'failed' : 'pending';
            
            await database.pool.execute(
                `UPDATE pulse_backfill_queue SET status = ?, attempts = ?, error_message = ? WHERE id = ?`,
                [status, attempts, error.message, item.id]
            );
        }
    }

    // =========================================================================
    // HEALTH MONITORING
    // =========================================================================

    async runHealthCheck() {
        const health = await this.healthMonitor.check(CONFIG.primaryPairs);
        this.stats.lastHealthCheck = new Date();
        
        if (health.issues.length > 0) {
            console.log(`⚠️ Health issues: ${health.issues.join(', ')}`);
        }
    }

    // =========================================================================
    // CLEANUP
    // =========================================================================

    async cleanupOldData() {
        console.log('🧹 Running cleanup...');
        
        // Delete old ticks
        const tickCutoff = new Date();
        tickCutoff.setHours(tickCutoff.getHours() - CONFIG.tickRetentionHours);
        
        const [tickResult] = await database.pool.execute(
            `DELETE FROM pulse_ticks WHERE timestamp < ?`,
            [tickCutoff]
        );
        
        // Delete completed backfill items older than 7 days
        const [backfillResult] = await database.pool.execute(
            `DELETE FROM pulse_backfill_queue WHERE status = 'completed' AND created_at < DATE_SUB(NOW(), INTERVAL 7 DAY)`
        );
        
        // Delete old health metrics older than 30 days
        await database.pool.execute(
            `DELETE FROM pulse_health_metrics WHERE recorded_at < DATE_SUB(NOW(), INTERVAL 30 DAY)`
        );
        
        console.log(`🧹 Deleted ${tickResult.affectedRows} ticks, ${backfillResult.affectedRows} backfill items`);
    }

    // =========================================================================
    // UTILITIES
    // =========================================================================

    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    getStats() {
        return {
            ...this.stats,
            tickStoreStats: this.tickStore.getStats(),
            candleBuilderStats: this.candleBuilder.getStats(),
            backfillStats: this.dukascopyBackfill.getStats(),
        };
    }

    async shutdown() {
        console.log('🛑 Shutting down self-healing engine...');
        this.isRunning = false;
        await this.tickStore.flushAll();
        await database.disconnect();
        console.log('✅ Shutdown complete');
    }
}

// =============================================================================
// EXPORTS & STARTUP
// =============================================================================

const engine = new SelfHealingEngine();

process.on('SIGTERM', () => engine.shutdown());
process.on('SIGINT', () => engine.shutdown());

module.exports = { engine, CONFIG };

if (require.main === module) {
    engine.init().catch(error => {
        console.error('❌ Failed to start:', error);
        process.exit(1);
    });
}