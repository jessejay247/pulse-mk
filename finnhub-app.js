// =============================================================================
// finnhub-app.js - Data Ingestion Service with Spike Filter
// =============================================================================

require('dotenv').config();

const WebSocket = require('ws');
const cron = require('node-cron');
const database = require('./database');
const { SYMBOLS, getSymbolByFinnhub, toInternalSymbol } = require('./config/symbols');
const { isMarketOpenForSymbol, isForexMarketOpen } = require('./config/market-hours');

const ENABLE_GAP_RECOVERY = process.env.ENABLE_GAP_RECOVERY === 'true';

class FinnhubDataIngestion {
    constructor() {
        this.apiKey = process.env.FINNHUB_API_KEY || 'd4rljk9r01qgts2osudgd4rljk9r01qgts2osue0';
        this.candleBuffers = new Map();
        this.ws = null;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 10;
        this.tickCallbacks = [];
        this.isConnected = false;
        this.maxBufferSize = 500;
        this.lastCleanup = Date.now();
        
        // 🎯 NEW: Price tracking for spike detection
        this.lastPrices = new Map();
        this.priceHistory = new Map(); // Rolling window for volatility calc
        
        // Spike filter thresholds (percentage)
        this.spikeThresholds = {
            forex: 0.5,    // 0.5% max tick-to-tick change for forex
            metal: 1.0,    // 1% for metals (more volatile)
            crypto: 3.0,   // 3% for crypto (very volatile)
            stock: 5.0     // 5% for stocks (gaps common)
        };
    }

    async init() {
        await database.connect();
        
        // Load last known prices from DB to avoid false spikes on startup
        await this.loadLastPrices();
        
        if (ENABLE_GAP_RECOVERY) {
            try {
                const gapRecovery = require('./services/gap-recovery');
                await gapRecovery.recoverOnStartup();
                gapRecovery.startPeriodicCheck(120);
            } catch (error) {
                console.log('⚠️ Gap recovery disabled or failed:', error.message);
            }
        }
        
        await this.initWebSocket();
        this.startCleanupJob();
        this.startBufferFlushJob();
        this.startMemoryCleanup();
        
        console.log('🚀 Finnhub Data Ingestion Started (with spike filter)');
        console.log(`📈 Tracking ${Object.keys(SYMBOLS).length} symbols`);
        this.logMemoryUsage();
    }

    // 🎯 NEW: Load last prices from database on startup
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
            
            console.log(`📊 Loaded ${this.lastPrices.size} last prices from DB`);
        } catch (error) {
            console.error('⚠️ Could not load last prices:', error.message);
        }
    }

    // 🎯 NEW: Spike detection
    isSpike(symbol, symbolType, newPrice) {
        const lastData = this.lastPrices.get(symbol);
        
        // No last price = can't detect spike, accept it
        if (!lastData) {
            return { isSpike: false, reason: 'no_history' };
        }
        
        const lastPrice = lastData.price;
        const timeDiff = Date.now() - lastData.timestamp.getTime();
        
        // If last price is very old (>5 min), be more lenient
        const isStale = timeDiff > 5 * 60 * 1000;
        
        const changePercent = Math.abs((newPrice - lastPrice) / lastPrice) * 100;
        let threshold = this.spikeThresholds[symbolType] || 1.0;
        
        // Double threshold for stale data
        if (isStale) {
            threshold *= 2;
        }
        
        if (changePercent > threshold) {
            return {
                isSpike: true,
                reason: `${changePercent.toFixed(3)}% change exceeds ${threshold}% threshold`,
                lastPrice,
                newPrice,
                changePercent
            };
        }
        
        return { isSpike: false, changePercent };
    }

    // 🎯 NEW: Update price tracking
    updatePriceTracking(symbol, price) {
        this.lastPrices.set(symbol, {
            price,
            timestamp: new Date()
        });
        
        // Keep rolling history (last 20 prices) for volatility calculation
        if (!this.priceHistory.has(symbol)) {
            this.priceHistory.set(symbol, []);
        }
        const history = this.priceHistory.get(symbol);
        history.push(price);
        if (history.length > 20) {
            history.shift();
        }
    }

    logMemoryUsage() {
        const used = process.memoryUsage();
        console.log(`📊 Memory: RSS=${Math.round(used.rss / 1024 / 1024)}MB, Heap=${Math.round(used.heapUsed / 1024 / 1024)}MB`);
    }

    startMemoryCleanup() {
        setInterval(() => {
            this.cleanupBuffers();
            this.logMemoryUsage();
            if (global.gc) global.gc();
        }, 5 * 60 * 1000);
    }

    getSymbolType(internalSymbol) {
        for (const [symbol, config] of Object.entries(SYMBOLS)) {
            if (symbol.replace('/', '') === internalSymbol) {
                return config.type;
            }
        }
        return 'forex';
    }

    shouldSaveCandle(candle) {
        const symbolType = this.getSymbolType(candle.symbol);
        const marketStatus = isMarketOpenForSymbol(symbolType, candle.timestamp);
        return marketStatus.open;
    }

    cleanupBuffers() {
        const now = Date.now();
        const maxAge = 5 * 60 * 1000;
        let cleaned = 0;
        let skippedClosed = 0;
        
        for (const [key, candle] of this.candleBuffers.entries()) {
            if (now - candle.lastUpdate > maxAge) {
                if (this.shouldSaveCandle(candle)) {
                    this.saveCandleToDatabase(candle);
                } else {
                    skippedClosed++;
                }
                this.candleBuffers.delete(key);
                cleaned++;
            }
        }
        
        if (this.candleBuffers.size > this.maxBufferSize) {
            const entries = Array.from(this.candleBuffers.entries())
                .sort((a, b) => a[1].lastUpdate - b[1].lastUpdate);
            
            const toRemove = entries.slice(0, entries.length - this.maxBufferSize);
            for (const [key, candle] of toRemove) {
                if (this.shouldSaveCandle(candle)) {
                    this.saveCandleToDatabase(candle);
                } else {
                    skippedClosed++;
                }
                this.candleBuffers.delete(key);
                cleaned++;
            }
        }
        
        if (cleaned > 0) {
            console.log(`🧹 Cleaned ${cleaned} buffers, ${skippedClosed} skipped`);
        }
    }

    async initWebSocket() {
        if (this.ws) {
            this.ws.terminate();
        }

        this.ws = new WebSocket(`wss://ws.finnhub.io?token=${this.apiKey}`);

        this.ws.on('open', () => {
            console.log('✅ Finnhub WebSocket connected');
            this.isConnected = true;
            this.reconnectAttempts = 0;
            
            for (const [symbol, config] of Object.entries(SYMBOLS)) {
                this.ws.send(JSON.stringify({
                    type: 'subscribe',
                    symbol: config.finnhub
                }));
            }
            console.log(`📊 Subscribed to ${Object.keys(SYMBOLS).length} symbols`);
        });

        this.ws.on('message', (data) => {
            try {
                const message = JSON.parse(data);
                
                if (message.type === 'trade' && message.data) {
                    const trades = message.data.slice(0, 5);
                    trades.forEach(trade => this.processTrade(trade));
                } else if (message.type === 'ping') {
                    this.ws.send(JSON.stringify({ type: 'pong' }));
                }
            } catch (error) {
                console.error('❌ Error processing message:', error.message);
            }
        });

        this.ws.on('error', (error) => {
            console.error('❌ Finnhub WebSocket error:', error.message);
            this.isConnected = false;
        });

        this.ws.on('close', () => {
            console.log('🔌 Finnhub WebSocket disconnected');
            this.isConnected = false;
            this.scheduleReconnect();
        });

        this.heartbeatInterval = setInterval(() => {
            if (this.ws?.readyState === WebSocket.OPEN) {
                this.ws.send(JSON.stringify({ type: 'ping' }));
            }
        }, 30000);
    }

    scheduleReconnect() {
        if (this.reconnectAttempts >= this.maxReconnectAttempts) {
            console.error('❌ Max reconnection attempts reached');
            return;
        }

        const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);
        this.reconnectAttempts++;
        
        console.log(`🔄 Reconnecting in ${delay/1000}s (attempt ${this.reconnectAttempts})`);
        setTimeout(() => this.initWebSocket(), delay);
    }

    processTrade(trade) {
        const { p: price, s: finnhubSymbol, t: timestamp, v: volume } = trade;
        
        const symbolInfo = getSymbolByFinnhub(finnhubSymbol);
        if (!symbolInfo) return;

        const { symbol, type } = symbolInfo;
        const internalSymbol = toInternalSymbol(symbol);
        const tradeTime = new Date(timestamp);

        // 🎯 SPIKE DETECTION - Check before processing
        const spikeCheck = this.isSpike(internalSymbol, type, price);
        
        if (spikeCheck.isSpike) {
            console.warn(`⚠️ SPIKE REJECTED: ${internalSymbol} ${spikeCheck.lastPrice?.toFixed(5)} → ${price.toFixed(5)} (${spikeCheck.reason})`);
            return; // Don't process this tick
        }

        // Update price tracking (only for valid prices)
        this.updatePriceTracking(internalSymbol, price);

        // Market hours check
        const marketStatus = isMarketOpenForSymbol(type, tradeTime);
        
        // Always emit tick for real-time display
        this.emitTick({
            symbol: internalSymbol,
            displaySymbol: symbol,
            price,
            volume,
            timestamp: tradeTime,
            marketClosed: !marketStatus.open,
            reason: marketStatus.reason
        });

        // Don't create candles if market is closed
        if (!marketStatus.open) {
            return;
        }

        const timeframes = process.env.NODE_ENV === 'production' 
            ? ['M1', 'M5', 'H1', 'D1']
            : ['M1', 'M5', 'M15', 'M30', 'H1', 'H4', 'D1'];
            
        timeframes.forEach(timeframe => {
            this.updateCandle(internalSymbol, timeframe, tradeTime, price, volume || 0);
        });
    }

    emitTick(tick) {
        const callbacks = this.tickCallbacks.slice(0, 10);
        callbacks.forEach(callback => {
            try {
                callback(tick);
            } catch (error) {
                console.error('❌ Error in tick callback:', error.message);
            }
        });
    }

    onTick(callback) {
        if (this.tickCallbacks.length < 10) {
            this.tickCallbacks.push(callback);
        }
    }

    getCandleStartTime(timestamp, timeframe) {
        const time = new Date(timestamp);
        
        // 🎯 FIX: Use UTC methods for consistency
        switch (timeframe) {
            case 'M1':
                time.setUTCSeconds(0, 0);
                break;
            case 'M5':
                time.setUTCMinutes(Math.floor(time.getUTCMinutes() / 5) * 5, 0, 0);
                break;
            case 'M15':
                time.setUTCMinutes(Math.floor(time.getUTCMinutes() / 15) * 15, 0, 0);
                break;
            case 'M30':
                time.setUTCMinutes(Math.floor(time.getUTCMinutes() / 30) * 30, 0, 0);
                break;
            case 'H1':
                time.setUTCMinutes(0, 0, 0);
                break;
            case 'H4':
                time.setUTCHours(Math.floor(time.getUTCHours() / 4) * 4, 0, 0, 0);
                break;
            case 'D1':
                time.setUTCHours(0, 0, 0, 0);
                break;
        }
        
        return time;
    }

    updateCandle(symbol, timeframe, tradeTime, price, volume) {
        const candleStart = this.getCandleStartTime(tradeTime, timeframe);
        const bufferKey = `${symbol}_${timeframe}_${candleStart.getTime()}`;

        if (!this.candleBuffers.has(bufferKey)) {
            if (this.candleBuffers.size >= this.maxBufferSize) {
                this.cleanupBuffers();
            }
            
            this.candleBuffers.set(bufferKey, {
                symbol,
                timeframe,
                timestamp: candleStart,
                open: price,
                high: price,
                low: price,
                close: price,
                volume: volume,
                tradeCount: 1,
                lastUpdate: Date.now()
            });
        } else {
            const candle = this.candleBuffers.get(bufferKey);
            candle.high = Math.max(candle.high, price);
            candle.low = Math.min(candle.low, price);
            candle.close = price;
            candle.volume += volume;
            candle.tradeCount++;
            candle.lastUpdate = Date.now();
        }

        const candle = this.candleBuffers.get(bufferKey);
        if (candle.tradeCount >= 5 || (Date.now() - candle.lastUpdate) > 15000) {
            if (this.shouldSaveCandle(candle)) {
                this.saveCandleToDatabase(candle);
            }
        }
    }

    async saveCandleToDatabase(candle) {
        try {
            await database.pool.execute(`
                INSERT INTO pulse_market_data 
                (symbol, timeframe, timestamp, open, high, low, close, volume, spread)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
                ON DUPLICATE KEY UPDATE
                high = GREATEST(high, VALUES(high)),
                low = LEAST(low, VALUES(low)),
                close = VALUES(close),
                volume = volume + VALUES(volume)
            `, [
                candle.symbol,
                candle.timeframe,
                candle.timestamp,
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume
            ]);
        } catch (error) {
            console.error('❌ Error saving candle:', error.message);
        }
    }

    startCleanupJob() {
        cron.schedule('0 * * * *', async () => {
            await database.cleanupOldCandles();
            this.logMemoryUsage();
        });
    }

    startBufferFlushJob() {
        cron.schedule('*/30 * * * * *', () => {
            const now = Date.now();
            let flushed = 0;
            let skippedClosed = 0;
            
            for (const [key, candle] of this.candleBuffers.entries()) {
                if (now - candle.lastUpdate > 30000) {
                    if (this.shouldSaveCandle(candle)) {
                        this.saveCandleToDatabase(candle);
                        flushed++;
                    } else {
                        skippedClosed++;
                    }
                    this.candleBuffers.delete(key);
                }
            }
            
            if (flushed > 0 || skippedClosed > 0) {
                console.log(`💾 Flushed ${flushed} candles, ${skippedClosed} skipped`);
            }
        });
    }

    getStatus() {
        const forexStatus = isForexMarketOpen();
        return {
            connected: this.isConnected,
            symbolCount: Object.keys(SYMBOLS).length,
            bufferSize: this.candleBuffers.size,
            trackedPrices: this.lastPrices.size,
            reconnectAttempts: this.reconnectAttempts,
            forexMarketOpen: forexStatus.open,
            marketSession: forexStatus.session || forexStatus.reason
        };
    }

    async shutdown() {
        console.log('🛑 Shutting down data ingestion...');
        
        for (const candle of this.candleBuffers.values()) {
            if (this.shouldSaveCandle(candle)) {
                await this.saveCandleToDatabase(candle);
            }
        }
        this.candleBuffers.clear();
        
        if (this.heartbeatInterval) clearInterval(this.heartbeatInterval);
        if (this.ws) this.ws.close();
        
        await database.disconnect();
        console.log('✅ Shutdown complete');
    }
}

const dataIngestion = new FinnhubDataIngestion();

process.on('SIGTERM', () => dataIngestion.shutdown());
process.on('SIGINT', () => dataIngestion.shutdown());

module.exports = dataIngestion;

if (require.main === module) {
    dataIngestion.init().catch(error => {
        console.error('❌ Failed to start:', error);
        process.exit(1);
    });
}