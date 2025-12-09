// =============================================================================
// finnhub-app.js - Data Ingestion Service with Market Hours (Memory Optimized)
// =============================================================================

require('dotenv').config();

const WebSocket = require('ws');
const cron = require('node-cron');
const database = require('./database');
const { SYMBOLS, getSymbolByFinnhub, toInternalSymbol } = require('./config/symbols');
const { isMarketOpenForSymbol } = require('./config/market-hours');

// Skip gap recovery on free tier to save memory - can be enabled on paid plans
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
        
        // Memory management
        this.maxBufferSize = 500; // Limit buffer entries
        this.lastCleanup = Date.now();
    }

    async init() {
        await database.connect();
        
        // Only run gap recovery if explicitly enabled (uses lots of memory)
        if (ENABLE_GAP_RECOVERY) {
            try {
                const gapRecovery = require('./services/gap-recovery');
                await gapRecovery.recoverOnStartup();
                gapRecovery.startPeriodicCheck(120); // Less frequent on free tier
            } catch (error) {
                console.log('⚠️ Gap recovery disabled or failed:', error.message);
            }
        } else {
            console.log('ℹ️ Gap recovery disabled for memory optimization');
        }
        
        await this.initWebSocket();
        this.startCleanupJob();
        this.startBufferFlushJob();
        
        // Periodic memory cleanup
        this.startMemoryCleanup();
        
        console.log('🚀 Finnhub Data Ingestion Started!');
        console.log(`📈 Tracking ${Object.keys(SYMBOLS).length} symbols`);
        this.logMemoryUsage();
    }

    logMemoryUsage() {
        const used = process.memoryUsage();
        console.log(`📊 Memory: RSS=${Math.round(used.rss / 1024 / 1024)}MB, Heap=${Math.round(used.heapUsed / 1024 / 1024)}MB/${Math.round(used.heapTotal / 1024 / 1024)}MB`);
    }

    startMemoryCleanup() {
        // Run garbage collection hints and cleanup every 5 minutes
        setInterval(() => {
            this.cleanupBuffers();
            this.logMemoryUsage();
            
            // Force GC if available (run node with --expose-gc)
            if (global.gc) {
                global.gc();
            }
        }, 5 * 60 * 1000);
    }

    cleanupBuffers() {
        const now = Date.now();
        const maxAge = 5 * 60 * 1000; // 5 minutes
        let cleaned = 0;
        
        for (const [key, candle] of this.candleBuffers.entries()) {
            if (now - candle.lastUpdate > maxAge) {
                this.saveCandleToDatabase(candle);
                this.candleBuffers.delete(key);
                cleaned++;
            }
        }
        
        // If still too many buffers, force cleanup oldest ones
        if (this.candleBuffers.size > this.maxBufferSize) {
            const entries = Array.from(this.candleBuffers.entries())
                .sort((a, b) => a[1].lastUpdate - b[1].lastUpdate);
            
            const toRemove = entries.slice(0, entries.length - this.maxBufferSize);
            for (const [key, candle] of toRemove) {
                this.saveCandleToDatabase(candle);
                this.candleBuffers.delete(key);
                cleaned++;
            }
        }
        
        if (cleaned > 0) {
            console.log(`🧹 Cleaned ${cleaned} stale buffers, ${this.candleBuffers.size} remaining`);
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
            
            // Subscribe to all symbols
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
                    // Process only first few trades per batch to reduce memory pressure
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

        // Heartbeat
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
        
        // Get our symbol config
        const symbolInfo = getSymbolByFinnhub(finnhubSymbol);
        if (!symbolInfo) {
            return;
        }

        const { symbol, type } = symbolInfo;
        const internalSymbol = toInternalSymbol(symbol);
        const tradeTime = new Date(timestamp);

        // Market hours check
        const marketStatus = isMarketOpenForSymbol(type, tradeTime);
        if (!marketStatus.open) {
            this.emitTick({
                symbol: internalSymbol,
                displaySymbol: symbol,
                price,
                volume,
                timestamp: tradeTime,
                marketClosed: true,
                reason: marketStatus.reason
            });
            return;
        }

        // Emit tick for real-time consumers
        this.emitTick({
            symbol: internalSymbol,
            displaySymbol: symbol,
            price,
            volume,
            timestamp: tradeTime,
            marketClosed: false
        });

        // Only update essential timeframes to save memory
        // Reduced from 7 timeframes to 4 for free tier
        const timeframes = process.env.NODE_ENV === 'production' 
            ? ['M1', 'M5', 'H1', 'D1']  // Production: fewer timeframes
            : ['M1', 'M5', 'M15', 'M30', 'H1', 'H4', 'D1']; // Dev: all timeframes
            
        timeframes.forEach(timeframe => {
            this.updateCandle(internalSymbol, timeframe, tradeTime, price, volume || 0);
        });
    }

    emitTick(tick) {
        // Limit number of callbacks to prevent memory leaks
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
        // Limit total callbacks
        if (this.tickCallbacks.length < 10) {
            this.tickCallbacks.push(callback);
        }
    }

    getCandleStartTime(timestamp, timeframe) {
        const time = new Date(timestamp);
        
        switch (timeframe) {
            case 'M1':
                time.setSeconds(0, 0);
                break;
            case 'M5':
                time.setMinutes(Math.floor(time.getMinutes() / 5) * 5, 0, 0);
                break;
            case 'M15':
                time.setMinutes(Math.floor(time.getMinutes() / 15) * 15, 0, 0);
                break;
            case 'M30':
                time.setMinutes(Math.floor(time.getMinutes() / 30) * 30, 0, 0);
                break;
            case 'H1':
                time.setMinutes(0, 0, 0);
                break;
            case 'H4':
                time.setHours(Math.floor(time.getHours() / 4) * 4, 0, 0, 0);
                break;
            case 'D1':
                time.setHours(0, 0, 0, 0);
                break;
        }
        
        return time;
    }

    updateCandle(symbol, timeframe, tradeTime, price, volume) {
        const candleStart = this.getCandleStartTime(tradeTime, timeframe);
        const bufferKey = `${symbol}_${timeframe}_${candleStart.getTime()}`;

        if (!this.candleBuffers.has(bufferKey)) {
            // Check buffer size before adding
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

        // Save more frequently to keep buffer small
        const candle = this.candleBuffers.get(bufferKey);
        if (candle.tradeCount >= 5 || (Date.now() - candle.lastUpdate) > 15000) {
            this.saveCandleToDatabase(candle);
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
        // Cleanup old candles every hour
        cron.schedule('0 * * * *', async () => {
            await database.cleanupOldCandles();
            this.logMemoryUsage();
        });
    }

    startBufferFlushJob() {
        // Flush stale buffers every 30 seconds (more frequent for memory management)
        cron.schedule('*/30 * * * * *', () => {
            const now = Date.now();
            let flushed = 0;
            
            for (const [key, candle] of this.candleBuffers.entries()) {
                if (now - candle.lastUpdate > 30000) {
                    this.saveCandleToDatabase(candle);
                    this.candleBuffers.delete(key);
                    flushed++;
                }
            }
            
            if (flushed > 0) {
                console.log(`💾 Flushed ${flushed} candles to DB`);
            }
        });
    }

    getStatus() {
        return {
            connected: this.isConnected,
            symbolCount: Object.keys(SYMBOLS).length,
            bufferSize: this.candleBuffers.size,
            reconnectAttempts: this.reconnectAttempts
        };
    }

    async shutdown() {
        console.log('🛑 Shutting down data ingestion...');
        
        // Save all buffered candles
        for (const candle of this.candleBuffers.values()) {
            await this.saveCandleToDatabase(candle);
        }
        this.candleBuffers.clear();
        
        if (this.heartbeatInterval) {
            clearInterval(this.heartbeatInterval);
        }
        
        if (this.ws) {
            this.ws.close();
        }
        
        await database.disconnect();
        console.log('✅ Shutdown complete');
    }
}

// Create singleton instance
const dataIngestion = new FinnhubDataIngestion();

// Handle graceful shutdown
process.on('SIGTERM', () => dataIngestion.shutdown());
process.on('SIGINT', () => dataIngestion.shutdown());

// Export for use by API server
module.exports = dataIngestion;

// Start if run directly
if (require.main === module) {
    dataIngestion.init().catch(error => {
        console.error('❌ Failed to start:', error);
        process.exit(1);
    });
}