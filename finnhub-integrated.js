// =============================================================================
// finnhub-integrated.js - Finnhub WebSocket with Self-Healing Integration
// =============================================================================
//
// This replaces/enhances finnhub-app.js to integrate with the self-healing system
// - Sends ticks to TickStore (not directly to candles)
// - Uses SpikeFilter before accepting ticks
// - Includes REST API fallback for missed ticks
// =============================================================================

require('dotenv').config();

const WebSocket = require('ws');
const axios = require('axios');
const { TickStore } = require('./services/tick-store');
const { SpikeFilter } = require('./services/spike-filter');
const { SYMBOLS, getSymbolByFinnhub, toInternalSymbol } = require('./config/symbols');
const { isMarketOpenForSymbol } = require('./config/market-hours');

class FinnhubIntegrated {
    constructor() {
        this.apiKey = process.env.FINNHUB_API_KEY;
        this.wsUrl = `wss://ws.finnhub.io?token=${this.apiKey}`;
        this.restUrl = 'https://finnhub.io/api/v1';
        
        this.ws = null;
        this.isConnected = false;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 10;
        
        // Integration with self-healing system
        this.tickStore = new TickStore();
        this.spikeFilter = new SpikeFilter();
        
        // Tick callbacks for WebSocket broadcasting
        this.tickCallbacks = [];
        
        // REST API rate limiting
        this.lastRestCall = 0;
        this.restCallInterval = 1000; // 1 second between REST calls
        
        // Health tracking
        this.stats = {
            ticksReceived: 0,
            ticksAccepted: 0,
            ticksRejected: 0,
            restFallbacks: 0,
            reconnects: 0,
        };
        
        // Track last tick time per symbol for REST fallback
        this.lastTickTime = new Map();
    }

    async init() {
        console.log('🚀 Initializing Finnhub Integrated Service');
        
        // Load last prices for spike detection
        await this.spikeFilter.loadLastPrices();
        
        // Connect WebSocket
        await this.connectWebSocket();
        
        // Start REST fallback checker
        this.startRestFallback();
        
        // Start heartbeat
        this.startHeartbeat();
        
        console.log(`✅ Finnhub service ready, tracking ${Object.keys(SYMBOLS).length} symbols`);
    }

    // =========================================================================
    // WEBSOCKET CONNECTION
    // =========================================================================

    async connectWebSocket() {
        return new Promise((resolve, reject) => {
            if (this.ws) {
                this.ws.terminate();
            }

            this.ws = new WebSocket(this.wsUrl);

            this.ws.on('open', () => {
                console.log('✅ Finnhub WebSocket connected');
                this.isConnected = true;
                this.reconnectAttempts = 0;
                
                // Subscribe to all symbols
                this.subscribeAll();
                resolve();
            });

            this.ws.on('message', (data) => {
                this.handleMessage(data);
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

            // Timeout for initial connection
            setTimeout(() => {
                if (!this.isConnected) {
                    reject(new Error('WebSocket connection timeout'));
                }
            }, 10000);
        });
    }

    subscribeAll() {
        for (const [symbol, config] of Object.entries(SYMBOLS)) {
            this.ws.send(JSON.stringify({
                type: 'subscribe',
                symbol: config.finnhub
            }));
        }
        console.log(`📊 Subscribed to ${Object.keys(SYMBOLS).length} symbols`);
    }

    scheduleReconnect() {
        if (this.reconnectAttempts >= this.maxReconnectAttempts) {
            console.error('❌ Max reconnection attempts reached');
            return;
        }

        const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);
        this.reconnectAttempts++;
        this.stats.reconnects++;
        
        console.log(`🔄 Reconnecting in ${delay/1000}s (attempt ${this.reconnectAttempts})`);
        setTimeout(() => this.connectWebSocket(), delay);
    }

    // =========================================================================
    // MESSAGE HANDLING
    // =========================================================================

    handleMessage(data) {
        try {
            const message = JSON.parse(data);
            
            if (message.type === 'trade' && message.data) {
                // Process only first few trades per message to avoid flooding
                const trades = message.data.slice(0, 5);
                for (const trade of trades) {
                    this.processTrade(trade);
                }
            } else if (message.type === 'ping') {
                this.ws.send(JSON.stringify({ type: 'pong' }));
            }
        } catch (error) {
            console.error('❌ Message parse error:', error.message);
        }
    }

    async processTrade(trade) {
        const { p: price, s: finnhubSymbol, t: timestamp, v: volume } = trade;
        
        // Get symbol info
        const symbolInfo = getSymbolByFinnhub(finnhubSymbol);
        if (!symbolInfo) return;

        const { symbol, type } = symbolInfo;
        const internalSymbol = toInternalSymbol(symbol);
        const tradeTime = new Date(timestamp);

        this.stats.ticksReceived++;

        // Check market hours
        const marketStatus = isMarketOpenForSymbol(type, tradeTime);

        // Send tick to TickStore (which handles spike detection)
        const result = await this.tickStore.addTick(
            internalSymbol,
            price,
            volume || 0,
            tradeTime,
            'finnhub'
        );

        if (result.accepted) {
            this.stats.ticksAccepted++;
            this.lastTickTime.set(internalSymbol, Date.now());
            
            // Emit tick for WebSocket broadcasting
            this.emitTick({
                symbol: internalSymbol,
                displaySymbol: symbol,
                price,
                volume: volume || 0,
                timestamp: tradeTime,
                marketClosed: !marketStatus.open,
            });
        } else {
            this.stats.ticksRejected++;
        }
    }

    // =========================================================================
    // REST API FALLBACK
    // =========================================================================

    startRestFallback() {
        // Check every 30 seconds for symbols without recent ticks
        setInterval(() => this.checkAndFillMissingTicks(), 30000);
    }

    async checkAndFillMissingTicks() {
        const now = Date.now();
        const staleThreshold = 60000; // 1 minute without ticks = stale
        
        for (const [symbol, config] of Object.entries(SYMBOLS)) {
            const internalSymbol = toInternalSymbol(symbol);
            const lastTick = this.lastTickTime.get(internalSymbol) || 0;
            
            // Check if market is open and we haven't received ticks
            const marketStatus = isMarketOpenForSymbol(config.type);
            if (!marketStatus.open) continue;
            
            if (now - lastTick > staleThreshold) {
                await this.fetchRestQuote(internalSymbol, config);
            }
        }
    }

    async fetchRestQuote(internalSymbol, config) {
        // Rate limit REST calls
        const now = Date.now();
        if (now - this.lastRestCall < this.restCallInterval) {
            return;
        }
        this.lastRestCall = now;

        try {
            let url;
            if (config.type === 'forex' || config.type === 'metal') {
                url = `${this.restUrl}/forex/candle?symbol=${config.finnhub}&resolution=1&count=1&token=${this.apiKey}`;
            } else if (config.type === 'crypto') {
                url = `${this.restUrl}/crypto/candle?symbol=${config.finnhub}&resolution=1&count=1&token=${this.apiKey}`;
            } else {
                url = `${this.restUrl}/quote?symbol=${config.finnhub}&token=${this.apiKey}`;
            }

            const response = await axios.get(url, { timeout: 5000 });
            
            let price;
            if (response.data.c && Array.isArray(response.data.c)) {
                price = response.data.c[response.data.c.length - 1];
            } else if (response.data.c) {
                price = response.data.c;
            }

            if (price && !isNaN(price)) {
                await this.tickStore.addTick(
                    internalSymbol,
                    price,
                    0,
                    new Date(),
                    'rest_fallback'
                );
                this.stats.restFallbacks++;
                this.lastTickTime.set(internalSymbol, Date.now());
            }

        } catch (error) {
            // Silent fail for REST fallback
            if (error.response?.status === 429) {
                console.warn('⚠️ REST API rate limited, backing off');
                this.lastRestCall = Date.now() + 60000; // Back off 1 minute
            }
        }
    }

    // =========================================================================
    // HEARTBEAT & HEALTH
    // =========================================================================

    startHeartbeat() {
        setInterval(() => {
            if (this.ws?.readyState === WebSocket.OPEN) {
                this.ws.send(JSON.stringify({ type: 'ping' }));
            }
        }, 30000);
    }

    getStatus() {
        return {
            connected: this.isConnected,
            symbolCount: Object.keys(SYMBOLS).length,
            stats: this.stats,
            tickStoreStats: this.tickStore.getStats(),
            reconnectAttempts: this.reconnectAttempts,
        };
    }

    // =========================================================================
    // TICK EMISSION (for WebSocket broadcasting)
    // =========================================================================

    emitTick(tick) {
        for (const callback of this.tickCallbacks) {
            try {
                callback(tick);
            } catch (error) {
                console.error('❌ Tick callback error:', error.message);
            }
        }
    }

    onTick(callback) {
        if (this.tickCallbacks.length < 10) {
            this.tickCallbacks.push(callback);
        }
    }

    // =========================================================================
    // SHUTDOWN
    // =========================================================================

    async shutdown() {
        console.log('🛑 Shutting down Finnhub service...');
        
        // Flush all buffered ticks
        await this.tickStore.flushAll();
        
        if (this.ws) {
            this.ws.close();
        }
        
        console.log('✅ Finnhub service shutdown complete');
    }
}

// Singleton instance
const finnhubService = new FinnhubIntegrated();

module.exports = finnhubService;

// Allow running standalone
if (require.main === module) {
    finnhubService.init().catch(error => {
        console.error('❌ Failed to start:', error);
        process.exit(1);
    });
}