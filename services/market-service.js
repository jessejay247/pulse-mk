// =============================================================================
// services/market-service.js - Market Status Service
// =============================================================================

const { getMarketStatus, isMarketOpenForSymbol, getNextForexOpen } = require('../config/market-hours');
const { SYMBOLS, getSymbol } = require('../config/symbols');

class MarketService {
    /**
     * Get full market status
     */
    getStatus() {
        return getMarketStatus();
    }

    /**
     * Check if trading is allowed for a symbol
     */
    canTrade(symbol) {
        const config = getSymbol(symbol);
        if (!config) {
            return { allowed: false, reason: 'Unknown symbol' };
        }

        const status = isMarketOpenForSymbol(config.type);
        return {
            allowed: status.open,
            reason: status.reason || null,
            session: status.session || null
        };
    }

    /**
     * Check if we should process candles for a symbol type
     * Used by the data ingestion service
     */
    shouldProcessCandle(symbolType) {
        const status = isMarketOpenForSymbol(symbolType);
        return status.open;
    }

    /**
     * Get next market open time for forex
     */
    getNextOpen() {
        const nextOpen = getNextForexOpen();
        if (!nextOpen) {
            return { isOpen: true, nextOpen: null };
        }
        return { isOpen: false, nextOpen };
    }

    /**
     * Get trading hours summary
     */
    getTradingHours() {
        return {
            forex: {
                description: 'Sunday 21:00 UTC to Friday 22:00 UTC',
                sessions: {
                    sydney: '21:00 - 06:00 UTC',
                    tokyo: '00:00 - 09:00 UTC',
                    london: '08:00 - 16:00 UTC',
                    newYork: '13:00 - 22:00 UTC'
                }
            },
            crypto: {
                description: '24/7 trading',
                sessions: null
            },
            stocks: {
                description: 'US market hours (09:30 - 16:00 ET)',
                sessions: {
                    preMarket: '04:00 - 09:30 ET',
                    regular: '09:30 - 16:00 ET',
                    afterHours: '16:00 - 20:00 ET'
                }
            }
        };
    }
}

module.exports = new MarketService();