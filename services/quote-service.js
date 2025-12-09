// =============================================================================
// services/quote-service.js - Quote and Price Service
// =============================================================================

const database = require('../database');
const { getSymbol, getSymbolsForTier, toDisplaySymbol, toInternalSymbol } = require('../config/symbols');

class QuoteService {
    /**
     * Get quote for a single symbol
     */
    async getQuote(symbol, includeChange = true) {
        const displaySymbol = toDisplaySymbol(symbol);
        const internalSymbol = toInternalSymbol(displaySymbol);

        const quote = await database.getLatestQuote(internalSymbol);
        if (!quote) {
            return null;
        }

        let changeData = { change: 0, changePercent: 0, high: 0, low: 0 };
        if (includeChange) {
            changeData = await database.getDailyChange(internalSymbol);
        }

        return {
            symbol: displaySymbol,
            bid: quote.bid,
            ask: quote.ask,
            mid: quote.mid,
            spread: quote.spread,
            change: changeData.change,
            change_percent: changeData.changePercent,
            high: changeData.high || quote.high,
            low: changeData.low || quote.low,
            timestamp: quote.timestamp
        };
    }

    /**
     * Get quotes for multiple symbols
     */
    async getQuotes(symbols) {
        const results = [];
        
        for (const symbol of symbols) {
            const displaySymbol = toDisplaySymbol(symbol);
            const internalSymbol = toInternalSymbol(displaySymbol);
            
            const quote = await database.getLatestQuote(internalSymbol);
            if (quote) {
                results.push({
                    symbol: displaySymbol,
                    bid: quote.bid,
                    ask: quote.ask,
                    spread: quote.spread,
                    timestamp: quote.timestamp
                });
            }
        }

        return results;
    }

    /**
     * Get all available quotes for a user's tier
     */
    async getAllQuotes(tier) {
        const availableSymbols = Object.keys(getSymbolsForTier(tier));
        return this.getQuotes(availableSymbols);
    }

    /**
     * Convert currency
     */
    async convert(from, to, amount) {
        // Try direct pair first
        let rate = await this.getConversionRate(from, to);
        
        if (!rate) {
            // Try inverse pair
            const inverseRate = await this.getConversionRate(to, from);
            if (inverseRate) {
                rate = 1 / inverseRate;
            }
        }

        if (!rate) {
            // Try via USD
            const fromUsdRate = await this.getUsdRate(from);
            const toUsdRate = await this.getUsdRate(to);
            
            if (fromUsdRate && toUsdRate) {
                rate = fromUsdRate / toUsdRate;
            }
        }

        if (!rate) {
            return null;
        }

        return {
            from,
            to,
            amount,
            result: amount * rate,
            rate,
            timestamp: new Date().toISOString()
        };
    }

    /**
     * Get conversion rate for a pair
     */
    async getConversionRate(from, to) {
        const symbol = `${from}/${to}`;
        const internalSymbol = toInternalSymbol(symbol);
        const quote = await database.getLatestQuote(internalSymbol);
        
        if (quote) {
            return quote.close;
        }

        // Check alternate format (for crypto)
        const altSymbol = `${from}${to}`;
        const altQuote = await database.getLatestQuote(altSymbol);
        
        return altQuote?.close || null;
    }

    /**
     * Get rate relative to USD
     */
    async getUsdRate(currency) {
        if (currency === 'USD') return 1;

        // Try XXX/USD
        let rate = await this.getConversionRate(currency, 'USD');
        if (rate) return rate;

        // Try USD/XXX (inverse)
        rate = await this.getConversionRate('USD', currency);
        if (rate) return 1 / rate;

        return null;
    }

    /**
     * Get latest price only (simplified)
     */
    async getPrice(symbol) {
        const displaySymbol = toDisplaySymbol(symbol);
        const internalSymbol = toInternalSymbol(displaySymbol);
        
        const quote = await database.getLatestQuote(internalSymbol);
        if (!quote) return null;

        return {
            symbol: displaySymbol,
            price: quote.close,
            timestamp: quote.timestamp
        };
    }
}

module.exports = new QuoteService();