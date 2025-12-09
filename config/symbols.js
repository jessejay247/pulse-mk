// =============================================================================
// config/symbols.js - Symbol definitions with tier access control
// =============================================================================

const SYMBOL_TIERS = {
    FREE: 'free',           // Crypto only
    INDIVIDUAL: 'individual', // Crypto + Major Forex
    BUSINESS: 'business'     // All symbols
};

// Symbol definitions with Finnhub mappings and metadata
const SYMBOLS = {
    // =========================================================================
    // MAJOR FOREX PAIRS (Individual + Business) - 7 pairs
    // =========================================================================
    'EUR/USD': {
        finnhub: 'OANDA:EUR_USD',
        type: 'forex',
        tier: SYMBOL_TIERS.INDIVIDUAL,
        base: 'EUR',
        quote: 'USD',
        pipSize: 0.0001,
        digits: 5,
        description: 'Euro vs US Dollar'
    },
    'GBP/USD': {
        finnhub: 'OANDA:GBP_USD',
        type: 'forex',
        tier: SYMBOL_TIERS.INDIVIDUAL,
        base: 'GBP',
        quote: 'USD',
        pipSize: 0.0001,
        digits: 5,
        description: 'British Pound vs US Dollar'
    },
    'USD/JPY': {
        finnhub: 'OANDA:USD_JPY',
        type: 'forex',
        tier: SYMBOL_TIERS.INDIVIDUAL,
        base: 'USD',
        quote: 'JPY',
        pipSize: 0.01,
        digits: 3,
        description: 'US Dollar vs Japanese Yen'
    },
    'USD/CHF': {
        finnhub: 'OANDA:USD_CHF',
        type: 'forex',
        tier: SYMBOL_TIERS.INDIVIDUAL,
        base: 'USD',
        quote: 'CHF',
        pipSize: 0.0001,
        digits: 5,
        description: 'US Dollar vs Swiss Franc'
    },
    'AUD/USD': {
        finnhub: 'OANDA:AUD_USD',
        type: 'forex',
        tier: SYMBOL_TIERS.INDIVIDUAL,
        base: 'AUD',
        quote: 'USD',
        pipSize: 0.0001,
        digits: 5,
        description: 'Australian Dollar vs US Dollar'
    },
    'USD/CAD': {
        finnhub: 'OANDA:USD_CAD',
        type: 'forex',
        tier: SYMBOL_TIERS.INDIVIDUAL,
        base: 'USD',
        quote: 'CAD',
        pipSize: 0.0001,
        digits: 5,
        description: 'US Dollar vs Canadian Dollar'
    },
    'NZD/USD': {
        finnhub: 'OANDA:NZD_USD',
        type: 'forex',
        tier: SYMBOL_TIERS.INDIVIDUAL,
        base: 'NZD',
        quote: 'USD',
        pipSize: 0.0001,
        digits: 5,
        description: 'New Zealand Dollar vs US Dollar'
    },

    // =========================================================================
    // MINOR/CROSS FOREX PAIRS (Business only) - 14 pairs
    // =========================================================================
    'EUR/GBP': {
        finnhub: 'OANDA:EUR_GBP',
        type: 'forex',
        tier: SYMBOL_TIERS.BUSINESS,
        base: 'EUR',
        quote: 'GBP',
        pipSize: 0.0001,
        digits: 5,
        description: 'Euro vs British Pound'
    },
    'EUR/JPY': {
        finnhub: 'OANDA:EUR_JPY',
        type: 'forex',
        tier: SYMBOL_TIERS.BUSINESS,
        base: 'EUR',
        quote: 'JPY',
        pipSize: 0.01,
        digits: 3,
        description: 'Euro vs Japanese Yen'
    },
    'GBP/JPY': {
        finnhub: 'OANDA:GBP_JPY',
        type: 'forex',
        tier: SYMBOL_TIERS.BUSINESS,
        base: 'GBP',
        quote: 'JPY',
        pipSize: 0.01,
        digits: 3,
        description: 'British Pound vs Japanese Yen'
    },
    'EUR/CHF': {
        finnhub: 'OANDA:EUR_CHF',
        type: 'forex',
        tier: SYMBOL_TIERS.BUSINESS,
        base: 'EUR',
        quote: 'CHF',
        pipSize: 0.0001,
        digits: 5,
        description: 'Euro vs Swiss Franc'
    },
    'GBP/CHF': {
        finnhub: 'OANDA:GBP_CHF',
        type: 'forex',
        tier: SYMBOL_TIERS.BUSINESS,
        base: 'GBP',
        quote: 'CHF',
        pipSize: 0.0001,
        digits: 5,
        description: 'British Pound vs Swiss Franc'
    },
    'AUD/JPY': {
        finnhub: 'OANDA:AUD_JPY',
        type: 'forex',
        tier: SYMBOL_TIERS.BUSINESS,
        base: 'AUD',
        quote: 'JPY',
        pipSize: 0.01,
        digits: 3,
        description: 'Australian Dollar vs Japanese Yen'
    },
    'EUR/AUD': {
        finnhub: 'OANDA:EUR_AUD',
        type: 'forex',
        tier: SYMBOL_TIERS.BUSINESS,
        base: 'EUR',
        quote: 'AUD',
        pipSize: 0.0001,
        digits: 5,
        description: 'Euro vs Australian Dollar'
    },
    'EUR/CAD': {
        finnhub: 'OANDA:EUR_CAD',
        type: 'forex',
        tier: SYMBOL_TIERS.BUSINESS,
        base: 'EUR',
        quote: 'CAD',
        pipSize: 0.0001,
        digits: 5,
        description: 'Euro vs Canadian Dollar'
    },
    'GBP/AUD': {
        finnhub: 'OANDA:GBP_AUD',
        type: 'forex',
        tier: SYMBOL_TIERS.BUSINESS,
        base: 'GBP',
        quote: 'AUD',
        pipSize: 0.0001,
        digits: 5,
        description: 'British Pound vs Australian Dollar'
    },
    'GBP/CAD': {
        finnhub: 'OANDA:GBP_CAD',
        type: 'forex',
        tier: SYMBOL_TIERS.BUSINESS,
        base: 'GBP',
        quote: 'CAD',
        pipSize: 0.0001,
        digits: 5,
        description: 'British Pound vs Canadian Dollar'
    },
    'AUD/CAD': {
        finnhub: 'OANDA:AUD_CAD',
        type: 'forex',
        tier: SYMBOL_TIERS.BUSINESS,
        base: 'AUD',
        quote: 'CAD',
        pipSize: 0.0001,
        digits: 5,
        description: 'Australian Dollar vs Canadian Dollar'
    },
    'AUD/NZD': {
        finnhub: 'OANDA:AUD_NZD',
        type: 'forex',
        tier: SYMBOL_TIERS.BUSINESS,
        base: 'AUD',
        quote: 'NZD',
        pipSize: 0.0001,
        digits: 5,
        description: 'Australian Dollar vs New Zealand Dollar'
    },
    'NZD/JPY': {
        finnhub: 'OANDA:NZD_JPY',
        type: 'forex',
        tier: SYMBOL_TIERS.BUSINESS,
        base: 'NZD',
        quote: 'JPY',
        pipSize: 0.01,
        digits: 3,
        description: 'New Zealand Dollar vs Japanese Yen'
    },
    'CAD/JPY': {
        finnhub: 'OANDA:CAD_JPY',
        type: 'forex',
        tier: SYMBOL_TIERS.BUSINESS,
        base: 'CAD',
        quote: 'JPY',
        pipSize: 0.01,
        digits: 3,
        description: 'Canadian Dollar vs Japanese Yen'
    },

    // =========================================================================
    // METALS (Individual + Business) - 2 symbols
    // =========================================================================
    'XAU/USD': {
        finnhub: 'OANDA:XAU_USD',
        type: 'metal',
        tier: SYMBOL_TIERS.INDIVIDUAL,
        base: 'XAU',
        quote: 'USD',
        pipSize: 0.01,
        digits: 2,
        description: 'Gold vs US Dollar'
    },
    'XAG/USD': {
        finnhub: 'OANDA:XAG_USD',
        type: 'metal',
        tier: SYMBOL_TIERS.BUSINESS,
        base: 'XAG',
        quote: 'USD',
        pipSize: 0.001,
        digits: 3,
        description: 'Silver vs US Dollar'
    },

    // =========================================================================
    // CRYPTO (All tiers) - 10 pairs
    // =========================================================================
    'BTC/USD': {
        finnhub: 'BINANCE:BTCUSDT',
        type: 'crypto',
        tier: SYMBOL_TIERS.FREE,
        base: 'BTC',
        quote: 'USD',
        pipSize: 0.01,
        digits: 2,
        description: 'Bitcoin vs US Dollar'
    },
    'ETH/USD': {
        finnhub: 'BINANCE:ETHUSDT',
        type: 'crypto',
        tier: SYMBOL_TIERS.FREE,
        base: 'ETH',
        quote: 'USD',
        pipSize: 0.01,
        digits: 2,
        description: 'Ethereum vs US Dollar'
    },
    'XRP/USD': {
        finnhub: 'BINANCE:XRPUSDT',
        type: 'crypto',
        tier: SYMBOL_TIERS.FREE,
        base: 'XRP',
        quote: 'USD',
        pipSize: 0.0001,
        digits: 4,
        description: 'Ripple vs US Dollar'
    },
    'SOL/USD': {
        finnhub: 'BINANCE:SOLUSDT',
        type: 'crypto',
        tier: SYMBOL_TIERS.FREE,
        base: 'SOL',
        quote: 'USD',
        pipSize: 0.01,
        digits: 2,
        description: 'Solana vs US Dollar'
    },
    'ADA/USD': {
        finnhub: 'BINANCE:ADAUSDT',
        type: 'crypto',
        tier: SYMBOL_TIERS.FREE,
        base: 'ADA',
        quote: 'USD',
        pipSize: 0.0001,
        digits: 4,
        description: 'Cardano vs US Dollar'
    },
    'DOGE/USD': {
        finnhub: 'BINANCE:DOGEUSDT',
        type: 'crypto',
        tier: SYMBOL_TIERS.FREE,
        base: 'DOGE',
        quote: 'USD',
        pipSize: 0.00001,
        digits: 5,
        description: 'Dogecoin vs US Dollar'
    },
    'DOT/USD': {
        finnhub: 'BINANCE:DOTUSDT',
        type: 'crypto',
        tier: SYMBOL_TIERS.FREE,
        base: 'DOT',
        quote: 'USD',
        pipSize: 0.001,
        digits: 3,
        description: 'Polkadot vs US Dollar'
    },
    'LTC/USD': {
        finnhub: 'BINANCE:LTCUSDT',
        type: 'crypto',
        tier: SYMBOL_TIERS.FREE,
        base: 'LTC',
        quote: 'USD',
        pipSize: 0.01,
        digits: 2,
        description: 'Litecoin vs US Dollar'
    },
    'AVAX/USD': {
        finnhub: 'BINANCE:AVAXUSDT',
        type: 'crypto',
        tier: SYMBOL_TIERS.FREE,
        base: 'AVAX',
        quote: 'USD',
        pipSize: 0.01,
        digits: 2,
        description: 'Avalanche vs US Dollar'
    },
    'MATIC/USD': {
        finnhub: 'BINANCE:MATICUSDT',
        type: 'crypto',
        tier: SYMBOL_TIERS.FREE,
        base: 'MATIC',
        quote: 'USD',
        pipSize: 0.0001,
        digits: 4,
        description: 'Polygon vs US Dollar'
    },

    // =========================================================================
    // STOCKS (Business only) - 10 symbols
    // =========================================================================
    'AAPL': {
        finnhub: 'AAPL',
        type: 'stock',
        tier: SYMBOL_TIERS.BUSINESS,
        base: 'AAPL',
        quote: 'USD',
        pipSize: 0.01,
        digits: 2,
        description: 'Apple Inc.'
    },
    'TSLA': {
        finnhub: 'TSLA',
        type: 'stock',
        tier: SYMBOL_TIERS.BUSINESS,
        base: 'TSLA',
        quote: 'USD',
        pipSize: 0.01,
        digits: 2,
        description: 'Tesla Inc.'
    },
    'MSFT': {
        finnhub: 'MSFT',
        type: 'stock',
        tier: SYMBOL_TIERS.BUSINESS,
        base: 'MSFT',
        quote: 'USD',
        pipSize: 0.01,
        digits: 2,
        description: 'Microsoft Corporation'
    },
    'GOOGL': {
        finnhub: 'GOOGL',
        type: 'stock',
        tier: SYMBOL_TIERS.BUSINESS,
        base: 'GOOGL',
        quote: 'USD',
        pipSize: 0.01,
        digits: 2,
        description: 'Alphabet Inc.'
    },
    'AMZN': {
        finnhub: 'AMZN',
        type: 'stock',
        tier: SYMBOL_TIERS.BUSINESS,
        base: 'AMZN',
        quote: 'USD',
        pipSize: 0.01,
        digits: 2,
        description: 'Amazon.com Inc.'
    },
    'META': {
        finnhub: 'META',
        type: 'stock',
        tier: SYMBOL_TIERS.BUSINESS,
        base: 'META',
        quote: 'USD',
        pipSize: 0.01,
        digits: 2,
        description: 'Meta Platforms Inc.'
    },
    'NVDA': {
        finnhub: 'NVDA',
        type: 'stock',
        tier: SYMBOL_TIERS.BUSINESS,
        base: 'NVDA',
        quote: 'USD',
        pipSize: 0.01,
        digits: 2,
        description: 'NVIDIA Corporation'
    }
};

// Total: 7 major + 14 minor + 2 metals + 10 crypto + 7 stocks = 40 symbols
// Room for 10 more if needed

// Helper functions
function getSymbol(symbol) {
    // Normalize: accept both EUR/USD and EURUSD formats
    const normalized = symbol.includes('/') ? symbol : 
        symbol.replace(/([A-Z]{3})([A-Z]{3,})/, '$1/$2');
    return SYMBOLS[normalized] || null;
}

function getSymbolByFinnhub(finnhubSymbol) {
    for (const [symbol, config] of Object.entries(SYMBOLS)) {
        if (config.finnhub === finnhubSymbol) {
            return { symbol, ...config };
        }
    }
    return null;
}

function getSymbolsForTier(tier) {
    const tierPriority = {
        [SYMBOL_TIERS.FREE]: 0,
        [SYMBOL_TIERS.INDIVIDUAL]: 1,
        [SYMBOL_TIERS.BUSINESS]: 2
    };
    
    const userTierLevel = tierPriority[tier] ?? 0;
    
    return Object.entries(SYMBOLS)
        .filter(([_, config]) => tierPriority[config.tier] <= userTierLevel)
        .reduce((acc, [symbol, config]) => {
            acc[symbol] = config;
            return acc;
        }, {});
}

function getSymbolsByType(type, tier = SYMBOL_TIERS.BUSINESS) {
    const availableSymbols = getSymbolsForTier(tier);
    return Object.entries(availableSymbols)
        .filter(([_, config]) => config.type === type)
        .reduce((acc, [symbol, config]) => {
            acc[symbol] = config;
            return acc;
        }, {});
}

function getAllFinnhubSymbols() {
    return Object.values(SYMBOLS).map(s => s.finnhub);
}

function canAccessSymbol(symbol, userTier) {
    const config = getSymbol(symbol);
    if (!config) return false;
    
    const tierPriority = {
        [SYMBOL_TIERS.FREE]: 0,
        [SYMBOL_TIERS.INDIVIDUAL]: 1,
        [SYMBOL_TIERS.BUSINESS]: 2
    };
    
    return tierPriority[userTier] >= tierPriority[config.tier];
}

// Convert internal symbol to display format
function toDisplaySymbol(internalSymbol) {
    // EURUSD -> EUR/USD, BTCUSD -> BTC/USD
    if (internalSymbol.includes('/')) return internalSymbol;
    if (SYMBOLS[internalSymbol]) return internalSymbol; // Already correct (stocks)
    
    // Try to find in our symbols
    for (const symbol of Object.keys(SYMBOLS)) {
        if (symbol.replace('/', '') === internalSymbol) {
            return symbol;
        }
    }
    return internalSymbol;
}

// Convert display symbol to internal format (for DB)
function toInternalSymbol(displaySymbol) {
    return displaySymbol.replace('/', '');
}

module.exports = {
    SYMBOLS,
    SYMBOL_TIERS,
    getSymbol,
    getSymbolByFinnhub,
    getSymbolsForTier,
    getSymbolsByType,
    getAllFinnhubSymbols,
    canAccessSymbol,
    toDisplaySymbol,
    toInternalSymbol
};