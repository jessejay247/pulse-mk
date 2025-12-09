// =============================================================================
// config/market-hours.js - Trading sessions and holidays
// =============================================================================

// Market sessions (all times in UTC)
const MARKET_SESSIONS = {
    sydney: {
        name: 'Sydney',
        open: { hour: 21, minute: 0 },   // 21:00 UTC (previous day)
        close: { hour: 6, minute: 0 },    // 06:00 UTC
        timezone: 'Australia/Sydney',
        daysOpen: [0, 1, 2, 3, 4]  // Sun-Thu (UTC perspective)
    },
    tokyo: {
        name: 'Tokyo',
        open: { hour: 0, minute: 0 },     // 00:00 UTC
        close: { hour: 9, minute: 0 },    // 09:00 UTC
        timezone: 'Asia/Tokyo',
        daysOpen: [1, 2, 3, 4, 5]  // Mon-Fri
    },
    london: {
        name: 'London',
        open: { hour: 8, minute: 0 },     // 08:00 UTC
        close: { hour: 16, minute: 0 },   // 16:00 UTC
        timezone: 'Europe/London',
        daysOpen: [1, 2, 3, 4, 5]  // Mon-Fri
    },
    newYork: {
        name: 'New York',
        open: { hour: 13, minute: 0 },    // 13:00 UTC
        close: { hour: 22, minute: 0 },   // 22:00 UTC
        timezone: 'America/New_York',
        daysOpen: [1, 2, 3, 4, 5]  // Mon-Fri
    }
};

// Forex market hours (Sunday 21:00 UTC to Friday 22:00 UTC)
const FOREX_MARKET = {
    weekStart: { day: 0, hour: 21, minute: 0 },  // Sunday 21:00 UTC
    weekEnd: { day: 5, hour: 22, minute: 0 }     // Friday 22:00 UTC
};

// Stock market hours (US Eastern Time, converted to UTC)
const STOCK_MARKET = {
    preMarket: { hour: 9, minute: 0 },    // 04:00 ET = 09:00 UTC (summer)
    marketOpen: { hour: 14, minute: 30 }, // 09:30 ET = 14:30 UTC (summer)
    marketClose: { hour: 21, minute: 0 }, // 16:00 ET = 21:00 UTC (summer)
    afterHours: { hour: 1, minute: 0 },   // 20:00 ET = 01:00 UTC next day
    daysOpen: [1, 2, 3, 4, 5]  // Mon-Fri
};

// Major holidays when forex market is closed or has reduced liquidity
// Format: MM-DD or YYYY-MM-DD for specific years
const HOLIDAYS = {
    // Fixed holidays (every year)
    fixed: [
        { month: 1, day: 1, name: "New Year's Day", type: 'closed' },
        { month: 12, day: 25, name: "Christmas Day", type: 'closed' },
    ],
    // Variable holidays (specific dates for years)
    // Add more as needed
    2025: [
        { month: 1, day: 1, name: "New Year's Day", type: 'closed' },
        { month: 1, day: 20, name: "MLK Day", type: 'us_closed' },
        { month: 2, day: 17, name: "Presidents Day", type: 'us_closed' },
        { month: 4, day: 18, name: "Good Friday", type: 'reduced' },
        { month: 5, day: 26, name: "Memorial Day", type: 'us_closed' },
        { month: 7, day: 4, name: "Independence Day", type: 'us_closed' },
        { month: 9, day: 1, name: "Labor Day", type: 'us_closed' },
        { month: 11, day: 27, name: "Thanksgiving", type: 'us_closed' },
        { month: 12, day: 25, name: "Christmas", type: 'closed' },
        { month: 12, day: 26, name: "Boxing Day", type: 'reduced' },
    ],
    2026: [
        { month: 1, day: 1, name: "New Year's Day", type: 'closed' },
        { month: 1, day: 19, name: "MLK Day", type: 'us_closed' },
        { month: 2, day: 16, name: "Presidents Day", type: 'us_closed' },
        { month: 4, day: 3, name: "Good Friday", type: 'reduced' },
        { month: 5, day: 25, name: "Memorial Day", type: 'us_closed' },
        { month: 7, day: 3, name: "Independence Day (Observed)", type: 'us_closed' },
        { month: 9, day: 7, name: "Labor Day", type: 'us_closed' },
        { month: 11, day: 26, name: "Thanksgiving", type: 'us_closed' },
        { month: 12, day: 25, name: "Christmas", type: 'closed' },
    ]
};

/**
 * Check if a given time is within forex trading hours
 * Forex trades from Sunday 21:00 UTC to Friday 22:00 UTC
 */
function isForexMarketOpen(date = new Date()) {
    const utc = new Date(date.toISOString());
    const day = utc.getUTCDay();
    const hour = utc.getUTCHours();
    const minute = utc.getUTCMinutes();
    const timeValue = hour * 60 + minute;

    // Check holidays first
    const holiday = getHoliday(utc);
    if (holiday && holiday.type === 'closed') {
        return { open: false, reason: holiday.name };
    }

    // Saturday: Always closed
    if (day === 6) {
        return { open: false, reason: 'Weekend - Market closed' };
    }

    // Sunday: Open after 21:00 UTC
    if (day === 0) {
        if (timeValue >= 21 * 60) {
            return { open: true, session: 'sydney' };
        }
        return { open: false, reason: 'Weekend - Opens Sunday 21:00 UTC' };
    }

    // Friday: Close at 22:00 UTC
    if (day === 5) {
        if (timeValue < 22 * 60) {
            return { open: true, session: getCurrentSession(utc) };
        }
        return { open: false, reason: 'Weekend - Market closed' };
    }

    // Monday - Thursday: Always open
    return { open: true, session: getCurrentSession(utc) };
}

/**
 * Check if stock market (US) is open
 */
function isStockMarketOpen(date = new Date()) {
    const utc = new Date(date.toISOString());
    const day = utc.getUTCDay();
    const hour = utc.getUTCHours();
    const minute = utc.getUTCMinutes();
    const timeValue = hour * 60 + minute;

    // Weekend check
    if (day === 0 || day === 6) {
        return { open: false, reason: 'Weekend - US market closed' };
    }

    // Holiday check
    const holiday = getHoliday(utc);
    if (holiday && (holiday.type === 'closed' || holiday.type === 'us_closed')) {
        return { open: false, reason: holiday.name };
    }

    // Regular hours: 14:30 - 21:00 UTC (summer time)
    // TODO: Adjust for daylight saving time
    const marketOpen = 14 * 60 + 30;  // 14:30 UTC
    const marketClose = 21 * 60;       // 21:00 UTC

    if (timeValue >= marketOpen && timeValue < marketClose) {
        return { open: true, session: 'regular' };
    }

    // Pre-market: 09:00 - 14:30 UTC
    if (timeValue >= 9 * 60 && timeValue < marketOpen) {
        return { open: true, session: 'pre-market' };
    }

    // After-hours: 21:00 - 01:00 UTC (next day)
    if (timeValue >= marketClose || timeValue < 1 * 60) {
        return { open: true, session: 'after-hours' };
    }

    return { open: false, reason: 'Outside trading hours' };
}

/**
 * Crypto markets are 24/7
 */
function isCryptoMarketOpen() {
    return { open: true, session: '24/7' };
}

/**
 * Check if a symbol's market is open
 */
function isMarketOpenForSymbol(symbolType, date = new Date()) {
    switch (symbolType) {
        case 'forex':
        case 'metal':
            return isForexMarketOpen(date);
        case 'stock':
            return isStockMarketOpen(date);
        case 'crypto':
            return isCryptoMarketOpen();
        default:
            return { open: true, session: 'unknown' };
    }
}

/**
 * Get current trading session
 */
function getCurrentSession(date = new Date()) {
    const utc = new Date(date.toISOString());
    const hour = utc.getUTCHours();

    // Determine overlapping sessions
    const sessions = [];

    // Sydney: 21:00 - 06:00 UTC
    if (hour >= 21 || hour < 6) sessions.push('sydney');
    
    // Tokyo: 00:00 - 09:00 UTC
    if (hour >= 0 && hour < 9) sessions.push('tokyo');
    
    // London: 08:00 - 16:00 UTC
    if (hour >= 8 && hour < 16) sessions.push('london');
    
    // New York: 13:00 - 22:00 UTC
    if (hour >= 13 && hour < 22) sessions.push('newYork');

    return sessions.length > 0 ? sessions.join('/') : 'off-hours';
}

/**
 * Get holiday for a specific date
 */
function getHoliday(date) {
    const year = date.getUTCFullYear();
    const month = date.getUTCMonth() + 1;
    const day = date.getUTCDate();

    // Check year-specific holidays
    const yearHolidays = HOLIDAYS[year] || [];
    const holiday = yearHolidays.find(h => h.month === month && h.day === day);
    if (holiday) return holiday;

    // Check fixed holidays
    return HOLIDAYS.fixed.find(h => h.month === month && h.day === day);
}

/**
 * Get market status summary
 */
function getMarketStatus(date = new Date()) {
    const utc = new Date(date.toISOString());
    const forex = isForexMarketOpen(utc);
    const stock = isStockMarketOpen(utc);
    const crypto = isCryptoMarketOpen();

    // Get session info
    const sessionInfo = {};
    for (const [key, session] of Object.entries(MARKET_SESSIONS)) {
        const hour = utc.getUTCHours();
        const isOpen = isSessionOpen(session, hour, utc.getUTCDay());
        
        sessionInfo[key] = {
            open: isOpen,
            next_open: isOpen ? null : getNextSessionOpen(session, utc),
            closes_at: isOpen ? getSessionClose(session, utc) : null
        };
    }

    return {
        forex_market_open: forex.open,
        stock_market_open: stock.open,
        crypto_market_open: crypto.open,
        current_session: forex.session || null,
        sessions: sessionInfo,
        server_time: utc.toISOString()
    };
}

function isSessionOpen(session, hour, day) {
    if (!session.daysOpen.includes(day)) return false;
    
    const { open, close } = session;
    
    // Handle overnight sessions (like Sydney)
    if (open.hour > close.hour) {
        return hour >= open.hour || hour < close.hour;
    }
    
    return hour >= open.hour && hour < close.hour;
}

function getNextSessionOpen(session, now) {
    const next = new Date(now);
    next.setUTCHours(session.open.hour, session.open.minute, 0, 0);
    
    if (next <= now) {
        next.setUTCDate(next.getUTCDate() + 1);
    }
    
    // Find next valid day
    while (!session.daysOpen.includes(next.getUTCDay())) {
        next.setUTCDate(next.getUTCDate() + 1);
    }
    
    return next.toISOString();
}

function getSessionClose(session, now) {
    const close = new Date(now);
    close.setUTCHours(session.close.hour, session.close.minute, 0, 0);
    
    // If close is before now (overnight session), it's tomorrow
    if (close <= now) {
        close.setUTCDate(close.getUTCDate() + 1);
    }
    
    return close.toISOString();
}

/**
 * Get next market open time for forex
 */
function getNextForexOpen(date = new Date()) {
    const utc = new Date(date.toISOString());
    const day = utc.getUTCDay();
    
    // If it's Saturday, next open is Sunday 21:00
    // If it's Sunday before 21:00, next open is Sunday 21:00
    // If it's Friday after 22:00, next open is Sunday 21:00
    
    const nextOpen = new Date(utc);
    
    if (day === 6) {
        // Saturday -> Sunday 21:00
        nextOpen.setUTCDate(nextOpen.getUTCDate() + 1);
        nextOpen.setUTCHours(21, 0, 0, 0);
    } else if (day === 0 && utc.getUTCHours() < 21) {
        // Sunday before 21:00
        nextOpen.setUTCHours(21, 0, 0, 0);
    } else if (day === 5 && utc.getUTCHours() >= 22) {
        // Friday after 22:00 -> Sunday 21:00
        nextOpen.setUTCDate(nextOpen.getUTCDate() + 2);
        nextOpen.setUTCHours(21, 0, 0, 0);
    } else {
        // Market is open
        return null;
    }
    
    return nextOpen.toISOString();
}

module.exports = {
    MARKET_SESSIONS,
    FOREX_MARKET,
    STOCK_MARKET,
    HOLIDAYS,
    isForexMarketOpen,
    isStockMarketOpen,
    isCryptoMarketOpen,
    isMarketOpenForSymbol,
    getCurrentSession,
    getMarketStatus,
    getNextForexOpen,
    getHoliday
};