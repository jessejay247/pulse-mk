// =============================================================================
// middleware/rate-limit.js - In-Memory Rate Limiting (Redis-ready)
// =============================================================================

/**
 * In-memory rate limit store
 * NOTE: For production with multiple instances, replace with Redis
 * 
 * Structure:
 * {
 *   'minute:userId:2024-01-15-14-30': count,
 *   'daily:userId:2024-01-15': count
 * }
 */
const rateLimitStore = new Map();

// Cleanup old entries every 5 minutes
setInterval(() => {
    const now = Date.now();
    for (const [key, data] of rateLimitStore.entries()) {
        if (data.expiresAt < now) {
            rateLimitStore.delete(key);
        }
    }
}, 5 * 60 * 1000);

/**
 * Rate Limit Middleware
 * Enforces per-minute and daily limits based on user's plan
 */
async function rateLimit(req, res, next) {
    if (!req.auth) {
        return next();
    }

    const userId = req.auth.userId;
    const plan = req.auth.plan;

    const now = new Date();
    const minuteKey = `minute:${userId}:${formatMinute(now)}`;
    const dailyKey = `daily:${userId}:${formatDate(now)}`;

    // Get current counts
    const minuteData = rateLimitStore.get(minuteKey) || { count: 0, expiresAt: getMinuteExpiry() };
    const dailyData = rateLimitStore.get(dailyKey) || { count: 0, expiresAt: getDailyExpiry() };

    // Check daily limit first
    if (dailyData.count >= plan.apiCallsPerDay) {
        return res.status(429).json({
            success: false,
            error: {
                code: 'DAILY_LIMIT_EXCEEDED',
                message: 'Daily API call limit exceeded',
                status: 429,
                retry_after: Math.ceil((dailyData.expiresAt - Date.now()) / 1000)
            }
        }).set({
            'X-RateLimit-Limit': plan.apiCallsPerMinute,
            'X-RateLimit-Remaining': 0,
            'X-Daily-Limit': plan.apiCallsPerDay,
            'X-Daily-Remaining': 0,
            'Retry-After': Math.ceil((dailyData.expiresAt - Date.now()) / 1000)
        });
    }

    // Check per-minute limit
    if (minuteData.count >= plan.apiCallsPerMinute) {
        const retryAfter = Math.ceil((minuteData.expiresAt - Date.now()) / 1000);
        return res.status(429).json({
            success: false,
            error: {
                code: 'RATE_LIMIT_EXCEEDED',
                message: 'Rate limit exceeded. Please wait before making more requests.',
                status: 429,
                retry_after: retryAfter
            }
        }).set({
            'X-RateLimit-Limit': plan.apiCallsPerMinute,
            'X-RateLimit-Remaining': 0,
            'X-RateLimit-Reset': Math.floor(minuteData.expiresAt / 1000),
            'Retry-After': retryAfter
        });
    }

    // Increment counters
    minuteData.count++;
    dailyData.count++;
    rateLimitStore.set(minuteKey, minuteData);
    rateLimitStore.set(dailyKey, dailyData);

    // Add rate limit headers to response
    res.set({
        'X-RateLimit-Limit': plan.apiCallsPerMinute,
        'X-RateLimit-Remaining': Math.max(0, plan.apiCallsPerMinute - minuteData.count),
        'X-RateLimit-Reset': Math.floor(minuteData.expiresAt / 1000),
        'X-Daily-Limit': plan.apiCallsPerDay,
        'X-Daily-Remaining': Math.max(0, plan.apiCallsPerDay - dailyData.count)
    });

    next();
}

/**
 * WebSocket connection rate limiter
 * Tracks concurrent connections per user
 */
const wsConnectionStore = new Map();

function checkWebSocketLimit(userId, plan) {
    const current = wsConnectionStore.get(userId) || 0;
    const limit = plan.websocketConnections || 0;

    if (current >= limit) {
        return {
            allowed: false,
            current,
            limit,
            message: `WebSocket connection limit reached (${current}/${limit})`
        };
    }

    return {
        allowed: true,
        current,
        limit
    };
}

function incrementWsConnection(userId) {
    const current = wsConnectionStore.get(userId) || 0;
    wsConnectionStore.set(userId, current + 1);
}

function decrementWsConnection(userId) {
    const current = wsConnectionStore.get(userId) || 0;
    wsConnectionStore.set(userId, Math.max(0, current - 1));
}

function getWsConnectionCount(userId) {
    return wsConnectionStore.get(userId) || 0;
}

/**
 * Get rate limit status for a user
 */
function getRateLimitStatus(userId, plan) {
    const now = new Date();
    const minuteKey = `minute:${userId}:${formatMinute(now)}`;
    const dailyKey = `daily:${userId}:${formatDate(now)}`;

    const minuteData = rateLimitStore.get(minuteKey) || { count: 0 };
    const dailyData = rateLimitStore.get(dailyKey) || { count: 0 };

    return {
        minuteLimit: plan.apiCallsPerMinute,
        minuteUsed: minuteData.count,
        minuteRemaining: Math.max(0, plan.apiCallsPerMinute - minuteData.count),
        dailyLimit: plan.apiCallsPerDay,
        dailyUsed: dailyData.count,
        dailyRemaining: Math.max(0, plan.apiCallsPerDay - dailyData.count)
    };
}

// Helper functions
function formatMinute(date) {
    return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())}-${pad(date.getUTCHours())}-${pad(date.getUTCMinutes())}`;
}

function formatDate(date) {
    return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())}`;
}

function pad(num) {
    return num.toString().padStart(2, '0');
}

function getMinuteExpiry() {
    const now = new Date();
    return now.getTime() + (60 - now.getUTCSeconds()) * 1000;
}

function getDailyExpiry() {
    const tomorrow = new Date();
    tomorrow.setUTCDate(tomorrow.getUTCDate() + 1);
    tomorrow.setUTCHours(0, 0, 0, 0);
    return tomorrow.getTime();
}

module.exports = {
    rateLimit,
    checkWebSocketLimit,
    incrementWsConnection,
    decrementWsConnection,
    getWsConnectionCount,
    getRateLimitStatus
};