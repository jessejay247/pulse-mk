// =============================================================================
// middleware/usage-logger.js - API Usage Logging
// =============================================================================

const database = require('../database');
const { getClientIp } = require('./auth');

/**
 * Log API usage after request completes
 */
function logUsage(req, res, next) {
    const startTime = Date.now();

    // Override res.json to capture response
    const originalJson = res.json.bind(res);
    res.json = (body) => {
        const responseTimeMs = Date.now() - startTime;
        
        // Log asynchronously (don't block response)
        if (req.auth) {
            setImmediate(() => {
                logRequest(req, res, responseTimeMs);
            });
        }

        return originalJson(body);
    };

    next();
}

/**
 * Log the request details
 */
async function logRequest(req, res, responseTimeMs) {
    try {
        await database.logApiCall(
            req.auth.userId,
            req.auth.apiKeyId,
            'rest',
            {
                endpoint: req.path,
                method: req.method,
                responseCode: res.statusCode,
                responseTimeMs: responseTimeMs,
                ipAddress: getClientIp(req)
            }
        );
    } catch (error) {
        console.error('❌ Error logging API usage:', error);
    }
}

/**
 * Log WebSocket events
 */
async function logWebSocketEvent(userId, apiKeyId, eventType, details = {}) {
    try {
        await database.logApiCall(userId, apiKeyId, eventType, details);
    } catch (error) {
        console.error('❌ Error logging WebSocket event:', error);
    }
}

/**
 * Log WebSocket connection
 */
async function logWsConnect(auth, ipAddress) {
    await logWebSocketEvent(
        auth.userId,
        auth.apiKeyId,
        'websocket_connect',
        { ipAddress }
    );
}

/**
 * Log WebSocket disconnection
 */
async function logWsDisconnect(auth, ipAddress, duration) {
    await logWebSocketEvent(
        auth.userId,
        auth.apiKeyId,
        'websocket_disconnect',
        { 
            ipAddress,
            responseTimeMs: duration  // Connection duration
        }
    );
}

/**
 * Log WebSocket message
 */
async function logWsMessage(auth, messageType) {
    await database.incrementUsageStats(auth.userId, 'websocket_message');
}

module.exports = {
    logUsage,
    logWebSocketEvent,
    logWsConnect,
    logWsDisconnect,
    logWsMessage
};