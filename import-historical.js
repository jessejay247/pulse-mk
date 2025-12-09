const fs = require('fs');
const readline = require('readline');
const mysql = require('mysql2/promise');
const path = require('path');

class HistoricalDataImporter {
    constructor() {
        this.dbConfig = {
            host: process.env.DB_HOST || 'localhost',
            user: process.env.DB_USER || 'root',
            password: process.env.DB_PASSWORD || '',
            database: process.env.DB_NAME || 'pulse_markets'
        };
        this.connection = null;
        this.batchSize = 500; // Insert in batches for better performance
    }

    async connect() {
        try {
            this.connection = await mysql.createConnection(this.dbConfig);
            console.log('✅ Connected to MySQL database');
            
            // Ensure table can handle large spread values
            await this.ensureTableSchema();
        } catch (error) {
            console.error('❌ Database connection failed:', error);
            throw error;
        }
    }

    async ensureTableSchema() {
        try {
            // Create table if not exists
            await this.connection.execute(`
                CREATE TABLE IF NOT EXISTS pulse_market_data (
                    id BIGINT AUTO_INCREMENT,
                    symbol VARCHAR(20) NOT NULL,
                    timeframe VARCHAR(10) NOT NULL,
                    timestamp DATETIME NOT NULL,
                    open DECIMAL(18,8),
                    high DECIMAL(18,8),
                    low DECIMAL(18,8),
                    close DECIMAL(18,8),
                    volume DECIMAL(24,8) DEFAULT 0,
                    spread DECIMAL(16,2) DEFAULT 0,
                    PRIMARY KEY (id),
                    UNIQUE KEY unique_candle (symbol, timeframe, timestamp),
                    INDEX idx_symbol_time (symbol, timestamp)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            `);
            
            // Try to alter spread column for existing tables
            try {
                await this.connection.execute(`
                    ALTER TABLE pulse_market_data 
                    MODIFY COLUMN spread DECIMAL(16,2) DEFAULT 0
                `);
                console.log('✅ Updated spread column to DECIMAL(16,2)');
            } catch (e) {
                // Column might already be correct
            }
        } catch (error) {
            console.error('Schema setup error:', error.message);
        }
    }

    async importCSV(filePath, symbol, timeframe) {
        console.log(`📂 Importing ${filePath} for ${symbol} ${timeframe}`);
        
        // First, read all lines into memory (for files under ~1GB this is fine)
        const lines = await this.readAllLines(filePath);
        console.log(`📄 Read ${lines.length} lines from file`);
        
        let importedCount = 0;
        let errorCount = 0;
        let batch = [];
        
        for (let i = 0; i < lines.length; i++) {
            const line = lines[i];
            
            // Skip empty lines
            if (!line.trim()) continue;
            
            // Skip header line
            if (i === 0 && this.isHeaderLine(line)) {
                console.log('📋 Skipping header line');
                continue;
            }
            
            const candle = this.parseLine(line, symbol, timeframe);
            if (candle) {
                batch.push(candle);
                importedCount++;
                
                // Insert batch when full
                if (batch.length >= this.batchSize) {
                    await this.insertBatch(batch);
                    batch = [];
                    
                    // Show progress
                    if (importedCount % 5000 === 0) {
                        console.log(`📊 Processed ${importedCount} candles...`);
                    }
                }
            } else {
                errorCount++;
            }
        }
        
        // Insert remaining batch
        if (batch.length > 0) {
            await this.insertBatch(batch);
        }
        
        console.log(`✅ Import completed: ${importedCount} imported, ${errorCount} errors`);
        return { imported: importedCount, errors: errorCount };
    }

    async readAllLines(filePath) {
        return new Promise((resolve, reject) => {
            const lines = [];
            const fileStream = fs.createReadStream(filePath);
            const rl = readline.createInterface({
                input: fileStream,
                crlfDelay: Infinity
            });

            rl.on('line', (line) => {
                lines.push(line);
            });

            rl.on('close', () => {
                resolve(lines);
            });

            rl.on('error', (error) => {
                reject(error);
            });

            fileStream.on('error', (error) => {
                reject(error);
            });
        });
    }

    isHeaderLine(line) {
        const lower = line.toLowerCase();
        return lower.includes('timestamp') || 
               lower.includes('date') || 
               lower.includes('time') ||
               lower.includes('open') && lower.includes('close');
    }

    parseLine(line, symbol, timeframe) {
        // Expected format: "2009-11-29 00:00,1.50094,1.50197,1.4962,1.49943,2955"
        const parts = line.split(',');
        
        if (parts.length < 5) {
            return null;
        }

        const [dateTime, open, high, low, close, spread] = parts;
        
        // Parse the timestamp
        let timestamp;
        try {
            timestamp = new Date(dateTime.trim());
            if (isNaN(timestamp.getTime())) {
                return null;
            }
        } catch (error) {
            return null;
        }

        // Convert to numbers
        const openPrice = parseFloat(open);
        const highPrice = parseFloat(high);
        const lowPrice = parseFloat(low);
        const closePrice = parseFloat(close);
        
        // Handle spread
        let spreadValue = spread ? parseFloat(spread) : 0;
        if (isNaN(spreadValue) || spreadValue > 9999999999) {
            spreadValue = 0;
        }

        // Validate numbers
        if (isNaN(openPrice) || isNaN(highPrice) || isNaN(lowPrice) || isNaN(closePrice)) {
            return null;
        }

        return {
            symbol,
            timeframe,
            timestamp,
            open: openPrice,
            high: highPrice,
            low: lowPrice,
            close: closePrice,
            volume: 0,
            spread: spreadValue
        };
    }

    async insertBatch(batch) {
        if (batch.length === 0) return;

        try {
            const placeholders = batch.map(() => '(?, ?, ?, ?, ?, ?, ?, ?, ?)').join(',');
            const values = batch.flatMap(c => [
                c.symbol,
                c.timeframe,
                c.timestamp,
                c.open,
                c.high,
                c.low,
                c.close,
                c.volume,
                c.spread
            ]);

            await this.connection.execute(`
                INSERT IGNORE INTO pulse_market_data 
                (symbol, timeframe, timestamp, open, high, low, close, volume, spread)
                VALUES ${placeholders}
            `, values);
        } catch (error) {
            console.error('❌ Batch insert error:', error.message);
            // Fall back to individual inserts on error
            for (const candle of batch) {
                try {
                    await this.connection.execute(`
                        INSERT IGNORE INTO pulse_market_data 
                        (symbol, timeframe, timestamp, open, high, low, close, volume, spread)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    `, [
                        candle.symbol, candle.timeframe, candle.timestamp,
                        candle.open, candle.high, candle.low, candle.close,
                        candle.volume, candle.spread
                    ]);
                } catch (e) {
                    // Skip duplicates silently
                }
            }
        }
    }

    async disconnect() {
        if (this.connection) {
            await this.connection.end();
            console.log('📌 Database connection closed');
        }
    }
}

// Command line interface
async function main() {
    const args = process.argv.slice(2);
    
    if (args.length < 3) {
        console.log(`
Usage: node import-historical.js <symbol> <timeframe> <csv_file_path>

Arguments:
  symbol        - Trading symbol (e.g., EURUSD, BTCUSD)
  timeframe     - Timeframe (M1, M5, M15, M30, H1, H4, D1, W1, MN)
  csv_file_path - Path to CSV file

Example:
  node import-historical.js EURUSD D1 ./data/EURUSD_D1.csv

CSV Format:
  timestamp,open,high,low,close,spread
  2009-11-29 00:00,1.50094,1.50197,1.4962,1.49943,2955
        `);
        process.exit(1);
    }

    const [symbol, timeframe, filePath] = args;
    
    if (!fs.existsSync(filePath)) {
        console.error('❌ File not found:', filePath);
        process.exit(1);
    }

    const importer = new HistoricalDataImporter();
    
    try {
        await importer.connect();
        await importer.importCSV(filePath, symbol.toUpperCase(), timeframe.toUpperCase());
    } catch (error) {
        console.error('❌ Import failed:', error);
    } finally {
        await importer.disconnect();
    }
}

if (require.main === module) {
    main();
}

module.exports = HistoricalDataImporter;