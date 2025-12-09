require('dotenv').config();

const HistoricalDataImporter = require('./import-historical');
const fs = require('fs');
const path = require('path');

class BatchImporter {
    constructor() {
        this.importer = new HistoricalDataImporter();
    }

    async importFolder(folderPath) {
        console.log(`📁 Scanning folder: ${folderPath}`);
        
        const files = fs.readdirSync(folderPath);
        const csvFiles = files.filter(file => file.toLowerCase().endsWith('.csv'));
        
        console.log(`📊 Found ${csvFiles.length} CSV files`);
        
        let successCount = 0;
        let failCount = 0;
        
        for (const file of csvFiles) {
            const fileName = path.basename(file, '.csv');
            const [symbol, timeframe] = this.parseFileName(fileName);
            
            if (symbol && timeframe) {
                const filePath = path.join(folderPath, file);
                console.log(`\n📄 Processing: ${file} -> ${symbol} ${timeframe}`);
                
                try {
                    await this.importer.importCSV(filePath, symbol, timeframe);
                    successCount++;
                } catch (error) {
                    console.error(`❌ Failed to import ${file}:`, error.message);
                    failCount++;
                }
            } else {
                console.log(`⚠️  Skipping: ${file} - cannot parse symbol/timeframe from filename`);
                console.log(`   Expected format: SYMBOL_TIMEFRAME.csv (e.g., EURUSD_D1.csv)`);
                failCount++;
            }
        }
        
        console.log(`\n========================================`);
        console.log(`📊 Import Summary:`);
        console.log(`   ✅ Successful: ${successCount}`);
        console.log(`   ❌ Failed: ${failCount}`);
        console.log(`========================================`);
    }

    parseFileName(fileName) {
        // Your files are like: EURUSD_D1.csv, BTCUSDT_M1.csv
        // Pattern: SYMBOL_TIMEFRAME.csv
        
        // Handle different separator styles
        let parts;
        if (fileName.includes('_')) {
            parts = fileName.split('_');
        } else if (fileName.includes('-')) {
            parts = fileName.split('-');
        } else {
            // Try to extract timeframe from end
            const match = fileName.match(/^(.+?)(M1|M5|M15|M30|H1|H4|D1|W1|MN)$/i);
            if (match) {
                parts = [match[1], match[2]];
            } else {
                return [null, null];
            }
        }
        
        if (parts.length >= 2) {
            const symbol = parts[0].toUpperCase();
            const timeframe = parts[parts.length - 1].toUpperCase();
            
            // Validate timeframe format
            const validTimeframes = ['M1', 'M5', 'M15', 'M30', 'H1', 'H4', 'D1', 'W1', 'MN'];
            if (validTimeframes.includes(timeframe)) {
                return [symbol, timeframe];
            }
        }
        
        return [null, null];
    }

    async disconnect() {
        await this.importer.disconnect();
    }
}

// Command line for batch import
async function main() {
    const args = process.argv.slice(2);
    
    if (args.length < 1) {
        console.log(`
🎯 Historical Data Batch Importer

Usage: node batch-import.js <folder_path>

Example:
  node batch-import.js ./data
  node batch-import.js ./historical

File naming convention:
  - EURUSD_D1.csv
  - EURUSD_M1.csv
  - BTCUSD_H1.csv
  - GBPUSD_M15.csv

Supported timeframes:
  M1, M5, M15, M30, H1, H4, D1, W1, MN

CSV format expected:
  timestamp, open, high, low, close, spread

Example CSV line:
  2009-11-29 00:00,1.50094,1.50197,1.4962,1.49943,2955
        `);
        process.exit(1);
    }

    const folderPath = args[0];
    
    if (!fs.existsSync(folderPath)) {
        console.error('❌ Folder not found:', folderPath);
        process.exit(1);
    }

    const batchImporter = new BatchImporter();
    
    try {
        await batchImporter.importer.connect();
        await batchImporter.importFolder(folderPath);
        console.log('\n✅ Batch import completed!');
    } catch (error) {
        console.error('❌ Batch import failed:', error);
    } finally {
        await batchImporter.disconnect();
    }
}

if (require.main === module) {
    main();
}

module.exports = BatchImporter;