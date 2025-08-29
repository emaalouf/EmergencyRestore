#!/usr/bin/env node

const sql = require('mssql');
require('dotenv').config();

// Disaster Recovery Configuration
const recoveryConfig = {
  // Source Database (where data currently exists)
  source: {
    server: process.env.SOURCE_DB_SERVER,
    port: parseInt(process.env.SOURCE_DB_PORT) || 1433,
    database: process.env.SOURCE_DB_DATABASE,
    user: process.env.SOURCE_DB_USER,
    password: process.env.SOURCE_DB_PASSWORD,
    options: {
      encrypt: process.env.SOURCE_DB_ENCRYPT === 'true',
      trustServerCertificate: process.env.SOURCE_DB_TRUST_SERVER_CERTIFICATE === 'true',
      connectionTimeout: parseInt(process.env.SOURCE_DB_CONNECTION_TIMEOUT) || 60000,
      requestTimeout: parseInt(process.env.SOURCE_DB_REQUEST_TIMEOUT) || 120000,
    },
    pool: {
      max: 10,
      min: 0,
      idleTimeoutMillis: 30000,
    },
  },

  // Target Database (where data needs to be restored)
  target: {
    server: process.env.TARGET_DB_SERVER,
    port: parseInt(process.env.TARGET_DB_PORT) || 1433,
    database: process.env.TARGET_DB_DATABASE,
    user: process.env.TARGET_DB_USER,
    password: process.env.TARGET_DB_PASSWORD,
    options: {
      encrypt: process.env.TARGET_DB_ENCRYPT === 'true',
      trustServerCertificate: process.env.TARGET_DB_TRUST_SERVER_CERTIFICATE === 'true',
      connectionTimeout: parseInt(process.env.TARGET_DB_CONNECTION_TIMEOUT) || 60000,
      requestTimeout: parseInt(process.env.TARGET_DB_REQUEST_TIMEOUT) || 120000,
    },
    pool: {
      max: 10,
      min: 0,
      idleTimeoutMillis: 30000,
    },
  },

  // Migration Settings
  migration: {
    exportPath: process.env.MIGRATION_EXPORT_PATH || './disaster_recovery_export',
    validateData: process.env.VALIDATE_DATA === 'true',
    createBackup: process.env.CREATE_BACKUP === 'true',
    backupPath: process.env.BACKUP_PATH || './pre_migration_backup',
    maxRetries: parseInt(process.env.MAX_RETRIES) || 3,
    retryDelay: parseInt(process.env.RETRY_DELAY) || 5000,
  }
};

// Logger utility with timestamps
class Logger {
  static log(message, type = 'INFO') {
    const timestamp = new Date().toISOString();
    console.log(`[${timestamp}] [${type}] ${message}`);
  }

  static error(message) {
    this.log(message, 'ERROR');
  }

  static success(message) {
    this.log(message, 'SUCCESS');
  }

  static warning(message) {
    this.log(message, 'WARNING');
  }

  static critical(message) {
    this.log(message, 'CRITICAL');
  }
}

// Disaster Recovery Manager
class DisasterRecoveryManager {
  constructor() {
    this.sourcePool = null;
    this.targetPool = null;
    this.migrationStats = {
      startTime: null,
      endTime: null,
      tablesProcessed: 0,
      rowsMigrated: 0,
      errors: [],
      warnings: []
    };
  }

  async connectToSource() {
    try {
      Logger.log('🔌 Connecting to SOURCE database...');
      this.sourcePool = await sql.connect(recoveryConfig.source);
      Logger.success('✅ Connected to source database successfully');

      // Validate source connection
      const result = await this.sourcePool.request().query('SELECT @@VERSION as version');
      Logger.log(`📊 Source SQL Server Version: ${result.recordset[0].version.split('\n')[0]}`);

      return true;
    } catch (error) {
      Logger.error(`❌ Failed to connect to source database: ${error.message}`);
      throw error;
    }
  }

  async connectToTarget() {
    try {
      Logger.log('🔌 Connecting to TARGET database...');
      this.targetPool = await sql.connect(recoveryConfig.target);
      Logger.success('✅ Connected to target database successfully');

      // Validate target connection
      const result = await this.targetPool.request().query('SELECT @@VERSION as version');
      Logger.log(`📊 Target SQL Server Version: ${result.recordset[0].version.split('\n')[0]}`);

      return true;
    } catch (error) {
      Logger.error(`❌ Failed to connect to target database: ${error.message}`);
      throw error;
    }
  }

  async validateSourceData() {
    try {
      Logger.log('🔍 Validating source database...');

      // Get database size and table counts
      const dbSizeResult = await this.sourcePool.request().query(`
        SELECT
          DB_NAME() as database_name,
          SUM(size * 8.0 / 1024 / 1024) as size_gb
        FROM sys.master_files
        WHERE database_id = DB_ID()
        GROUP BY database_id
      `);

      Logger.log(`📏 Source database size: ${dbSizeResult.recordset[0].size_gb.toFixed(2)} GB`);

      // Get table information
      const tablesResult = await this.sourcePool.request().query(`
        SELECT
          t.TABLE_SCHEMA,
          t.TABLE_NAME,
          SUM(p.rows) as row_count
        FROM INFORMATION_SCHEMA.TABLES t
        LEFT JOIN sys.tables st ON t.TABLE_NAME = st.name
        LEFT JOIN sys.partitions p ON st.object_id = p.object_id AND p.index_id IN (0,1)
        WHERE t.TABLE_TYPE = 'BASE TABLE'
        GROUP BY t.TABLE_SCHEMA, t.TABLE_NAME
        ORDER BY SUM(p.rows) DESC
      `);

      Logger.log(`📋 Found ${tablesResult.recordset.length} tables:`);
      tablesResult.recordset.slice(0, 10).forEach(table => {
        Logger.log(`   - ${table.TABLE_SCHEMA}.${table.TABLE_NAME}: ${table.row_count.toLocaleString()} rows`);
      });

      if (tablesResult.recordset.length > 10) {
        Logger.log(`   ... and ${tablesResult.recordset.length - 10} more tables`);
      }

      return {
        databaseSize: dbSizeResult.recordset[0].size_gb,
        tableCount: tablesResult.recordset.length,
        tables: tablesResult.recordset
      };
    } catch (error) {
      Logger.error(`❌ Error validating source data: ${error.message}`);
      throw error;
    }
  }

  async createTargetBackup() {
    if (!recoveryConfig.migration.createBackup) {
      Logger.log('⏭️  Skipping target backup (CREATE_BACKUP=false)');
      return;
    }

    try {
      Logger.log('💾 Creating backup of target database before migration...');

      // Create backup directory
      const fs = require('fs').promises;
      await fs.mkdir(recoveryConfig.migration.backupPath, { recursive: true });

      // Get target database info
      const dbInfo = await this.targetPool.request().query(`
        SELECT
          t.TABLE_SCHEMA,
          t.TABLE_NAME,
          SUM(p.rows) as row_count
        FROM INFORMATION_SCHEMA.TABLES t
        LEFT JOIN sys.tables st ON t.TABLE_NAME = st.name
        LEFT JOIN sys.partitions p ON st.object_id = p.object_id AND p.index_id IN (0,1)
        WHERE t.TABLE_TYPE = 'BASE TABLE'
        GROUP BY t.TABLE_SCHEMA, t.TABLE_NAME
        HAVING SUM(p.rows) > 0
      `);

      if (dbInfo.recordset.length === 0) {
        Logger.log('📭 Target database is empty, no backup needed');
        return;
      }

      Logger.warning(`⚠️  Target database contains ${dbInfo.recordset.length} tables with data`);
      Logger.warning('💾 Creating backup before proceeding with migration...');

      // Export target data as backup
      const { DatabaseExporter } = require('./export-db');

      // Temporarily switch config to target
      const originalConfig = { ...recoveryConfig.source };
      recoveryConfig.source = recoveryConfig.target;
      process.env.EXPORT_PATH = recoveryConfig.migration.backupPath;

      const exporter = new DatabaseExporter();
      await exporter.connect();
      const backupResult = await exporter.exportDatabase();
      await exporter.disconnect();

      // Restore original config
      recoveryConfig.source = originalConfig;

      Logger.success(`✅ Target backup created in: ${recoveryConfig.migration.backupPath}`);
      return backupResult;

    } catch (error) {
      Logger.error(`❌ Error creating target backup: ${error.message}`);
      throw error;
    }
  }

  async performMigration() {
    try {
      Logger.critical('🚨 STARTING DISASTER RECOVERY MIGRATION 🚨');
      Logger.critical('⚠️  This will REPLACE all data in the target database ⚠️');

      this.migrationStats.startTime = new Date();

      // Step 1: Validate source data
      const sourceInfo = await this.validateSourceData();

      // Step 2: Create target backup if needed
      await this.createTargetBackup();

      // Step 3: Export from source
      Logger.log('📤 Step 1/3: Exporting data from source database...');
      const { DatabaseExporter } = require('./export-db');

      // Configure export for source
      process.env.DB_SERVER = recoveryConfig.source.server;
      process.env.DB_PORT = recoveryConfig.source.port;
      process.env.DB_DATABASE = recoveryConfig.source.database;
      process.env.DB_USER = recoveryConfig.source.user;
      process.env.DB_PASSWORD = recoveryConfig.source.password;
      process.env.EXPORT_PATH = recoveryConfig.migration.exportPath;
      process.env.MAX_ROWS_PER_FILE = '50000'; // Smaller chunks for faster processing

      const exporter = new DatabaseExporter();
      await exporter.connect();
      const exportResult = await exporter.exportDatabase();
      await exporter.disconnect();

      Logger.success('✅ Source data exported successfully');

      // Step 4: Import to target
      Logger.log('📥 Step 2/3: Importing data to target database...');
      const { DatabaseImporter } = require('./import-db');

      // Configure import for target
      process.env.DB_SERVER = recoveryConfig.target.server;
      process.env.DB_PORT = recoveryConfig.target.port;
      process.env.DB_USER = recoveryConfig.target.user;
      process.env.DB_PASSWORD = recoveryConfig.target.password;
      process.env.TARGET_DATABASE = recoveryConfig.target.database;
      process.env.IMPORT_PATH = recoveryConfig.migration.exportPath;
      process.env.DROP_EXISTING = 'true';
      process.env.CREATE_DATABASE = 'false'; // Assume target DB exists

      const importer = new DatabaseImporter();
      const importResult = await importer.importDatabase();

      Logger.success('✅ Data imported to target successfully');

      // Step 5: Validate migration
      Logger.log('🔍 Step 3/3: Validating migration...');
      const validationResult = await this.validateMigration(sourceInfo);

      this.migrationStats.endTime = new Date();

      // Generate migration report
      await this.generateMigrationReport(sourceInfo, exportResult, importResult, validationResult);

      Logger.critical('🎉 DISASTER RECOVERY MIGRATION COMPLETED SUCCESSFULLY! 🎉');

      return {
        success: true,
        sourceInfo,
        exportResult,
        importResult,
        validationResult,
        stats: this.migrationStats
      };

    } catch (error) {
      Logger.critical(`💥 DISASTER RECOVERY MIGRATION FAILED: ${error.message}`);
      this.migrationStats.errors.push(error.message);
      throw error;
    }
  }

  async validateMigration(sourceInfo) {
    try {
      Logger.log('🔍 Validating migration results...');

      // Get target database info
      const targetTables = await this.targetPool.request().query(`
        SELECT
          t.TABLE_SCHEMA,
          t.TABLE_NAME,
          SUM(p.rows) as row_count
        FROM INFORMATION_SCHEMA.TABLES t
        LEFT JOIN sys.tables st ON t.TABLE_NAME = st.name
        LEFT JOIN sys.partitions p ON st.object_id = p.object_id AND p.index_id IN (0,1)
        WHERE t.TABLE_TYPE = 'BASE TABLE'
        GROUP BY t.TABLE_SCHEMA, t.TABLE_NAME
        ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
      `);

      // Compare source and target
      const sourceMap = new Map();
      sourceInfo.tables.forEach(table => {
        sourceMap.set(`${table.TABLE_SCHEMA}.${table.TABLE_NAME}`, table.row_count);
      });

      const targetMap = new Map();
      targetTables.recordset.forEach(table => {
        targetMap.set(`${table.TABLE_SCHEMA}.${table.TABLE_NAME}`, table.row_count);
      });

      let totalSourceRows = 0;
      let totalTargetRows = 0;
      const discrepancies = [];

      for (const [tableName, sourceRows] of sourceMap) {
        const targetRows = targetMap.get(tableName);
        totalSourceRows += sourceRows;

        if (targetRows === undefined) {
          discrepancies.push(`${tableName}: Missing in target`);
        } else if (sourceRows !== targetRows) {
          discrepancies.push(`${tableName}: Source=${sourceRows}, Target=${targetRows}`);
        } else {
          totalTargetRows += targetRows;
        }
      }

      const validation = {
        sourceTableCount: sourceInfo.tables.length,
        targetTableCount: targetTables.recordset.length,
        totalSourceRows,
        totalTargetRows,
        discrepancies,
        success: discrepancies.length === 0
      };

      if (validation.success) {
        Logger.success(`✅ Migration validation PASSED: ${totalSourceRows} rows migrated successfully`);
      } else {
        Logger.warning(`⚠️  Migration validation found ${discrepancies.length} discrepancies:`);
        discrepancies.forEach(discrepancy => Logger.warning(`   - ${discrepancy}`));
      }

      return validation;

    } catch (error) {
      Logger.error(`❌ Error validating migration: ${error.message}`);
      throw error;
    }
  }

  async generateMigrationReport(sourceInfo, exportResult, importResult, validationResult) {
    try {
      const report = {
        migration_report: {
          timestamp: new Date().toISOString(),
          duration: this.migrationStats.endTime - this.migrationStats.startTime,
          source: {
            server: recoveryConfig.source.server,
            database: recoveryConfig.source.database,
            size_gb: sourceInfo.databaseSize,
            table_count: sourceInfo.tableCount
          },
          target: {
            server: recoveryConfig.target.server,
            database: recoveryConfig.target.database
          },
          results: {
            export_success: exportResult.schema_exported && exportResult.data_exported,
            import_success: importResult.schema_imported && importResult.data_imported,
            validation_success: validationResult.success,
            tables_created: importResult.tables_created,
            rows_migrated: validationResult.totalTargetRows
          },
          validation: validationResult,
          export_path: recoveryConfig.migration.exportPath,
          backup_path: recoveryConfig.migration.backupPath
        },
        errors: this.migrationStats.errors,
        warnings: this.migrationStats.warnings
      };

      // Write report to file
      const fs = require('fs').promises;
      await fs.mkdir(recoveryConfig.migration.exportPath, { recursive: true });
      await fs.writeFile(
        `${recoveryConfig.migration.exportPath}/migration_report.json`,
        JSON.stringify(report, null, 2)
      );

      Logger.success(`📄 Migration report saved to: ${recoveryConfig.migration.exportPath}/migration_report.json`);

      return report;

    } catch (error) {
      Logger.error(`❌ Error generating migration report: ${error.message}`);
      throw error;
    }
  }

  async disconnect() {
    try {
      if (this.sourcePool) {
        await this.sourcePool.close();
        Logger.log('Source database connection closed');
      }
      if (this.targetPool) {
        await this.targetPool.close();
        Logger.log('Target database connection closed');
      }
    } catch (error) {
      Logger.error(`Error closing connections: ${error.message}`);
    }
  }

  async executeDisasterRecovery() {
    try {
      Logger.critical('🚨 DISASTER RECOVERY MODE ACTIVATED 🚨');
      Logger.critical('This will migrate data from source to target database');
      Logger.critical('Ensure you have backups and understand the risks!');

      // Validate configuration
      if (!this.validateConfiguration()) {
        throw new Error('Invalid disaster recovery configuration');
      }

      // Connect to both databases
      await this.connectToSource();
      await this.connectToTarget();

      // Execute migration
      const result = await this.performMigration();

      Logger.critical('🎉 DISASTER RECOVERY COMPLETED SUCCESSFULLY! 🎉');
      return result;

    } catch (error) {
      Logger.critical(`💥 DISASTER RECOVERY FAILED: ${error.message}`);
      throw error;
    } finally {
      await this.disconnect();
    }
  }

  validateConfiguration() {
    const required = [
      'SOURCE_DB_SERVER', 'SOURCE_DB_DATABASE', 'SOURCE_DB_USER', 'SOURCE_DB_PASSWORD',
      'TARGET_DB_SERVER', 'TARGET_DB_DATABASE', 'TARGET_DB_USER', 'TARGET_DB_PASSWORD'
    ];

    const missing = required.filter(key => !process.env[key]);

    if (missing.length > 0) {
      Logger.error(`Missing required environment variables: ${missing.join(', ')}`);
      Logger.error('Please configure your .env file with source and target database credentials');
      return false;
    }

    // Warn about same server/database
    if (recoveryConfig.source.server === recoveryConfig.target.server &&
        recoveryConfig.source.database === recoveryConfig.target.database) {
      Logger.warning('⚠️  Source and target are the same database!');
      Logger.warning('This will overwrite your source data!');
      return false;
    }

    return true;
  }
}

// Main execution function
async function main() {
  try {
    Logger.log('🚨 Starting Disaster Recovery Migration...');

    const recoveryManager = new DisasterRecoveryManager();
    const result = await recoveryManager.executeDisasterRecovery();

    console.log('\n' + '='.repeat(60));
    console.log('📊 MIGRATION SUMMARY');
    console.log('='.repeat(60));
    console.log(`✅ Status: ${result.success ? 'SUCCESS' : 'FAILED'}`);
    console.log(`⏱️  Duration: ${Math.round((result.stats.endTime - result.stats.startTime) / 1000)} seconds`);
    console.log(`📋 Tables: ${result.sourceInfo.tableCount}`);
    console.log(`📊 Rows: ${result.validation.totalTargetRows.toLocaleString()}`);
    console.log(`📁 Export Path: ${recoveryConfig.migration.exportPath}`);
    if (recoveryConfig.migration.createBackup) {
      console.log(`💾 Backup Path: ${recoveryConfig.migration.backupPath}`);
    }
    console.log('='.repeat(60));

    if (result.validation.discrepancies.length > 0) {
      console.log('\n⚠️  DISCREPANCIES FOUND:');
      result.validation.discrepancies.forEach(discrepancy => {
        console.log(`   - ${discrepancy}`);
      });
    }

    process.exit(0);

  } catch (error) {
    Logger.critical(`💥 Disaster recovery failed: ${error.message}`);
    process.exit(1);
  }
}

// Handle process termination
process.on('SIGINT', () => {
  Logger.critical('Received SIGINT - Emergency stop initiated');
  process.exit(130);
});

process.on('SIGTERM', () => {
  Logger.critical('Received SIGTERM - Emergency stop initiated');
  process.exit(143);
});

// Run disaster recovery if this file is executed directly
if (require.main === module) {
  main();
}

module.exports = { DisasterRecoveryManager, Logger };