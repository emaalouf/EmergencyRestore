#!/usr/bin/env node

const sql = require('mssql');
const fs = require('fs').promises;
const path = require('path');
require('dotenv').config();

// Configuration from environment variables
const config = {
  server: process.env.DB_SERVER || 'localhost',
  port: parseInt(process.env.DB_PORT) || 1433,
  database: process.env.DB_DATABASE,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  options: {
    encrypt: process.env.DB_ENCRYPT === 'true',
    trustServerCertificate: process.env.DB_TRUST_SERVER_CERTIFICATE === 'true',
    connectionTimeout: parseInt(process.env.DB_CONNECTION_TIMEOUT) || 30000,
    requestTimeout: parseInt(process.env.DB_REQUEST_TIMEOUT) || 30000,
  },
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000,
  },
};

const exportConfig = {
  path: process.env.EXPORT_PATH || './exports',
  format: process.env.EXPORT_FORMAT || 'json',
  includeSchema: process.env.INCLUDE_SCHEMA === 'true',
  includeData: process.env.INCLUDE_DATA === 'true',
};

// Logger utility
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
}

// Database connection class
class DatabaseExporter {
  constructor() {
    this.pool = null;
  }

  async connect() {
    try {
      Logger.log('Attempting to connect to MSSQL database...');
      this.pool = await sql.connect(config);
      Logger.success('Successfully connected to database');
      return true;
    } catch (error) {
      Logger.error(`Failed to connect to database: ${error.message}`);
      throw error;
    }
  }

  async disconnect() {
    try {
      if (this.pool) {
        await this.pool.close();
        Logger.log('Database connection closed');
      }
    } catch (error) {
      Logger.error(`Error closing database connection: ${error.message}`);
    }
  }

  async getAllTables() {
    try {
      Logger.log('Retrieving list of all tables...');
      const result = await this.pool.request().query(`
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
      `);

      const tables = result.recordset.map(row => ({
        schema: row.TABLE_SCHEMA,
        name: row.TABLE_NAME,
        fullName: `${row.TABLE_SCHEMA}.${row.TABLE_NAME}`
      }));

      Logger.success(`Found ${tables.length} tables`);
      return tables;
    } catch (error) {
      Logger.error(`Error retrieving tables: ${error.message}`);
      throw error;
    }
  }

  async exportSchema() {
    try {
      Logger.log('Starting schema export...');

      // Get all tables with their structure
      const tables = await this.getAllTables();
      const schema = {
        database: config.database,
        exported_at: new Date().toISOString(),
        tables: []
      };

      for (const table of tables) {
        Logger.log(`Exporting schema for table: ${table.fullName}`);

        // Get column information
        const columnsResult = await this.pool.request().query(`
          SELECT
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            IS_NULLABLE,
            COLUMN_DEFAULT
          FROM INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_SCHEMA = '${table.schema}'
            AND TABLE_NAME = '${table.name}'
          ORDER BY ORDINAL_POSITION
        `);

        // Get primary key information
        const pkResult = await this.pool.request().query(`
          SELECT COLUMN_NAME
          FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
          WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
            AND TABLE_SCHEMA = '${table.schema}'
            AND TABLE_NAME = '${table.name}'
        `);

        const primaryKeys = pkResult.recordset.map(row => row.COLUMN_NAME);

        // Get foreign key information
        const fkResult = await this.pool.request().query(`
          SELECT
            fk.name AS FK_NAME,
            tp.name AS parent_table,
            tr.name AS referenced_table,
            cp.name AS parent_column,
            cr.name AS referenced_column
          FROM sys.foreign_keys fk
          INNER JOIN sys.tables tp ON fk.parent_object_id = tp.object_id
          INNER JOIN sys.tables tr ON fk.referenced_object_id = tr.object_id
          INNER JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
          INNER JOIN sys.columns cp ON fkc.parent_column_id = cp.column_id AND fkc.parent_object_id = cp.object_id
          INNER JOIN sys.columns cr ON fkc.referenced_column_id = cr.column_id AND fkc.referenced_object_id = cr.object_id
          WHERE tp.name = '${table.name}' AND SCHEMA_NAME(tp.schema_id) = '${table.schema}'
        `);

        const tableSchema = {
          schema: table.schema,
          name: table.name,
          columns: columnsResult.recordset,
          primary_keys: primaryKeys,
          foreign_keys: fkResult.recordset
        };

        schema.tables.push(tableSchema);
      }

      // Ensure export directory exists
      await fs.mkdir(exportConfig.path, { recursive: true });

      // Write schema to file
      const schemaPath = path.join(exportConfig.path, 'schema.json');
      await fs.writeFile(schemaPath, JSON.stringify(schema, null, 2));
      Logger.success(`Schema exported to: ${schemaPath}`);

      return schema;
    } catch (error) {
      Logger.error(`Error exporting schema: ${error.message}`);
      throw error;
    }
  }

  async exportTableData(table) {
    try {
      Logger.log(`Exporting data for table: ${table.fullName}`);

      // Get row count first
      const countResult = await this.pool.request().query(`
        SELECT COUNT(*) as row_count
        FROM ${table.fullName}
      `);

      const rowCount = countResult.recordset[0].row_count;
      Logger.log(`Table ${table.fullName} has ${rowCount} rows`);

      if (rowCount === 0) {
        Logger.warning(`No data found in table: ${table.fullName}`);
        return { table: table.fullName, rowCount: 0, data: [] };
      }

      // Export data in batches to handle large tables
      const batchSize = 1000000;
      let allData = [];
      let offset = 0;

      while (offset < rowCount) {
        const dataResult = await this.pool.request().query(`
          SELECT *
          FROM ${table.fullName}
          ORDER BY (SELECT NULL)
          OFFSET ${offset} ROWS
          FETCH NEXT ${batchSize} ROWS ONLY
        `);

        allData = allData.concat(dataResult.recordset);
        offset += batchSize;

        Logger.log(`Exported ${allData.length} of ${rowCount} rows from ${table.fullName}`);
      }

      const tableData = {
        table: table.fullName,
        rowCount: rowCount,
        exported_at: new Date().toISOString(),
        data: allData
      };

      // Write table data to file
      const fileName = `${table.schema}_${table.name}_data.json`;
      const dataPath = path.join(exportConfig.path, fileName);
      await fs.writeFile(dataPath, JSON.stringify(tableData, null, 2));

      Logger.success(`Data exported to: ${dataPath}`);
      return tableData;
    } catch (error) {
      Logger.error(`Error exporting data for table ${table.fullName}: ${error.message}`);
      throw error;
    }
  }

  async exportAllData() {
    try {
      Logger.log('Starting data export for all tables...');

      const tables = await this.getAllTables();
      const exportedData = {
        database: config.database,
        exported_at: new Date().toISOString(),
        tables: []
      };

      for (const table of tables) {
        try {
          const tableData = await this.exportTableData(table);
          exportedData.tables.push({
            table: table.fullName,
            rowCount: tableData.rowCount,
            file: `${table.schema}_${table.name}_data.json`
          });
        } catch (error) {
          Logger.error(`Failed to export data for table ${table.fullName}: ${error.message}`);
          // Continue with other tables
        }
      }

      // Write summary file
      const summaryPath = path.join(exportConfig.path, 'data_export_summary.json');
      await fs.writeFile(summaryPath, JSON.stringify(exportedData, null, 2));
      Logger.success(`Data export summary written to: ${summaryPath}`);

      return exportedData;
    } catch (error) {
      Logger.error(`Error in data export: ${error.message}`);
      throw error;
    }
  }

  async exportDatabase() {
    try {
      Logger.log('Starting full database export...');

      const result = {
        database: config.database,
        exported_at: new Date().toISOString(),
        schema_exported: false,
        data_exported: false,
        files: []
      };

      // Connect to database
      await this.connect();

      // Export schema if requested
      if (exportConfig.includeSchema) {
        await this.exportSchema();
        result.schema_exported = true;
        result.files.push('schema.json');
      }

      // Export data if requested
      if (exportConfig.includeData) {
        await this.exportAllData();
        result.data_exported = true;
        result.files.push('data_export_summary.json');
      }

      Logger.success('Database export completed successfully');
      Logger.log(`Files exported to: ${path.resolve(exportConfig.path)}`);

      return result;
    } catch (error) {
      Logger.error(`Database export failed: ${error.message}`);
      throw error;
    } finally {
      await this.disconnect();
    }
  }
}

// Validation function
function validateConfiguration() {
  const required = ['DB_DATABASE', 'DB_USER', 'DB_PASSWORD'];
  const missing = required.filter(key => !process.env[key]);

  if (missing.length > 0) {
    Logger.error(`Missing required environment variables: ${missing.join(', ')}`);
    Logger.log('Please update your .env file with the required database credentials.');
    return false;
  }

  return true;
}

// Main execution function
async function main() {
  try {
    Logger.log('MSSQL Database Export Tool Starting...');

    // Validate configuration
    if (!validateConfiguration()) {
      process.exit(1);
    }

    // Create exporter instance
    const exporter = new DatabaseExporter();

    // Perform export
    const result = await exporter.exportDatabase();

    Logger.success('Export completed successfully!');
    console.log('\nExport Summary:');
    console.log(`- Database: ${result.database}`);
    console.log(`- Schema Exported: ${result.schema_exported}`);
    console.log(`- Data Exported: ${result.data_exported}`);
    console.log(`- Files Created: ${result.files.join(', ')}`);
    console.log(`- Export Path: ${path.resolve(exportConfig.path)}`);

  } catch (error) {
    Logger.error(`Export failed: ${error.message}`);
    process.exit(1);
  }
}

// Handle process termination
process.on('SIGINT', () => {
  Logger.log('Received SIGINT, shutting down gracefully...');
  process.exit(0);
});

process.on('SIGTERM', () => {
  Logger.log('Received SIGTERM, shutting down gracefully...');
  process.exit(0);
});

// Run the export if this file is executed directly
if (require.main === module) {
  main();
}

module.exports = { DatabaseExporter, Logger };