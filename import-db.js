#!/usr/bin/env node

const sql = require('mssql');
const fs = require('fs').promises;
const path = require('path');
require('dotenv').config();

// Configuration from environment variables
const config = {
  server: process.env.DB_SERVER,
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

const importConfig = {
  sourcePath: process.env.IMPORT_PATH || './exports',
  targetDatabase: process.env.TARGET_DATABASE || process.env.DB_DATABASE,
  dropExisting: process.env.DROP_EXISTING === 'true',
  createDatabase: process.env.CREATE_DATABASE === 'true',
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

// Database importer class
class DatabaseImporter {
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

  async fileExists(filePath) {
    try {
      await fs.access(filePath);
      return true;
    } catch {
      return false;
    }
  }

  async loadSchema() {
    try {
      Logger.log('Loading schema from export files...');

      const schemaPath = path.join(importConfig.sourcePath, 'schema.json');

      if (!await this.fileExists(schemaPath)) {
        throw new Error(`Schema file not found: ${schemaPath}`);
      }

      const schemaData = await fs.readFile(schemaPath, 'utf8');
      const schema = JSON.parse(schemaData);

      Logger.success(`Loaded schema for database: ${schema.database}`);
      Logger.log(`Found ${schema.tables.length} tables in schema`);

      return schema;
    } catch (error) {
      Logger.error(`Error loading schema: ${error.message}`);
      throw error;
    }
  }

  generateCreateTableSQL(table) {
    const columns = table.columns.map(col => {
      let sql = `[${col.COLUMN_NAME}] ${col.DATA_TYPE}`;

      if (col.CHARACTER_MAXIMUM_LENGTH) {
        sql += `(${col.CHARACTER_MAXIMUM_LENGTH})`;
      } else if (col.NUMERIC_PRECISION && col.NUMERIC_SCALE !== null) {
        sql += `(${col.NUMERIC_PRECISION}, ${col.NUMERIC_SCALE})`;
      } else if (col.NUMERIC_PRECISION) {
        sql += `(${col.NUMERIC_PRECISION})`;
      }

      if (col.IS_NULLABLE === 'NO') {
        sql += ' NOT NULL';
      }

      if (col.COLUMN_DEFAULT) {
        sql += ` DEFAULT ${col.COLUMN_DEFAULT}`;
      }

      return sql;
    }).join(',\n    ');

    let sql = `CREATE TABLE [${table.schema}].[${table.name}] (\n    ${columns}`;

    // Add primary key constraint if exists
    if (table.primary_keys && table.primary_keys.length > 0) {
      const pkColumns = table.primary_keys.map(pk => `[${pk}]`).join(', ');
      sql += `,\n    CONSTRAINT [PK_${table.name}] PRIMARY KEY (${pkColumns})`;
    }

    sql += '\n);';

    return sql;
  }

  async createTables(schema) {
    try {
      Logger.log('Creating database tables...');

      // First, create all tables without foreign keys
      for (const table of schema.tables) {
        Logger.log(`Creating table: ${table.schema}.${table.name}`);

        const createSQL = this.generateCreateTableSQL(table);
        Logger.log(`Executing: ${createSQL.replace(/\n/g, ' ')}`);

        await this.pool.request().query(createSQL);
        Logger.success(`Created table: ${table.schema}.${table.name}`);
      }

      // Then add foreign key constraints
      for (const table of schema.tables) {
        if (table.foreign_keys && table.foreign_keys.length > 0) {
          for (const fk of table.foreign_keys) {
            Logger.log(`Adding foreign key constraint to ${table.schema}.${table.name}`);

            const fkSQL = `
              ALTER TABLE [${table.schema}].[${table.name}]
              ADD CONSTRAINT [${fk.FK_NAME}]
              FOREIGN KEY ([${fk.parent_column}])
              REFERENCES [${fk.referenced_table}] ([${fk.referenced_column}])
            `;

            Logger.log(`Executing: ${fkSQL.replace(/\n/g, ' ')}`);

            try {
              await this.pool.request().query(fkSQL);
              Logger.success(`Added foreign key: ${fk.FK_NAME}`);
            } catch (error) {
              Logger.warning(`Failed to add foreign key ${fk.FK_NAME}: ${error.message}`);
              // Continue with other constraints
            }
          }
        }
      }

      Logger.success(`Created ${schema.tables.length} tables successfully`);
    } catch (error) {
      Logger.error(`Error creating tables: ${error.message}`);
      throw error;
    }
  }

  async loadTableData(table) {
    try {
      // First, check if there's a summary file (indicating chunked export)
      const summaryFileName = `${table.schema}_${table.name}_data_summary.json`;
      const summaryPath = path.join(importConfig.sourcePath, summaryFileName);

      if (await this.fileExists(summaryPath)) {
        Logger.log(`Found chunked data for table: ${table.schema}.${table.name}`);
        return await this.loadChunkedTableData(table, summaryPath);
      }

      // Fall back to single file format
      const dataFileName = `${table.schema}_${table.name}_data.json`;
      const dataPath = path.join(importConfig.sourcePath, dataFileName);

      if (!await this.fileExists(dataPath)) {
        Logger.warning(`Data file not found: ${dataPath}`);
        return null;
      }

      Logger.log(`Loading data for table: ${table.schema}.${table.name}`);

      const dataContent = await fs.readFile(dataPath, 'utf8');
      const tableData = JSON.parse(dataContent);

      Logger.success(`Loaded ${tableData.rowCount} rows for ${table.schema}.${table.name}`);

      return tableData;
    } catch (error) {
      Logger.error(`Error loading data for table ${table.schema}.${table.name}: ${error.message}`);
      throw error;
    }
  }

  async loadChunkedTableData(table, summaryPath) {
    try {
      // Load summary file
      const summaryContent = await fs.readFile(summaryPath, 'utf8');
      const summary = JSON.parse(summaryContent);

      Logger.log(`Loading ${summary.totalChunks} chunks for table: ${table.schema}.${table.name}`);

      let allData = [];
      let totalRows = 0;

      // Load each chunk
      for (const fileInfo of summary.files) {
        const chunkPath = path.join(importConfig.sourcePath, fileInfo.file);

        if (!await this.fileExists(chunkPath)) {
          Logger.warning(`Chunk file not found: ${chunkPath}`);
          continue;
        }

        Logger.log(`Loading chunk ${fileInfo.chunk}/${summary.totalChunks}`);

        const chunkContent = await fs.readFile(chunkPath, 'utf8');
        const chunkData = JSON.parse(chunkContent);

        allData = allData.concat(chunkData.data);
        totalRows += chunkData.rowCount;

        Logger.log(`Loaded chunk ${fileInfo.chunk}, total rows so far: ${totalRows}`);

        // Force garbage collection if available
        if (global.gc) {
          global.gc();
        }
      }

      const tableData = {
        table: table.fullName,
        rowCount: totalRows,
        exported_at: summary.exported_at,
        data: allData,
        isChunked: true,
        totalChunks: summary.totalChunks
      };

      Logger.success(`Loaded ${totalRows} rows from ${summary.totalChunks} chunks for ${table.schema}.${table.name}`);

      return tableData;
    } catch (error) {
      Logger.error(`Error loading chunked data: ${error.message}`);
      throw error;
    }
  }

  async importTableData(table, tableData) {
    try {
      if (!tableData || tableData.rowCount === 0) {
        Logger.log(`No data to import for table: ${table.schema}.${table.name}`);
        return;
      }

      Logger.log(`Importing ${tableData.rowCount} rows into ${table.schema}.${table.name}`);

      const columns = table.columns.map(col => col.COLUMN_NAME);
      const columnList = columns.map(col => `[${col}]`).join(', ');

      // Process data in batches to handle large datasets
      const batchSize = 1000;
      let importedCount = 0;

      for (let i = 0; i < tableData.data.length; i += batchSize) {
        const batch = tableData.data.slice(i, i + batchSize);

        // Build INSERT statement
        const valuePlaceholders = batch.map((_, index) =>
          `(${columns.map((_, colIndex) => `@p${index}_${colIndex}`).join(', ')})`
        ).join(',\n    ');

        const insertSQL = `
          INSERT INTO [${table.schema}].[${table.name}] (${columnList})
          VALUES ${valuePlaceholders}
        `;

        const request = this.pool.request();

        // Add parameters for this batch
        batch.forEach((row, rowIndex) => {
          columns.forEach((col, colIndex) => {
            const paramName = `p${rowIndex}_${colIndex}`;
            const value = row[col];

            // Handle different data types
            if (value === null || value === undefined) {
              request.input(paramName, null);
            } else if (typeof value === 'boolean') {
              request.input(paramName, sql.Bit, value);
            } else if (typeof value === 'number') {
              if (Number.isInteger(value)) {
                request.input(paramName, sql.Int, value);
              } else {
                request.input(paramName, sql.Decimal(18, 6), value);
              }
            } else if (value instanceof Date) {
              request.input(paramName, sql.DateTime, value);
            } else {
              request.input(paramName, sql.NVarChar, value.toString());
            }
          });
        });

        await request.query(insertSQL);
        importedCount += batch.length;

        Logger.log(`Imported ${importedCount}/${tableData.rowCount} rows for ${table.schema}.${table.name}`);
      }

      Logger.success(`Successfully imported ${importedCount} rows into ${table.schema}.${table.name}`);
    } catch (error) {
      Logger.error(`Error importing data for table ${table.schema}.${table.name}: ${error.message}`);
      throw error;
    }
  }

  async importAllData(schema) {
    try {
      Logger.log('Starting data import for all tables...');

      let totalImported = 0;
      let totalTables = 0;

      for (const table of schema.tables) {
        try {
          const tableData = await this.loadTableData(table);
          if (tableData) {
            await this.importTableData(table, tableData);
            totalImported += tableData.rowCount;
            totalTables++;
          }
        } catch (error) {
          Logger.error(`Failed to import data for table ${table.schema}.${table.name}: ${error.message}`);
          // Continue with other tables
        }
      }

      Logger.success(`Data import completed. Imported data for ${totalTables} tables, ${totalImported} total rows`);
    } catch (error) {
      Logger.error(`Error in data import: ${error.message}`);
      throw error;
    }
  }

  async importDatabase() {
    try {
      Logger.log('Starting database import...');

      const result = {
        database: importConfig.targetDatabase,
        imported_at: new Date().toISOString(),
        schema_imported: false,
        data_imported: false,
        tables_created: 0,
        rows_imported: 0
      };

      // Connect to database
      await this.connect();

      // Load and create schema
      const schema = await this.loadSchema();
      await this.createTables(schema);
      result.schema_imported = true;
      result.tables_created = schema.tables.length;

      // Import data
      await this.importAllData(schema);
      result.data_imported = true;

      Logger.success('Database import completed successfully');
      Logger.log(`Database: ${result.database}`);
      Logger.log(`Tables created: ${result.tables_created}`);
      Logger.log(`Import completed at: ${result.imported_at}`);

      return result;
    } catch (error) {
      Logger.error(`Database import failed: ${error.message}`);
      throw error;
    } finally {
      await this.disconnect();
    }
  }
}

// Validation function
function validateImportConfiguration() {
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
    Logger.log('MSSQL Database Import Tool Starting...');

    // Validate configuration
    if (!validateImportConfiguration()) {
      process.exit(1);
    }

    // Create importer instance
    const importer = new DatabaseImporter();

    // Perform import
    const result = await importer.importDatabase();

    Logger.success('Import completed successfully!');
    console.log('\nImport Summary:');
    console.log(`- Database: ${result.database}`);
    console.log(`- Schema Imported: ${result.schema_imported}`);
    console.log(`- Data Imported: ${result.data_imported}`);
    console.log(`- Tables Created: ${result.tables_created}`);
    console.log(`- Import Completed: ${result.imported_at}`);

  } catch (error) {
    Logger.error(`Import failed: ${error.message}`);
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

// Run the import if this file is executed directly
if (require.main === module) {
  main();
}

module.exports = { DatabaseImporter, Logger };