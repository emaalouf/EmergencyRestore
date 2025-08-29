require('dotenv').config();
const sql = require('mssql');

const sourceConfig = {
    server: process.env.SOURCE_DB_SERVER,
    database: process.env.SOURCE_DB_NAME,
    user: process.env.SOURCE_DB_USER,
    password: process.env.SOURCE_DB_PASSWORD,
    options: {
        encrypt: true,
        trustServerCertificate: true,
        enableArithAbort: true
    },
    pool: {
        min: parseInt(process.env.DB_POOL_MIN) || 0,
        max: parseInt(process.env.DB_POOL_MAX) || 10
    },
    requestTimeout: parseInt(process.env.DB_REQUEST_TIMEOUT) || 30000,
    connectionTimeout: parseInt(process.env.DB_CONNECTION_TIMEOUT) || 30000
};

const targetConfig = {
    server: process.env.TARGET_DB_SERVER,
    database: process.env.TARGET_DB_NAME,
    user: process.env.TARGET_DB_USER,
    password: process.env.TARGET_DB_PASSWORD,
    options: {
        encrypt: true,
        trustServerCertificate: true,
        enableArithAbort: true
    },
    pool: {
        min: parseInt(process.env.DB_POOL_MIN) || 0,
        max: parseInt(process.env.DB_POOL_MAX) || 10
    },
    requestTimeout: parseInt(process.env.DB_REQUEST_TIMEOUT) || 30000,
    connectionTimeout: parseInt(process.env.DB_CONNECTION_TIMEOUT) || 30000
};

async function getTableList(pool) {
    const result = await pool.request().query(`
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_TYPE = 'BASE TABLE' 
        AND TABLE_SCHEMA = 'dbo'
        ORDER BY TABLE_NAME
    `);
    return result.recordset.map(row => row.TABLE_NAME);
}

async function getTableSchema(pool, tableName) {
    const result = await pool.request().query(`
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            IS_NULLABLE,
            COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '${tableName}'
        AND TABLE_SCHEMA = 'dbo'
        ORDER BY ORDINAL_POSITION
    `);
    return result.recordset;
}

async function createTableIfNotExists(targetPool, tableName, schema) {
    try {
        const columns = schema.map(col => {
            let columnDef = `[${col.COLUMN_NAME}] ${col.DATA_TYPE}`;
            
            if (col.CHARACTER_MAXIMUM_LENGTH) {
                columnDef += `(${col.CHARACTER_MAXIMUM_LENGTH})`;
            }
            
            if (col.IS_NULLABLE === 'NO') {
                columnDef += ' NOT NULL';
            }
            
            if (col.COLUMN_DEFAULT) {
                columnDef += ` DEFAULT ${col.COLUMN_DEFAULT}`;
            }
            
            return columnDef;
        }).join(', ');
        
        const createTableQuery = `CREATE TABLE [${tableName}] (${columns})`;
        await targetPool.request().query(createTableQuery);
        console.log(`✅ Created table: ${tableName}`);
    } catch (error) {
        if (error.message.includes('already an object')) {
            console.log(`ℹ️  Table ${tableName} already exists`);
        } else {
            console.error(`❌ Error creating table ${tableName}:`, error.message);
            throw error;
        }
    }
}

async function clearTargetDatabase(targetPool) {
    console.log('🧹 Clearing target database...');
    
    const tables = await getTableList(targetPool);
    
    for (const table of tables) {
        try {
            await targetPool.request().query(`DELETE FROM [${table}]`);
            console.log(`✅ Cleared table: ${table}`);
        } catch (error) {
            console.error(`❌ Error clearing table ${table}:`, error.message);
        }
    }
    
    console.log('✅ Target database cleared successfully');
}

async function transferData(sourcePool, targetPool) {
    console.log('📊 Starting data transfer...');
    
    const tables = await getTableList(sourcePool);
    
    for (const table of tables) {
        try {
            console.log(`🔄 Transferring table: ${table}`);
            
            const schema = await getTableSchema(sourcePool, table);
            await createTableIfNotExists(targetPool, table, schema);
            
            const sourceData = await sourcePool.request().query(`SELECT * FROM [${table}]`);
            
            if (sourceData.recordset.length === 0) {
                console.log(`ℹ️  Table ${table} is empty, skipping...`);
                continue;
            }
            
            const columns = Object.keys(sourceData.recordset[0]);
            const columnList = columns.map(col => `[${col}]`).join(', ');
            const valuesList = columns.map(col => `@${col}`).join(', ');
            
            const insertQuery = `INSERT INTO [${table}] (${columnList}) VALUES (${valuesList})`;
            
            let insertedCount = 0;
            
            for (const row of sourceData.recordset) {
                try {
                    const request = targetPool.request();
                    
                    for (const column of columns) {
                        request.input(column, row[column]);
                    }
                    
                    await request.query(insertQuery);
                    insertedCount++;
                    
                    if (insertedCount % 100 === 0) {
                        console.log(`   📈 Inserted ${insertedCount}/${sourceData.recordset.length} rows`);
                    }
                } catch (error) {
                    console.error(`❌ Error inserting row in table ${table}:`, error.message);
                }
            }
            
            console.log(`✅ Transferred ${insertedCount}/${sourceData.recordset.length} rows for table: ${table}`);
            
        } catch (error) {
            console.error(`❌ Error transferring table ${table}:`, error.message);
        }
    }
    
    console.log('✅ Data transfer completed');
}

async function restoreDatabase() {
    let sourcePool, targetPool;
    
    try {
        console.log('🔌 Connecting to source database...');
        sourcePool = await sql.connect(sourceConfig);
        console.log('✅ Connected to source database');
        
        console.log('🔌 Connecting to target database...');
        targetPool = new sql.ConnectionPool(targetConfig);
        await targetPool.connect();
        console.log('✅ Connected to target database');
        
        await clearTargetDatabase(targetPool);
        
        await transferData(sourcePool, targetPool);
        
        console.log('🎉 Database restore completed successfully!');
        
    } catch (error) {
        console.error('💥 Database restore failed:', error.message);
        process.exit(1);
    } finally {
        if (sourcePool) {
            await sourcePool.close();
            console.log('🔌 Source database connection closed');
        }
        if (targetPool) {
            await targetPool.close();
            console.log('🔌 Target database connection closed');
        }
    }
}

if (require.main === module) {
    restoreDatabase();
}

module.exports = { restoreDatabase };