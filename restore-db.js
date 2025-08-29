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
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
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
            
            if (col.CHARACTER_MAXIMUM_LENGTH && col.CHARACTER_MAXIMUM_LENGTH > 0) {
                columnDef += `(${col.CHARACTER_MAXIMUM_LENGTH})`;
            } else if (col.DATA_TYPE === 'decimal' || col.DATA_TYPE === 'numeric') {
                if (col.NUMERIC_PRECISION && col.NUMERIC_SCALE !== null) {
                    columnDef += `(${col.NUMERIC_PRECISION},${col.NUMERIC_SCALE})`;
                }
            } else if (col.DATA_TYPE === 'varchar' || col.DATA_TYPE === 'nvarchar') {
                if (col.CHARACTER_MAXIMUM_LENGTH === -1) {
                    columnDef += '(MAX)';
                } else if (!col.CHARACTER_MAXIMUM_LENGTH) {
                    columnDef += '(255)';
                }
            }
            
            if (col.IS_NULLABLE === 'NO') {
                columnDef += ' NOT NULL';
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

async function getFunctions(pool) {
    const result = await pool.request().query(`
        SELECT 
            ROUTINE_NAME,
            ROUTINE_DEFINITION
        FROM INFORMATION_SCHEMA.ROUTINES 
        WHERE ROUTINE_TYPE = 'FUNCTION'
        AND ROUTINE_SCHEMA = 'dbo'
        ORDER BY ROUTINE_NAME
    `);
    return result.recordset;
}

async function getViews(pool) {
    const result = await pool.request().query(`
        SELECT 
            TABLE_NAME as VIEW_NAME,
            VIEW_DEFINITION
        FROM INFORMATION_SCHEMA.VIEWS 
        WHERE TABLE_SCHEMA = 'dbo'
        ORDER BY TABLE_NAME
    `);
    return result.recordset;
}

async function transferFunctions(sourcePool, targetPool) {
    console.log('🔧 Transferring functions...');
    
    try {
        const functions = await getFunctions(sourcePool);
        
        for (const func of functions) {
            try {
                await targetPool.request().query(`DROP FUNCTION IF EXISTS [${func.ROUTINE_NAME}]`);
                await targetPool.request().query(func.ROUTINE_DEFINITION);
                console.log(`✅ Transferred function: ${func.ROUTINE_NAME}`);
            } catch (error) {
                console.error(`❌ Error transferring function ${func.ROUTINE_NAME}:`, error.message);
            }
        }
        
        console.log(`✅ Functions transfer completed (${functions.length} functions)`);
    } catch (error) {
        console.error('❌ Error getting functions:', error.message);
    }
}

async function transferViews(sourcePool, targetPool) {
    console.log('👁️ Transferring views...');
    
    try {
        const views = await getViews(sourcePool);
        
        for (const view of views) {
            try {
                await targetPool.request().query(`DROP VIEW IF EXISTS [${view.VIEW_NAME}]`);
                await targetPool.request().query(view.VIEW_DEFINITION);
                console.log(`✅ Transferred view: ${view.VIEW_NAME}`);
            } catch (error) {
                console.error(`❌ Error transferring view ${view.VIEW_NAME}:`, error.message);
            }
        }
        
        console.log(`✅ Views transfer completed (${views.length} views)`);
    } catch (error) {
        console.error('❌ Error getting views:', error.message);
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
                    
                    if (insertedCount % 50000 === 0) {
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
        
        await transferFunctions(sourcePool, targetPool);
        
        await transferViews(sourcePool, targetPool);
        
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