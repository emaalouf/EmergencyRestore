const sql = require('mssql');
const { DatabaseError, executeWithRetry, sanitizeTableName } = require('./utils');

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
    const sanitizedTable = sanitizeTableName(tableName);
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
        WHERE TABLE_NAME = '${sanitizedTable}'
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
            throw new DatabaseError(`Error creating table ${tableName}: ${error.message}`, tableName, 'CREATE_TABLE');
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

function getSqlDataType(dataType, maxLength, precision, scale) {
    switch (dataType.toLowerCase()) {
        case 'int':
        case 'integer':
            return sql.Int;
        case 'bigint':
            return sql.BigInt;
        case 'smallint':
            return sql.SmallInt;
        case 'tinyint':
            return sql.TinyInt;
        case 'bit':
            return sql.Bit;
        case 'decimal':
        case 'numeric':
            return sql.Decimal(precision || 18, scale || 0);
        case 'money':
            return sql.Money;
        case 'smallmoney':
            return sql.SmallMoney;
        case 'float':
            return sql.Float;
        case 'real':
            return sql.Real;
        case 'datetime':
            return sql.DateTime;
        case 'datetime2':
            return sql.DateTime2;
        case 'smalldatetime':
            return sql.SmallDateTime;
        case 'date':
            return sql.Date;
        case 'time':
            return sql.Time;
        case 'datetimeoffset':
            return sql.DateTimeOffset;
        case 'uniqueidentifier':
            return sql.UniqueIdentifier;
        case 'varchar':
            return maxLength === -1 ? sql.VarChar(sql.MAX) : sql.VarChar(maxLength || 255);
        case 'nvarchar':
            return maxLength === -1 ? sql.NVarChar(sql.MAX) : sql.NVarChar(maxLength || 255);
        case 'char':
            return sql.Char(maxLength || 1);
        case 'nchar':
            return sql.NChar(maxLength || 1);
        case 'text':
            return sql.Text;
        case 'ntext':
            return sql.NText;
        case 'varbinary':
            return maxLength === -1 ? sql.VarBinary(sql.MAX) : sql.VarBinary(maxLength || 255);
        case 'binary':
            return sql.Binary(maxLength || 1);
        case 'image':
            return sql.Image;
        default:
            return sql.NVarChar(sql.MAX);
    }
}

async function bulkInsertData(targetPool, table, schema, rows) {
    if (!rows || rows.length === 0) {
        return;
    }
    
    const bulkTable = new sql.Table(table);
    
    for (const col of schema) {
        const sqlType = getSqlDataType(col.DATA_TYPE, col.CHARACTER_MAXIMUM_LENGTH, col.NUMERIC_PRECISION, col.NUMERIC_SCALE);
        bulkTable.columns.add(col.COLUMN_NAME, sqlType, {
            nullable: col.IS_NULLABLE === 'YES'
        });
    }
    
    for (const row of rows) {
        bulkTable.rows.add(...schema.map(col => row[col.COLUMN_NAME]));
    }
    
    const request = new sql.Request(targetPool);
    await request.bulk(bulkTable);
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

module.exports = {
    getTableList,
    getTableSchema,
    createTableIfNotExists,
    getFunctions,
    getViews,
    getSqlDataType,
    bulkInsertData,
    clearTargetDatabase
};