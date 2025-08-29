# MSSQL Database Export Tool

A comprehensive Node.js script to connect to Microsoft SQL Server databases, export complete schema and data, and securely manage database credentials using environment variables.

## Features

- 🔒 **Secure credential management** using environment variables
- 📊 **Complete schema export** including tables, columns, primary keys, and foreign keys
- 💾 **Full data export** with support for large tables using batch processing
- ⚡ **Optimized performance** with connection pooling and batch data retrieval
- 🛡️ **Comprehensive error handling** with detailed logging
- 📁 **Flexible export formats** (currently JSON, extensible for other formats)
- 🔧 **Configurable export options** via environment variables

## Prerequisites

- Node.js (version 14 or higher)
- Access to a Microsoft SQL Server database
- npm or yarn package manager

## Installation

1. **Clone or download the project files**

2. **Install dependencies:**
   ```bash
   npm install
   ```

3. **Configure database credentials:**
   - Copy `.env.example` to `.env`
   - Fill in your database connection details in the `.env` file

## Configuration

### Environment Variables

Create a `.env` file in the project root with the following variables:

```env
# Database Server Configuration
DB_SERVER=your-server-name
DB_PORT=1433
DB_DATABASE=your-database-name
DB_USER=your-username
DB_PASSWORD=your-password

# Connection Options
DB_ENCRYPT=false
DB_TRUST_SERVER_CERTIFICATE=true
DB_CONNECTION_TIMEOUT=30000
DB_REQUEST_TIMEOUT=30000

# Export Configuration
EXPORT_PATH=./exports
EXPORT_FORMAT=json
INCLUDE_SCHEMA=true
INCLUDE_DATA=true
```

### Configuration Details

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `DB_SERVER` | SQL Server hostname or IP | `localhost` | Yes |
| `DB_PORT` | SQL Server port | `1433` | No |
| `DB_DATABASE` | Database name to export | - | **Yes** |
| `DB_USER` | Database username | - | **Yes** |
| `DB_PASSWORD` | Database password | - | **Yes** |
| `DB_ENCRYPT` | Enable encryption | `false` | No |
| `DB_TRUST_SERVER_CERTIFICATE` | Trust server certificate | `true` | No |
| `DB_CONNECTION_TIMEOUT` | Connection timeout (ms) | `30000` | No |
| `DB_REQUEST_TIMEOUT` | Query timeout (ms) | `30000` | No |
| `EXPORT_PATH` | Output directory for exports | `./exports` | No |
| `EXPORT_FORMAT` | Export format (json) | `json` | No |
| `INCLUDE_SCHEMA` | Export database schema | `true` | No |
| `INCLUDE_DATA` | Export table data | `true` | No |

## Usage

### Basic Export

Run the complete database export:

```bash
npm start
```

Or run directly with Node.js:

```bash
node export-db.js
```

### Export Options

The script will export based on your configuration:

- **Schema Only**: Set `INCLUDE_DATA=false` to export only the database structure
- **Data Only**: Set `INCLUDE_SCHEMA=false` to export only the data
- **Full Export**: Keep both `INCLUDE_SCHEMA=true` and `INCLUDE_DATA=true` (default)

### Output Files

The export creates the following files in the `EXPORT_PATH` directory:

- **`schema.json`**: Complete database schema including:
  - Table definitions
  - Column information (name, type, constraints)
  - Primary key relationships
  - Foreign key relationships

- **`data_export_summary.json`**: Summary of exported data including:
  - List of all exported tables
  - Row counts for each table
  - File references for individual table data

- **`[schema]_[table]_data.json`**: Individual table data files containing:
  - Table name and schema
  - Row count
  - Complete table data as JSON array
  - Export timestamp

## Examples

### Example 1: Export Schema Only

```env
# .env configuration
DB_DATABASE=myapp_db
DB_USER=app_user
DB_PASSWORD=secure_password
INCLUDE_SCHEMA=true
INCLUDE_DATA=false
```

### Example 2: Export Large Database

```env
# .env configuration for large database
DB_DATABASE=bigdata_db
DB_USER=admin
DB_PASSWORD=admin_password
DB_CONNECTION_TIMEOUT=60000
DB_REQUEST_TIMEOUT=120000
EXPORT_PATH=./large_export
```

### Example 3: Azure SQL Database

```env
# .env configuration for Azure SQL
DB_SERVER=your-server.database.windows.net
DB_DATABASE=your_db
DB_USER=your_user@your-server
DB_PASSWORD=your_password
DB_ENCRYPT=true
DB_TRUST_SERVER_CERTIFICATE=false
```

## 🚨 Disaster Recovery Mode

For emergency database migrations between RDS instances:

### Quick Disaster Recovery Setup

1. **Configure disaster recovery environment:**
   ```bash
   cp .env.disaster-recovery .env
   nano .env  # Edit with your RDS credentials
   ```

2. **Run complete migration:**
   ```bash
   npm run disaster-recovery
   ```

### Disaster Recovery Features

- **RDS-to-RDS Migration**: Migrate data between AWS RDS instances
- **Automatic Validation**: Validates data integrity during migration
- **Backup Creation**: Creates backup of target before migration
- **Progress Monitoring**: Real-time migration progress and statistics
- **Error Recovery**: Handles network issues and temporary failures
- **Migration Reports**: Detailed reports of migration success/failures

### Disaster Recovery Configuration

```env
# Source Database (where data currently exists)
SOURCE_DB_SERVER=your-source-rds.amazonaws.com
SOURCE_DB_DATABASE=your_database
SOURCE_DB_USER=username
SOURCE_DB_PASSWORD=password

# Target Database (where data needs to be restored)
TARGET_DB_SERVER=your-target-rds.amazonaws.com
TARGET_DB_DATABASE=your_database
TARGET_DB_USER=username
TARGET_DB_PASSWORD=password

# Migration Settings
CREATE_BACKUP=true              # Backup target before migration
VALIDATE_DATA=true              # Validate data after migration
MAX_ROWS_PER_FILE=50000         # Chunk size for large tables
```

### Emergency Commands

```bash
# Start disaster recovery migration
npm run disaster-recovery

# Memory-optimized for large databases
node --max-old-space-size=4096 disaster-recovery.js

# Quick migration (minimal validation)
CREATE_BACKUP=false VALIDATE_DATA=false npm run migrate
```

📖 **See `DISASTER_RECOVERY_README.md`** for complete disaster recovery guide.

## Troubleshooting

### Common Issues

1. **Connection Failed**
   - Verify server name, port, and credentials
   - Check if SQL Server is running and accessible
   - Ensure firewall allows connections on port 1433

2. **Authentication Error**
   - Confirm username and password are correct
   - Check if SQL Server authentication is enabled
   - Verify user has necessary permissions

3. **Timeout Errors**
   - Increase `DB_CONNECTION_TIMEOUT` and `DB_REQUEST_TIMEOUT`
   - Check network connectivity
   - Consider exporting in smaller batches for very large tables

4. **Permission Errors**
   - Ensure the database user has SELECT permissions on all tables
   - Verify access to INFORMATION_SCHEMA views

### Debug Mode

For additional debugging information, you can modify the script to enable more verbose logging or add console.log statements as needed.

### Large Database Considerations

For databases with millions of rows:
- The script automatically batches data retrieval (10,000 rows per batch)
- Monitor memory usage during export
- Consider increasing Node.js memory limit: `node --max-old-space-size=4096 export-db.js`

## Security Best Practices

- ✅ **Never commit `.env` files** to version control
- ✅ **Use strong, unique passwords** for database accounts
- ✅ **Restrict database user permissions** to read-only when possible
- ✅ **Enable encryption** for production environments
- ✅ **Use environment-specific configuration** files

## File Structure

```
project-root/
├── export-db.js                 # Database export script
├── import-db.js                 # Database import script
├── disaster-recovery.js         # Disaster recovery migration script
├── package.json                 # Project dependencies
├── .env                         # Database credentials
├── .env.example                 # Environment variables template
├── .env.disaster-recovery       # Disaster recovery configuration template
├── exports/                     # Export output directory (created automatically)
│   ├── schema.json
│   ├── data_export_summary.json
│   └── *_data.json
├── DISASTER_RECOVERY_README.md  # Complete disaster recovery guide
└── README.md                    # This file
```

## License

ISC License - See package.json for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review the configuration examples
3. Ensure all prerequisites are met
4. Check SQL Server logs for additional error details