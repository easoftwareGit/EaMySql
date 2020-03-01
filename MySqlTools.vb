Imports MySql.Data.MySqlClient
Imports EaSql.Ea
Imports EaTools.Ea
Imports EaTools.DataTools

Public Class MySqlTools

#Region " single quotes, double quotes and back ticks "

    ' https://stackoverflow.com/questions/11321491/when-to-use-single-quotes-double-quotes-and-back-ticks-in-mysql
    ' 
    ' Back ticks are to be used for table and column identifiers, but are only necessary when the identifier is a MySQL reserved keyword, 
    ' or when the identifier contains whitespace characters or characters beyond a limited set (see below) It is often recommended to avoid 
    ' using reserved keywords as column or table identifiers when possible, avoiding the quoting issue.
    '
    ' Single quotes should be used for string values like in the VALUES() list. Double quotes are supported by MySQL for string values as well, 
    ' but single quotes are more widely accepted by other RDBMS, so it is a good habit to use single quotes instead of double.
    '
    ' MySQL also expects DATE and DATETIME literal values to be single-quoted as strings like '2001-01-01 00:00:00'. Consult the Date and Time 
    ' Literals documentation for more details, in particular alternatives to using the hyphen - as a segment delimiter in date strings.
    '
    ' Functions native to the RDBMS (for example, NOW() in MySQL) should not be quoted, although their arguments are subject to the same string 
    ' or identifier quoting rules already mentioned.
    '
    ' Back tick (`)
    ' table & column ───────┬─────┬──┬──┬──┬────┬──┬────┬──┬────┬──┬───────┐
    '                       ↓     ↓  ↓  ↓  ↓    ↓  ↓    ↓  ↓    ↓  ↓       ↓
    ' $query = "INSERT INTO `table` (`id`, `col1`, `col2`, `date`, `updated`) 
    '                        VALUES (NULL, 'val1', 'val2', '2001-01-01', NOW())";
    '                                ↑↑↑↑  ↑    ↑  ↑    ↑  ↑          ↑  ↑↑↑↑↑ 
    ' Unquoted keyword          ─────┴┴┴┘  │    │  │    │  │          │  │││││
    ' Single-quoted (') strings ───────────┴────┴──┴────┘  │          │  │││││
    ' Single-quoted (') DATE    ───────────────────────────┴──────────┘  │││││
    ' Unquoted function         ─────────────────────────────────────────┴┴┴┴┘    

#End Region

#Region " Class Constants, Enums and Variables "

#Region " Constants "

    Public Const cnCharMaxLen As String = "CHARACTER_MAXIMUM_LENGTH"
    Public Const cnCollation As String = "COLLATION"
    Public Const cnColumnName As String = "COLUMN_NAME"
    Public Const cnDataType As String = "DATA_TYPE"
    Public Const cnColDefault As String = "COLUMN_DEFAULT"
    Public Const cnIndexName As String = "INDEX_NAME"
    Public Const cnIsNullable As String = "IS_NULLABLE"
    Public Const cnNulls As String = "NULLS"
    Public Const cnNumPrecision As String = "NUMERIC_PRECISION"
    Public Const cnNumScale As String = "NUMERIC_SCALE"
    Public Const cnOrdinalPos As String = "ORDINAL_POSITION"
    Public Const cnParamMode As String = "PARAMETER_MODE"
    Public Const cnParamName As String = "PARAMETER_NAME"
    Public Const cnPrimaryKey As String = "PRIMARY_KEY"
    Public Const cnProcedureDef As String = "PROCEDURE_DEFINITION"
    Public Const cnProcedureName As String = "PROCEDURE_NAME"
    Public Const cnQueryDef As String = "QUERY_DEFINITION"
    Public Const cnQueryOrder As String = "QUERY_ORDER"
    Public Const cnQueryName As String = "QUERY_NAME"
    Public Const cnTableName As String = "TABLE_NAME"
    Public Const cnViewDef As String = "VIEW_DEFINITION"
    Public Const cnUnique As String = "UNIQUE"

    Public Const errAlreadyInUse As Integer = -2146825243
    Public Const errCannotOpen As Integer = -2147467259
    Public Const errCannotModify As Integer = -2147217911
    Public Const errNoColumn As Integer = -2147217904
    Public Const errNoTable As Integer = -2147217865
    Public Const errNoView As Integer = -2147467259


    'Private Const cnColHasDefault As String = "COLUMN_HASDEFAULT"
    'Private Const cnColFlags As String = "COLUMN_FLAGS"
    'Private Const cnCharOctLen As String = "CHARACTER_OCTET_LENGTH"
    'Private Const cnDateTimePrecision As String = "DATETIME_PRECISION"
    'Private Const cnNumPrecision As String = "NUMERIC_PRECISION"

    Public Const backTick As String = "`"

    Private Const DebugFormatFind As String = "No DoEvents; {0}, TryCount: {1}, Find: {2} as a {3}"
    Private Const DebugFormatStandard As String = "No DoEvents; {0}, TryCount: {1}, Item: {2}"

    Private Const efClosing As String = "Error closing connection. {0}, Server: {1}, Database: {2}"
    Private Const efCode As String = "Error Code: {0}, {1} Data Source: {2}"
    Private Const efMySqlExp As String = "Error Code: {0} {1}, Server: {2}, Database: {3}"
    Private Const efOpenConn As String = "Could not open connection: ""{0}"""
    Private Const efOther As String = "Other error: {0}"

    Private Const emInvalidVersion As String = "Invalid MySql Version"
    Private Const emGotDatabaseParam As String = "Database parameter in connection string"
    Private Const emNoVersion As String = "No MySql Version"
    Private Const emNoDatabaseParam As String = "No database parameter in connection string"

    Public Const dfShortDate As String = "yyyy-MM-dd"
    Public Const dfLongDate As String = dfShortDate & " H:m:s"
    Public Const dfReallyLongDate As String = dfLongDate & ".fffffff"

    'Private Const errAlreadyInUse As Integer = -2146825243
    'Private Const errCannotOpen As Integer = -2147467259
    'Private Const errCannotModify As Integer = -2147217911
    'Private Const errNoColumn As Integer = -2147217904
    'Private Const errNoTable As Integer = -2147217865
    'Private Const errNoView As Integer = -2147467259

    Private Const MaxTries As Integer = 10
    Private Const WaitMilliSeconds As Integer = 1000

    Private Const ceGeneralError As Integer = 0
    Private Const ceCannotConnectToToServer As Integer = 1042
    Private Const ceInvalidUsernamePassword As Integer = 1045
    Private Const ceDatabaseNotFound As Integer = 1049

    Public Const TimeStampValue As String = "CURRENT_TIMESTAMP"
    Public Const NativePassword As String = "mysql_native_password"

    Public Const sqlErr As Integer = -500
    Public Const sqlNoConnection As Integer = -501
    Public Const sqlCouldNotOpen As Integer = -502
    Public Const sqlCouldNotClose As Integer = -503
    Public Const sqlInvalidConnStr As Integer = -504
    Public Const sqlNoConvertToStr As Integer = -505
    Public Const sqlDeleteErr As Integer = -506
    Public Const sqlDropErr As Integer = -507
    Public Const sqlCreateErr As Integer = -508
    Public Const sqlAlterErr As Integer = -509
    Public Const sqlInvalidAlter As Integer = -510
    Public Const sqlNoDatabase As Integer = -511
    Public Const sqlNoTableName As Integer = -512
    Public Const sqlNoColumnName As Integer = -513
    Public Const sqlNoKeyName As Integer = -514
    Public Const sqlNoIndexName As Integer = -515
    Public Const sqlNoColumn As Integer = -516
    Public Const sqlNoUser As Integer = -517
    Public Const sqlInsertErr As Integer = -518
    Public Const sqlUpdateErr As Integer = -519
    Public Const sqlMakeTableErr As Integer = -520
    Public Const sqlRenameTableErr As Integer = -521
    Public Const sqlCreateIndexErr As Integer = -522
    Public Const sqlRenameColumnErr As Integer = -523
    Public Const sqlNoCommandText As Integer = -524
    Public Const sqlNoQueryName As Integer = -525
    Public Const sqlNoQueryType As Integer = -526
    Public Const sqlNoProcName As Integer = -527
    Public Const sqlNoViewName As Integer = -528
    Public Const sqlTableNotFound As Integer = -529
    Public Const sqlCreatePrimaryKey As Integer = -530
    Public Const sqlKeyWord As Integer = -531
    Public Const sqlEmptyTableErr As Integer = -532
    Public Const sqlNoSort As Integer = -533
    Public Const sqlInvalidSort As Integer = -534
    Public Const sqlAlreadyExists As Integer = -535
    Public Const sqlVersionErr As Integer = -536


    Public Const sqlCannotConnectToServer As Integer = -550
    Public Const sqlInvalidUserNamePassword As Integer = -551

    Public Const sqlCouldNotCreateDbAdapter As Integer = -560
    Public Const sqlCouldNotCreateCommandBuilder As Integer = -561
    Public Const sqlCouldNotCreateSelectCommand As Integer = -562
    Public Const sqlCouldNotCreateDeleteCommand As Integer = -563
    Public Const sqlCouldNotCreateInsertCommand As Integer = -564
    Public Const sqlCouldNotCreateUpdateCommand As Integer = -565

    Public Const sqlOtherErr As Integer = -599

#End Region

#Region " ENums "

    Enum ExistsTypes
        GotError = -3
        OverMaxTries = -2
        Yes = -1
        No = 0
    End Enum

    Enum AlterTableTypes
        atNone = 0
        atAddColumn = 1
        atAlterColumn = 2
        atDropColumn = 3
        atAddKey = 4
        atDropKey = 5
    End Enum

    Enum PreserveChangesTypes
        None = -1
        No = 0
        Yes = 1
    End Enum

    Enum FixedConcurExTypes
        CouldNotFix = -1
        NeedToUpdate = 0
        NeedToRefresh = 1
    End Enum

#End Region

#Region " Private Vars "

    Private Shared _connectionError As Integer = NoErrors
    Private Shared _connectionErrMsg As String = String.Empty
    Private Shared _customConnErrMsg As String = String.Empty

    Private Shared _errorCode As Integer = 0
    Private Shared _errorMessage As String = String.Empty

    Private Shared _mySqlVer As MySqlVersion = Nothing

#End Region

#End Region

#Region " KeyWords Array "

    Shared ReadOnly KeyWords As String() = {”ACCESSIBLE”,
                                                ”ACCOUNT”,
                                                ”ACTION”,
                                                ”ACTIVE”,
                                                ”ADD”,
                                                ”ADMIN”,
                                                ”AFTER”,
                                                ”AGAINST”,
                                                ”AGGREGATE”,
                                                ”ALGORITHM”,
                                                ”ALL”,
                                                ”ALTER”,
                                                ”ALWAYS”,
                                                ”ANALYSE”,
                                                ”ANALYZE”,
                                                ”AND”,
                                                ”ANY”,
                                                ”AS”,
                                                ”ASC”,
                                                ”ASCII”,
                                                ”ASENSITIVE”,
                                                ”AT”,
                                                ”AUTOEXTEND_SIZE”,
                                                ”AUTO_INCREMENT”,
                                                ”AVG”,
                                                ”AVG_ROW_LENGTH”,
                                                ”BACKUP”,
                                                ”BEFORE”,
                                                ”BEGIN”,
                                                ”BETWEEN”,
                                                ”BIGINT”,
                                                ”BINARY”,
                                                ”BINLOG”,
                                                ”BIT”,
                                                ”BLOB”,
                                                ”BLOCK”,
                                                ”BOOL”,
                                                ”BOOLEAN”,
                                                ”BOTH”,
                                                ”BTREE”,
                                                ”BUCKETS”,
                                                ”BY”,
                                                ”BYTE”,
                                                ”CACHE”,
                                                ”CALL”,
                                                ”CASCADE”,
                                                ”CASCADED”,
                                                ”CASE”,
                                                ”CATALOG_NAME”,
                                                ”CHAIN”,
                                                ”CHANGE”,
                                                ”CHANGED”,
                                                ”CHANNEL”,
                                                ”CHAR”,
                                                ”CHARACTER”,
                                                ”CHARSET”,
                                                ”CHECK”,
                                                ”CHECKSUM”,
                                                ”CIPHER”,
                                                ”CLASS_ORIGIN”,
                                                ”CLIENT”,
                                                ”CLONE”,
                                                ”CLOSE”,
                                                ”COALESCE”,
                                                ”CODE”,
                                                ”COLLATE”,
                                                ”COLLATION”,
                                                ”COLUMN”,
                                                ”COLUMNS”,
                                                ”COLUMN_FORMAT”,
                                                ”COLUMN_NAME”,
                                                ”COMMENT”,
                                                ”COMMIT”,
                                                ”COMMITTED”,
                                                ”COMPACT”,
                                                ”COMPLETION”,
                                                ”COMPONENT”,
                                                ”COMPRESSED”,
                                                ”COMPRESSION”,
                                                ”CONCURRENT”,
                                                ”CONDITION”,
                                                ”CONNECTION”,
                                                ”CONSISTENT”,
                                                ”CONSTRAINT”,
                                                ”CONSTRAINT_CATALOG”,
                                                ”CONSTRAINT_NAME”,
                                                ”CONSTRAINT_SCHEMA”,
                                                ”CONTAINS”,
                                                ”CONTEXT”,
                                                ”CONTINUE”,
                                                ”CONVERT”,
                                                ”CPU”,
                                                ”CREATE”,
                                                ”CROSS”,
                                                ”CUBE”,
                                                ”CUME_DIST”,
                                                ”CURRENT”,
                                                ”CURRENT_DATE”,
                                                ”CURRENT_TIME”,
                                                ”CURRENT_TIMESTAMP”,
                                                ”CURRENT_USER”,
                                                ”CURSOR”,
                                                ”CURSOR_NAME”,
                                                ”DATA”,
                                                ”DATABASE”,
                                                ”DATABASES”,
                                                ”DATAFILE”,
                                                ”DATE”,
                                                ”DATETIME”,
                                                ”DAY”,
                                                ”DAY_HOUR”,
                                                ”DAY_MICROSECOND”,
                                                ”DAY_MINUTE”,
                                                ”DAY_SECOND”,
                                                ”DEALLOCATE”,
                                                ”DEC”,
                                                ”DECIMAL”,
                                                ”DECLARE”,
                                                ”DEFAULT”,
                                                ”DEFAULT_AUTH”,
                                                ”DEFINER”,
                                                ”DEFINITION”,
                                                ”DELAYED”,
                                                ”DELAY_KEY_WRITE”,
                                                ”DELETE”,
                                                ”DENSE_RANK”,
                                                ”DESC”,
                                                ”DESCRIBE”,
                                                ”DESCRIPTION”,
                                                ”DES_KEY_FILE”,
                                                ”DETERMINISTIC”,
                                                ”DIAGNOSTICS”,
                                                ”DIRECTORY”,
                                                ”DISABLE”,
                                                ”DISCARD”,
                                                ”DISK”,
                                                ”DISTINCT”,
                                                ”DISTINCTROW”,
                                                ”DIV”,
                                                ”DO”,
                                                ”DOUBLE”,
                                                ”DROP”,
                                                ”DUAL”,
                                                ”DUMPFILE”,
                                                ”DUPLICATE”,
                                                ”DYNAMIC”,
                                                ”EACH”,
                                                ”ELSE”,
                                                ”ELSEIF”,
                                                ”EMPTY”,
                                                ”ENABLE”,
                                                ”ENCLOSED”,
                                                ”ENCRYPTION”,
                                                ”END”,
                                                ”ENDS”,
                                                ”ENGINE”,
                                                ”ENGINES”,
                                                ”ENUM”,
                                                ”ERROR”,
                                                ”ERRORS”,
                                                ”ESCAPE”,
                                                ”ESCAPED”,
                                                ”EVENT”,
                                                ”EVENTS”,
                                                ”EVERY”,
                                                ”EXCEPT”,
                                                ”EXCHANGE”,
                                                ”EXCLUDE”,
                                                ”EXECUTE”,
                                                ”EXISTS”,
                                                ”EXIT”,
                                                ”EXPANSION”,
                                                ”EXPIRE”,
                                                ”EXPLAIN”,
                                                ”EXPORT”,
                                                ”EXTENDED”,
                                                ”EXTENT_SIZE”,
                                                ”FALSE”,
                                                ”FAST”,
                                                ”FAULTS”,
                                                ”FETCH”,
                                                ”FIELDS”,
                                                ”FILE”,
                                                ”FILE_BLOCK_SIZE”,
                                                ”FILTER”,
                                                ”FIRST”,
                                                ”FIRST_VALUE”,
                                                ”FIXED”,
                                                ”FLOAT”,
                                                ”FLOAT4”,
                                                ”FLOAT8”,
                                                ”FLUSH”,
                                                ”FOLLOWING”,
                                                ”FOLLOWS”,
                                                ”FOR”,
                                                ”FORCE”,
                                                ”FOREIGN”,
                                                ”FORMAT”,
                                                ”FOUND”,
                                                ”FROM”,
                                                ”FULL”,
                                                ”FULLTEXT”,
                                                ”FUNCTION”,
                                                ”GENERAL”,
                                                ”GENERATED”,
                                                ”GEOMCOLLECTION”,
                                                ”GEOMETRY”,
                                                ”GEOMETRYCOLLECTION”,
                                                ”GET”,
                                                ”GET_FORMAT”,
                                                ”GET_MASTER_PUBLIC_KEY”,
                                                ”GLOBAL”,
                                                ”GRANT”,
                                                ”GRANTS”,
                                                ”GROUP”,
                                                ”GROUPING”,
                                                ”GROUPS”,
                                                ”GROUP_REPLICATION”,
                                                ”HANDLER”,
                                                ”HASH”,
                                                ”HAVING”,
                                                ”HELP”,
                                                ”HIGH_PRIORITY”,
                                                ”HISTOGRAM”,
                                                ”HISTORY”,
                                                ”HOST”,
                                                ”HOSTS”,
                                                ”HOUR”,
                                                ”HOUR_MICROSECOND”,
                                                ”HOUR_MINUTE”,
                                                ”HOUR_SECOND”,
                                                ”IDENTIFIED”,
                                                ”IF”,
                                                ”IGNORE”,
                                                ”IGNORE_SERVER_IDS”,
                                                ”IMPORT”,
                                                ”IN”,
                                                ”INACTIVE”,
                                                ”INDEX”,
                                                ”INDEXES”,
                                                ”INFILE”,
                                                ”INITIAL_SIZE”,
                                                ”INNER”,
                                                ”INOUT”,
                                                ”INSENSITIVE”,
                                                ”INSERT”,
                                                ”INSERT_METHOD”,
                                                ”INSTALL”,
                                                ”INSTANCE”,
                                                ”INT”,
                                                ”INT1”,
                                                ”INT2”,
                                                ”INT3”,
                                                ”INT4”,
                                                ”INT8”,
                                                ”INTEGER”,
                                                ”INTERVAL”,
                                                ”INTO”,
                                                ”INVISIBLE”,
                                                ”INVOKER”,
                                                ”IO”,
                                                ”IO_AFTER_GTIDS”,
                                                ”IO_BEFORE_GTIDS”,
                                                ”IO_THREAD”,
                                                ”IPC”,
                                                ”IS”,
                                                ”ISOLATION”,
                                                ”ISSUER”,
                                                ”ITERATE”,
                                                ”JOIN”,
                                                ”JSON”,
                                                ”JSON_TABLE”,
                                                ”KEY”,
                                                ”KEYS”,
                                                ”KEY_BLOCK_SIZE”,
                                                ”KILL”,
                                                ”LAG”,
                                                ”LANGUAGE”,
                                                ”LAST”,
                                                ”LAST_VALUE”,
                                                ”LEAD”,
                                                ”LEADING”,
                                                ”LEAVE”,
                                                ”LEAVES”,
                                                ”LEFT”,
                                                ”LESS”,
                                                ”LEVEL”,
                                                ”LIKE”,
                                                ”LIMIT”,
                                                ”LINEAR”,
                                                ”LINES”,
                                                ”LINESTRING”,
                                                ”LIST”,
                                                ”LOAD”,
                                                ”LOCAL”,
                                                ”LOCALTIME”,
                                                ”LOCALTIMESTAMP”,
                                                ”LOCK”,
                                                ”LOCKED”,
                                                ”LOCKS”,
                                                ”LOGFILE”,
                                                ”LOGS”,
                                                ”LONG”,
                                                ”LONGBLOB”,
                                                ”LONGTEXT”,
                                                ”LOOP”,
                                                ”LOW_PRIORITY”,
                                                ”MASTER”,
                                                ”MASTER_AUTO_POSITION”,
                                                ”MASTER_BIND”,
                                                ”MASTER_CONNECT_RETRY”,
                                                ”MASTER_DELAY”,
                                                ”MASTER_HEARTBEAT_PERIOD”,
                                                ”MASTER_HOST”,
                                                ”MASTER_LOG_FILE”,
                                                ”MASTER_LOG_POS”,
                                                ”MASTER_PASSWORD”,
                                                ”MASTER_PORT”,
                                                ”MASTER_PUBLIC_KEY_PATH”,
                                                ”MASTER_RETRY_COUNT”,
                                                ”MASTER_SERVER_ID”,
                                                ”MASTER_SSL”,
                                                ”MASTER_SSL_CA”,
                                                ”MASTER_SSL_CAPATH”,
                                                ”MASTER_SSL_CERT”,
                                                ”MASTER_SSL_CIPHER”,
                                                ”MASTER_SSL_CRL”,
                                                ”MASTER_SSL_CRLPATH”,
                                                ”MASTER_SSL_KEY”,
                                                ”MASTER_SSL_VERIFY_SERVER_CERT”,
                                                ”MASTER_TLS_VERSION”,
                                                ”MASTER_USER”,
                                                ”MATCH”,
                                                ”MAXVALUE”,
                                                ”MAX_CONNECTIONS_PER_HOUR”,
                                                ”MAX_QUERIES_PER_HOUR”,
                                                ”MAX_ROWS”,
                                                ”MAX_SIZE”,
                                                ”MAX_UPDATES_PER_HOUR”,
                                                ”MAX_USER_CONNECTIONS”,
                                                ”MEDIUM”,
                                                ”MEDIUMBLOB”,
                                                ”MEDIUMINT”,
                                                ”MEDIUMTEXT”,
                                                ”MEMORY”,
                                                ”MERGE”,
                                                ”MESSAGE_TEXT”,
                                                ”MICROSECOND”,
                                                ”MIDDLEINT”,
                                                ”MIGRATE”,
                                                ”MINUTE”,
                                                ”MINUTE_MICROSECOND”,
                                                ”MINUTE_SECOND”,
                                                ”MIN_ROWS”,
                                                ”MOD”,
                                                ”MODE”,
                                                ”MODIFIES”,
                                                ”MODIFY”,
                                                ”MONTH”,
                                                ”MULTILINESTRING”,
                                                ”MULTIPOINT”,
                                                ”MULTIPOLYGON”,
                                                ”MUTEX”,
                                                ”MYSQL_ERRNO”,
                                                ”NAME”,
                                                ”NAMES”,
                                                ”NATIONAL”,
                                                ”NATURAL”,
                                                ”NCHAR”,
                                                ”NDB”,
                                                ”NDBCLUSTER”,
                                                ”NESTED”,
                                                ”NEVER”,
                                                ”NEW”,
                                                ”NEXT”,
                                                ”NO”,
                                                ”NODEGROUP”,
                                                ”NONE”,
                                                ”NOT”,
                                                ”NOWAIT”,
                                                ”NO_WAIT”,
                                                ”NO_WRITE_TO_BINLOG”,
                                                ”NTH_VALUE”,
                                                ”NTILE”,
                                                ”NULL”,
                                                ”NULLS”,
                                                ”NUMBER”,
                                                ”NUMERIC”,
                                                ”NVARCHAR”,
                                                ”OF”,
                                                ”OFFSET”,
                                                ”ON”,
                                                ”ONE”,
                                                ”ONLY”,
                                                ”OPEN”,
                                                ”OPTIMIZE”,
                                                ”OPTIMIZER_COSTS”,
                                                ”OPTION”,
                                                ”OPTIONAL”,
                                                ”OPTIONALLY”,
                                                ”OPTIONS”,
                                                ”OR”,
                                                ”ORDER”,
                                                ”ORDINALITY”,
                                                ”ORGANIZATION”,
                                                ”OTHERS”,
                                                ”OUT”,
                                                ”OUTER”,
                                                ”OUTFILE”,
                                                ”OVER”,
                                                ”OWNER”,
                                                ”PACK_KEYS”,
                                                ”PAGE”,
                                                ”PARSER”,
                                                ”PARTIAL”,
                                                ”PARTITION”,
                                                ”PARTITIONING”,
                                                ”PARTITIONS”,
                                                ”PASSWORD”,
                                                ”PATH”,
                                                ”PERCENT_RANK”,
                                                ”PERSIST”,
                                                ”PERSIST_ONLY”,
                                                ”PHASE”,
                                                ”PLUGIN”,
                                                ”PLUGINS”,
                                                ”PLUGIN_DIR”,
                                                ”POINT”,
                                                ”POLYGON”,
                                                ”PORT”,
                                                ”PRECEDES”,
                                                ”PRECEDING”,
                                                ”PRECISION”,
                                                ”PREPARE”,
                                                ”PRESERVE”,
                                                ”PREV”,
                                                ”PRIMARY”,
                                                ”PRIVILEGES”,
                                                ”PROCEDURE”,
                                                ”PROCESS”,
                                                ”PROCESSLIST”,
                                                ”PROFILE”,
                                                ”PROFILES”,
                                                ”PROXY”,
                                                ”PURGE”,
                                                ”QUARTER”,
                                                ”QUERY”,
                                                ”QUICK”,
                                                ”RANGE”,
                                                ”RANK”,
                                                ”READ”,
                                                ”READS”,
                                                ”READ_ONLY”,
                                                ”READ_WRITE”,
                                                ”REAL”,
                                                ”REBUILD”,
                                                ”RECOVER”,
                                                ”RECURSIVE”,
                                                ”REDOFILE”,
                                                ”REDO_BUFFER_SIZE”,
                                                ”REDUNDANT”,
                                                ”REFERENCE”,
                                                ”REFERENCES”,
                                                ”REGEXP”,
                                                ”RELAY”,
                                                ”RELAYLOG”,
                                                ”RELAY_LOG_FILE”,
                                                ”RELAY_LOG_POS”,
                                                ”RELAY_THREAD”,
                                                ”RELEASE”,
                                                ”RELOAD”,
                                                ”REMOTE”,
                                                ”REMOVE”,
                                                ”RENAME”,
                                                ”REORGANIZE”,
                                                ”REPAIR”,
                                                ”REPEAT”,
                                                ”REPEATABLE”,
                                                ”REPLACE”,
                                                ”REPLICATE_DO_DB”,
                                                ”REPLICATE_DO_TABLE”,
                                                ”REPLICATE_IGNORE_DB”,
                                                ”REPLICATE_IGNORE_TABLE”,
                                                ”REPLICATE_REWRITE_DB”,
                                                ”REPLICATE_WILD_DO_TABLE”,
                                                ”REPLICATE_WILD_IGNORE_TABLE”,
                                                ”REPLICATION”,
                                                ”REQUIRE”,
                                                ”RESET”,
                                                ”RESIGNAL”,
                                                ”RESOURCE”,
                                                ”RESPECT”,
                                                ”RESTART”,
                                                ”RESTORE”,
                                                ”RESTRICT”,
                                                ”RESUME”,
                                                ”RETURN”,
                                                ”RETURNED_SQLSTATE”,
                                                ”RETURNS”,
                                                ”REUSE”,
                                                ”REVERSE”,
                                                ”REVOKE”,
                                                ”RIGHT”,
                                                ”RLIKE”,
                                                ”ROLE”,
                                                ”ROLLBACK”,
                                                ”ROLLUP”,
                                                ”ROTATE”,
                                                ”ROUTINE”,
                                                ”ROW”,
                                                ”ROWS”,
                                                ”ROW_COUNT”,
                                                ”ROW_FORMAT”,
                                                ”ROW_NUMBER”,
                                                ”RTREE”,
                                                ”SAVEPOINT”,
                                                ”SCHEDULE”,
                                                ”SCHEMA”,
                                                ”SCHEMAS”,
                                                ”SCHEMA_NAME”,
                                                ”SECOND”,
                                                ”SECONDARY_ENGINE”,
                                                ”SECONDARY_LOAD”,
                                                ”SECONDARY_UNLOAD”,
                                                ”SECOND_MICROSECOND”,
                                                ”SECURITY”,
                                                ”SELECT”,
                                                ”SENSITIVE”,
                                                ”SEPARATOR”,
                                                ”SERIAL”,
                                                ”SERIALIZABLE”,
                                                ”SERVER”,
                                                ”SESSION”,
                                                ”SET”,
                                                ”SHARE”,
                                                ”SHOW”,
                                                ”SHUTDOWN”,
                                                ”SIGNAL”,
                                                ”SIGNED”,
                                                ”SIMPLE”,
                                                ”SKIP”,
                                                ”SLAVE”,
                                                ”SLOW”,
                                                ”SMALLINT”,
                                                ”SNAPSHOT”,
                                                ”SOCKET”,
                                                ”SOME”,
                                                ”SONAME”,
                                                ”SOUNDS”,
                                                ”SOURCE”,
                                                ”SPATIAL”,
                                                ”SPECIFIC”,
                                                ”SQL”,
                                                ”SQLEXCEPTION”,
                                                ”SQLSTATE”,
                                                ”SQLWARNING”,
                                                ”SQL_AFTER_GTIDS”,
                                                ”SQL_AFTER_MTS_GAPS”,
                                                ”SQL_BEFORE_GTIDS”,
                                                ”SQL_BIG_RESULT”,
                                                ”SQL_BUFFER_RESULT”,
                                                ”SQL_CACHE”,
                                                ”SQL_CALC_FOUND_ROWS”,
                                                ”SQL_NO_CACHE”,
                                                ”SQL_SMALL_RESULT”,
                                                ”SQL_THREAD”,
                                                ”SQL_TSI_DAY”,
                                                ”SQL_TSI_HOUR”,
                                                ”SQL_TSI_MINUTE”,
                                                ”SQL_TSI_MONTH”,
                                                ”SQL_TSI_QUARTER”,
                                                ”SQL_TSI_SECOND”,
                                                ”SQL_TSI_WEEK”,
                                                ”SQL_TSI_YEAR”,
                                                ”SRID”,
                                                ”SSL”,
                                                ”STACKED”,
                                                ”START”,
                                                ”STARTING”,
                                                ”STARTS”,
                                                ”STATS_AUTO_RECALC”,
                                                ”STATS_PERSISTENT”,
                                                ”STATS_SAMPLE_PAGES”,
                                                ”STATUS”,
                                                ”STOP”,
                                                ”STORAGE”,
                                                ”STORED”,
                                                ”STRAIGHT_JOIN”,
                                                ”STRING”,
                                                ”SUBCLASS_ORIGIN”,
                                                ”SUBJECT”,
                                                ”SUBPARTITION”,
                                                ”SUBPARTITIONS”,
                                                ”SUPER”,
                                                ”SUSPEND”,
                                                ”SWAPS”,
                                                ”SWITCHES”,
                                                ”SYSTEM”,
                                                ”TABLE”,
                                                ”TABLES”,
                                                ”TABLESPACE”,
                                                ”TABLE_CHECKSUM”,
                                                ”TABLE_NAME”,
                                                ”TEMPORARY”,
                                                ”TEMPTABLE”,
                                                ”TERMINATED”,
                                                ”TEXT”,
                                                ”THAN”,
                                                ”THEN”,
                                                ”THREAD_PRIORITY”,
                                                ”TIES”,
                                                ”TIME”,
                                                ”TIMESTAMP”,
                                                ”TIMESTAMPADD”,
                                                ”TIMESTAMPDIFF”,
                                                ”TINYBLOB”,
                                                ”TINYINT”,
                                                ”TINYTEXT”,
                                                ”TO”,
                                                ”TRAILING”,
                                                ”TRANSACTION”,
                                                ”TRIGGER”,
                                                ”TRIGGERS”,
                                                ”TRUE”,
                                                ”TRUNCATE”,
                                                ”TYPE”,
                                                ”TYPES”,
                                                ”UNBOUNDED”,
                                                ”UNCOMMITTED”,
                                                ”UNDEFINED”,
                                                ”UNDO”,
                                                ”UNDOFILE”,
                                                ”UNDO_BUFFER_SIZE”,
                                                ”UNICODE”,
                                                ”UNINSTALL”,
                                                ”UNION”,
                                                ”UNIQUE”,
                                                ”UNKNOWN”,
                                                ”UNLOCK”,
                                                ”UNSIGNED”,
                                                ”UNTIL”,
                                                ”UPDATE”,
                                                ”UPGRADE”,
                                                ”USAGE”,
                                                ”USE”,
                                                ”USER”,
                                                ”USER_RESOURCES”,
                                                ”USE_FRM”,
                                                ”USING”,
                                                ”UTC_DATE”,
                                                ”UTC_TIME”,
                                                ”UTC_TIMESTAMP”,
                                                ”VALIDATION”,
                                                ”VALUE”,
                                                ”VALUES”,
                                                ”VARBINARY”,
                                                ”VARCHAR”,
                                                ”VARCHARACTER”,
                                                ”VARIABLES”,
                                                ”VARYING”,
                                                ”VCPU”,
                                                ”VIEW”,
                                                ”VIRTUAL”,
                                                ”VISIBLE”,
                                                ”WAIT”,
                                                ”WARNINGS”,
                                                ”WEEK”,
                                                ”WEIGHT_STRING”,
                                                ”WHEN”,
                                                ”WHERE”,
                                                ”WHILE”,
                                                ”WINDOW”,
                                                ”WITH”,
                                                ”WITHOUT”,
                                                ”WORK”,
                                                ”WRAPPER”,
                                                ”WRITE”,
                                                ”X509”,
                                                ”XA”,
                                                ”XID”,
                                                ”XML”,
                                                ”XOR”,
                                                ”YEAR”,
                                                ”YEAR_MONTH”,
                                                ”ZEROFILL”}

#End Region

#Region " New "

    Shared Sub New()
        Builder = New MySqlConnectionStringBuilder
    End Sub

#End Region

#Region " Connection String "

    Public Shared ReadOnly Builder As MySqlConnectionStringBuilder

    Public Shared Function ConnectionString(Server As String,
                                            User As String,
                                            Password As String) As String

        ' creates a connection string using the format
        ' sets Builder properties:
        '   Server
        '   UserId
        '   Password
        '
        ' vars passed: 
        '   Server - server name
        '   User - user name
        '   Password - user's password
        ' 
        ' returns:
        '   formatted connection string

        Builder.Clear()
        Builder.Server = Server.ToLower
        Builder.UserID = User.ToLower
        Builder.Password = Password
        Return Builder.ConnectionString
    End Function

    Public Shared Function ConnectionString(Server As String,
                                            User As String,
                                            Password As String,
                                            Database As String) As String

        ' creates a connection string using the format
        ' sets Builder properties:
        '   Server
        '   UserId
        '   Password
        '   Database
        '
        ' vars passed: 
        '   Server - server name
        '   User - user name
        '   Password - user's password
        '   Database - database name
        ' 
        ' returns:
        '   formatted connection string

        ConnectionString(Server, User, Password)
        Builder.Database = Database
        Return Builder.ConnectionString
    End Function

    Public Shared Function ConnectionString(Server As String,
                                            User As String,
                                            Password As String,
                                            Database As String,
                                            sslMode As MySqlSslMode) As String

        ' creates a connection string using the format
        ' sets Builder properties:
        '   Server
        '   UserId
        '   Password
        '   Database
        '   SslMode
        '
        ' vars passed: 
        '   Server - server name
        '   User - user name
        '   Password - user's password
        '   Database - database name
        '   sslMode - ssl mode
        ' 
        ' returns:
        '   formatted connection string

        ConnectionString(Server, User, Password, Database)
        Builder.SslMode = sslMode
        Return Builder.ConnectionString
    End Function

    Public Shared Function ConnectionString(Server As String,
                                            User As String,
                                            Password As String,
                                            Database As String,
                                            perSecInfo As Boolean) As String

        ' creates a connection string using the format
        ' sets Builder properties:
        '   Server
        '   UserId
        '   Password
        '   Database
        '   PersistSecurityInfo
        '
        ' vars passed: 
        '   Server - server name
        '   User - user name
        '   Password - user's password
        '   Database - database name
        '   perSecInfo - persist security info
        ' 
        ' returns:
        '   formatted connection string

        ConnectionString(Server, User, Password, Database)
        Builder.PersistSecurityInfo = perSecInfo
        Return Builder.ConnectionString
    End Function

    Public Shared Function ConnectionString(Server As String,
                                            User As String,
                                            Password As String,
                                            Database As String,
                                            perSecInfo As Boolean,
                                            sslMode As MySqlSslMode) As String

        ' creates a connection string using the format
        ' sets Builder properties:
        '   Server
        '   UserId
        '   Password
        '   Database
        '   PersistSecurityInfo
        '   SslMode
        '
        ' vars passed: 
        '   Server - server name
        '   User - user name
        '   Password - user's password
        '   Database - database name
        '   perSecInfo - persist security info
        '   sslMode - ssl mode
        ' 
        ' returns:
        '   formatted connection string

        ConnectionString(Server, User, Password, Database, perSecInfo)
        Builder.SslMode = sslMode
        Return Builder.ConnectionString
    End Function

    Public Shared Function SslModeFromString(sslModeStr As String) As MySqlSslMode

        Select Case sslModeStr.ToLower
            Case MySqlSslMode.None.ToString.ToLower
                Return MySqlSslMode.None
            Case MySqlSslMode.Prefered.ToString.ToLower, "preferred"
                Return MySqlSslMode.Prefered
            Case MySqlSslMode.Required.ToString.ToLower
                Return MySqlSslMode.Required
            Case MySqlSslMode.VerifyCA.ToString.ToLower
                Return MySqlSslMode.VerifyCA
            Case MySqlSslMode.VerifyFull.ToString.ToLower
                Return MySqlSslMode.VerifyFull
            Case Else
                Return MySqlSslMode.None
        End Select
    End Function

    Public Shared Function PrecisionSecurityInfoFromString(psInfoStr As String) As Boolean

        ' gets the precision security info value from a string
        '
        ' vars passed:
        '   psInfoStr - precision security info value as a string, either TRUE or FALSE
        '
        ' returns:
        '   TRUE - psInfoStr is not "FALSE"
        '   FALSE - psInfoStr is "FALSE"

        If psInfoStr.ToUpper = False.ToString.ToUpper Then
            Return False
        Else
            Return True
        End If
    End Function

#End Region

#Region " Connection "

    Public Shared ReadOnly Property ConnectionErr As Integer
        Get
            Return _connectionError
        End Get
    End Property

    Public Shared ReadOnly Property ConnectionErrMsg As String
        Get
            Return _connectionErrMsg        ' error message from MySql.Data.MySqlClient
        End Get
    End Property

    Public Shared ReadOnly Property CustomConnErrorMessage As String

        ' only use this if created ConnectionString from this class. 
        ' 1) call ConnectionString(...)  - this sets private vars used to create the custom error message
        ' 2) call MySqlConn(ConnectionString)  - pass the connection string from step 1
        ' if ConnectionString has not changed, ok to recall MySqlConn and user CustomConnErrorMessage 
        Get
            Return _customConnErrMsg
        End Get
    End Property

    Public Shared Function MySqlConn(ConnectionString As String) As MySqlConnection

        ' gets a connection to a mySql server
        ' opens and closes the connection to confirm the connection is valid
        '
        ' vars passed:
        '   ConnectionString - string with the connection parameters.
        '
        ' returns: 
        '   <> Nothing - MySqlConnection
        '   Nothing 
        '     - invalid connection string
        '     - error connection to MySqlServer
        '     - something went wrong

        Dim conn As MySqlConnection = Nothing
        _connectionError = NoErrors                                                     ' clear error code
        _connectionErrMsg = String.Empty                                                ' clear error messages
        _customConnErrMsg = String.Empty
        Try
            conn = New MySqlConnection(ConnectionString)                                ' create new connection
            conn.Open()                                                                 ' open the connection
        Catch exMySql As MySqlException                                                 ' if got native mySql exception
            _connectionErrMsg = exMySql.Message                                         ' get error message
            Select Case exMySql.Number
                Case ceGeneralError                                                     ' if a general error, get internal error for more detail
                    If exMySql.InnerException IsNot Nothing Then
                        If TypeOf (exMySql.InnerException) Is System.Exception Then
                            _connectionError = EaTools.IO.ioNoFileName
                            _connectionErrMsg &= " " & exMySql.InnerException.Message   ' add internal error message to general error message
                            _customConnErrMsg = exMySql.InnerException.Message
                        ElseIf TypeOf (exMySql.InnerException) Is MySqlException Then
                            Dim innerEx As MySqlException = CType(exMySql.InnerException, MySqlException)
                            Select Case innerEx.Number
                                Case ceInvalidUsernamePassword
                                    _connectionError = sqlInvalidUserNamePassword
                                    _customConnErrMsg = "Invalid user name or password"
                                Case ceDatabaseNotFound
                                    _connectionError = sqlNoDatabase
                                    _customConnErrMsg = "Database not found on server"
                                Case Else
                                    _connectionError = sqlOtherErr
                            End Select
                            _connectionErrMsg &= " " & innerEx.Message                  ' add internal error message to general error message
                        Else
                            _connectionErrMsg &= " " & exMySql.InnerException.Message   ' add internal error message to general error message
                        End If
                    Else                                                                ' else no exMySql.InnerException
                        If exMySql.ErrorCode = errCannotOpen Then                       ' if cannot open error, then user name or password invalid
                            _connectionError = sqlInvalidUserNamePassword
                            _customConnErrMsg = "Invalid user name or password"
                        End If
                    End If
                Case ceCannotConnectToToServer
                    _connectionError = sqlCannotConnectToServer
                    _customConnErrMsg = "Cannot connect to server"
                Case Else
                    _connectionError = sqlOtherErr
            End Select
        Catch ex As Exception
            _connectionErrMsg = ex.Message
            _connectionError = sqlOtherErr
            Return Nothing
        Finally
            Try
                If conn IsNot Nothing AndAlso conn.State = ConnectionState.Open Then    ' if got connection and connection is open
                    conn.Close()                                                        ' close the connection
                End If
            Catch exClose As Exception
                _connectionError = sqlCouldNotClose
            End Try
            If _connectionError <> NoErrors Then                                        ' if got errors
                conn.Dispose()                                                          ' free conn object's memory
                conn = Nothing                                                          ' reset connection to nothing
            End If
        End Try
        Return conn                                                                     ' return the connection
    End Function

#End Region

#Region " Error Code / Message "

    Shared ReadOnly Property ErrorCode As Integer
        Get
            Return _errorCode
        End Get
    End Property

    Shared ReadOnly Property ErrorMessage As String
        Get
            Return _errorMessage
        End Get
    End Property

#End Region

#Region " Create MySqlDataAdapt "

    Public Shared Function CreateMySqlDataAdapt(MySqlConn As MySqlConnection,
                                                ProcName As String,
                                                Optional Params As List(Of MySqlParam) = Nothing) As MySqlDataAdapter

        ' creates an MySqlDataAdapter
        '
        ' vars passed:
        '   MySqlConn - connection to use
        '   ProcName - name of procedure in MySql database        
        '   Params - list of parameters for procedure - pass [Nothing] to populate params later
        ' 
        '  returns:
        '   MySql.Data.MySqlClient.MySqlConnection - valid MySqlConnection
        '   Nothing - something went wrong

        _errorMessage = String.Empty
        Try
            Dim cmd As New MySqlCommand(ProcName, MySqlConn)                    ' create the my sql command using procedure name
            cmd.CommandType = CommandType.StoredProcedure                       ' set command type to stored procedure
            If Params IsNot Nothing Then                                        ' if got params
                For Each param In Params                                        ' for each param
                    cmd.Parameters.AddWithValue(param.Name, param.Value)        ' add param to command
                Next
            End If
            Return New MySqlDataAdapter(cmd)                                    ' return MySqlDataAdapter 
        Catch ex As Exception
            _errorCode = sqlOtherErr
            _errorMessage = String.Format(efOther, ex.Message)
            Return Nothing
        End Try
    End Function

    Public Shared Function CreateMySqlDataAdapt(MySqlConn As MySqlConnection,
                                                TableName As String,
                                                JustSelect As Boolean,
                                                Optional SelectCommandText As String = "") As MySqlDataAdapter

        ' creates an MySqlDataAdapter
        '
        ' vars passed:
        '   MySqlConn - connection to use
        '   TableName - name of table in MySql database        
        '   JustSelect - only populate the MySqlDataAdapter.SelectCommand.CommandText
        '   SelectCommandText - select command sql text
        ' 
        '  returns:
        '   MySql.Data.MySqlClient.MySqlConnection - valid MySqlConnection
        '   Nothing - something went wrong

        _errorMessage = String.Empty
        Try
            Dim sqlSelect As String
            If SelectCommandText = String.Empty Then                                                        ' if no specific select command
                sqlSelect = String.Format("SELECT {0}.* FROM {0}", TableName)                               ' use generic select all command
            Else                                                                                            ' else have specific sql select command
                sqlSelect = SelectCommandText                                                               ' use specific sql select command
            End If
            Dim MySqlDataAdapt As New MySqlDataAdapter(sqlSelect, MySqlConn)                                ' create mysql data adapter
            If MySqlDataAdapt Is Nothing Then                                                               ' if did not get a MySqlDataAdapter
                _errorCode = sqlCouldNotCreateDbAdapter
                _errorMessage = String.Format("Could not create a MySqlDataAdapter for table {0} in database {1} on server {2}.",
                                              TableName, MySqlConn.Database, MySqlConn.DataSource)
                Return Nothing                                                                              ' return nothing
            End If
            MySqlDataAdapt.SelectCommand = New MySqlCommand(sqlSelect, MySqlConn)                           ' create select command

            If JustSelect Then                                                                              ' if only want to select data
                MySqlDataAdapt.AcceptChangesDuringFill = False                                              ' when rows filled, RowState stays as Added         
                Return MySqlDataAdapt                                                                       ' return the MySqlDataAdapter now
            End If

            Dim cb As New MySqlCommandBuilder(MySqlDataAdapt)                                               ' get SQL command building 
            If cb Is Nothing Then                                                                           ' if did not get a command builder
                _errorCode = sqlCouldNotCreateCommandBuilder
                _errorMessage = String.Format("Could not create SQL commands for MySqlDataAdapter for table {0} in database {1} on server {2}.",
                                              TableName, MySqlConn.Database, MySqlConn.DataSource)
                Return Nothing                                                                              ' return nothing
            Else                                                                                            ' else got command builder
                If MySqlDataAdapt.SelectCommand Is Nothing Then                                             ' if no SELECT command
                    _errorCode = sqlCouldNotCreateSelectCommand
                    _errorMessage = String.Format("Could not create SQL SELECT command for MySqlDataAdapter for table {0} in database {1} on server {2}.",
                                                  TableName, MySqlConn.Database, MySqlConn.DataSource)
                    Return Nothing                                                                          ' return nothing
                End If
            End If

            MySqlDataAdapt.DeleteCommand = cb.GetDeleteCommand()                                            ' get the delete command
            If MySqlDataAdapt.DeleteCommand Is Nothing OrElse MySqlDataAdapt.DeleteCommand.CommandText = String.Empty Then
                _errorCode = sqlCouldNotCreateDeleteCommand
                _errorMessage = String.Format("Could not create SQL DELETE command for MySqlDataAdapter for table {0} in database {1} on server {2}.",
                                              TableName, MySqlConn.Database, MySqlConn.DataSource)
                Return Nothing                                                                              ' return nothing
            End If
            MySqlDataAdapt.InsertCommand = cb.GetInsertCommand()                                            ' get the insert command
            If MySqlDataAdapt.InsertCommand Is Nothing OrElse MySqlDataAdapt.InsertCommand.CommandText = String.Empty Then
                _errorCode = sqlCouldNotCreateInsertCommand
                _errorMessage = String.Format("Could not create SQL INSERT command for MySqlDataAdapter for table {0} in database {1} on server {2}.",
                                              TableName, MySqlConn.Database, MySqlConn.DataSource)
                Return Nothing                                                                              ' return nothing
            End If
            MySqlDataAdapt.UpdateCommand = cb.GetUpdateCommand()                                            ' get the update command
            If MySqlDataAdapt.UpdateCommand Is Nothing OrElse MySqlDataAdapt.UpdateCommand.CommandText = String.Empty Then
                _errorCode = sqlCouldNotCreateUpdateCommand
                _errorMessage = String.Format("Could not create SQL UPDATE command for MySqlDataAdapter for table {0} on connection {1}.",
                                              TableName, MySqlConn.Database, MySqlConn.DataSource)
                Return Nothing                                                                              ' return nothing
            End If

            Return MySqlDataAdapt                   ' return the MySqlDataAdapt
        Catch ex As Exception
            _errorCode = sqlOtherErr
            _errorMessage = String.Format(efOther, ex.Message)
            Return Nothing
        End Try
    End Function

    Public Shared Function CreateMySqlDataAdapt(MySqlConn As MySqlConnection,
                                                TableName As String,
                                                SelectCommandText As String,
                                                DeleteCommandText As String,
                                                InsertCommandText As String,
                                                UpdateCommandText As String) As MySqlDataAdapter

        ' creates an MySqlDataAdapter
        '
        ' vars passed:
        '   MySqlConn - connection to use
        '   TableName - name of table in MySql database        
        '   SelectCommandText - select command text
        '   DeleteCommandText - delete command text
        '   InsertCommandText - insert command text
        '   UpdateCommandText - update command text
        ' 
        '  returns:
        '   MySql.Data.MySqlClient.MySqlConnection - valid MySqlConnection
        '   Nothing - something went wrong

        _errorMessage = String.Empty
        Try
            Dim MySqlDataAdapt As New MySqlDataAdapter()
            If MySqlDataAdapt Is Nothing Then                                                               ' if did not get a MySqlDataAdapter
                _errorCode = sqlCouldNotCreateDbAdapter
                _errorMessage = String.Format("Could not create a MySqlDataAdapter for table {0} in database {1} on server {2}.",
                                              TableName, MySqlConn.Database, MySqlConn.DataSource)
                Return Nothing                                                                              ' return nothing
            End If

            If SelectCommandText <> String.Empty Then
                MySqlDataAdapt.SelectCommand = New MySqlCommand(SelectCommandText, MySqlConn)               ' create select command
                If MySqlDataAdapt.SelectCommand Is Nothing OrElse MySqlDataAdapt.SelectCommand.CommandText = String.Empty Then
                    _errorCode = sqlCouldNotCreateDeleteCommand
                    _errorMessage = String.Format("Could not create SQL SELECT command for MySqlDataAdapter for table {0} in database {1} on server {2}.",
                                              TableName, MySqlConn.Database, MySqlConn.DataSource)
                    Return Nothing                                                                          ' return nothing
                End If
            End If

            If DeleteCommandText <> String.Empty Then
                MySqlDataAdapt.DeleteCommand = New MySqlCommand(DeleteCommandText, MySqlConn)               ' get the delete command
                If MySqlDataAdapt.DeleteCommand Is Nothing OrElse MySqlDataAdapt.DeleteCommand.CommandText = String.Empty Then
                    _errorCode = sqlCouldNotCreateDeleteCommand
                    _errorMessage = String.Format("Could not create SQL DELETE command for MySqlDataAdapter for table {0} in database {1} on server {2}.",
                                                  TableName, MySqlConn.Database, MySqlConn.DataSource)
                    Return Nothing                                                                          ' return nothing
                End If
            End If

            If InsertCommandText <> String.Empty Then
                MySqlDataAdapt.InsertCommand = New MySqlCommand(InsertCommandText, MySqlConn)               ' get the insert command
                If MySqlDataAdapt.InsertCommand Is Nothing OrElse MySqlDataAdapt.InsertCommand.CommandText = String.Empty Then
                    _errorCode = sqlCouldNotCreateInsertCommand
                    _errorMessage = String.Format("Could not create SQL INSERT command for MySqlDataAdapter for table {0} in database {1} on server {2}.",
                                                  TableName, MySqlConn.Database, MySqlConn.DataSource)
                    Return Nothing                                                                          ' return nothing
                End If
            End If

            If UpdateCommandText <> String.Empty Then
                MySqlDataAdapt.UpdateCommand = New MySqlCommand(UpdateCommandText, MySqlConn)               ' get the update command
                If MySqlDataAdapt.UpdateCommand Is Nothing OrElse MySqlDataAdapt.UpdateCommand.CommandText = String.Empty Then
                    _errorCode = sqlCouldNotCreateUpdateCommand
                    _errorMessage = String.Format("Could not create SQL UPDATE command for MySqlDataAdapter for table {0} on connection {1}.",
                                                  TableName, MySqlConn.Database, MySqlConn.DataSource)
                    Return Nothing                                                                          ' return nothing
                End If
            End If

            Return MySqlDataAdapt                   ' return the MySqlDataAdapt
        Catch ex As Exception
            _errorCode = sqlOtherErr
            _errorMessage = String.Format(efOther, ex.Message)
            Return Nothing
        End Try
    End Function

#End Region

#Region " Execute Non Query "

    Public Shared Function ExecuteNonQuery(MySqlConn As MySqlConnection,
                                           SqlText As String,
                                           Optional ErrToHandle As Integer = sqlNoErrors,
                                           Optional HandledReturn As Integer = sqlNoErrors,
                                           Optional Params As List(Of MySqlParam) = Nothing) As Integer

        ' executes a non query SQL command
        '
        ' vars passed:
        '   MySqlConn - MySql connection to use
        '   SqlText - SqlText to use
        '   ErrToHandle - error value to handle
        '   HandledReturn - return value for error handled
        '   Params - list of parameter names and values for query
        '
        ' returns:
        '   >= 0 - no errors
        '   sqlNoConnection - no connection
        '   sqlCouldNotOpen - could not open
        '   sqlCouldNotClose - could not close
        '   <0 - SqlCommand.ExecuteNonQuery error value
        '   sqlOtherErr - other error in sql 

        _errorMessage = String.Empty
        Dim returnValue As Integer = 0
        Dim TryCount As Integer = 0
        Try
            Do
                Try
                    Using MySqlCommand As New MySqlCommand(SqlText, MySqlConn)
                        MySqlCommand.Connection.Open()                                                  ' open the connection
                        If MySqlCommand.Connection.State <> ConnectionState.Open Then                   ' if did not open
                            Return sqlCouldNotOpen                                                      ' return error
                        End If

                        If Params IsNot Nothing Then                                                    ' if got list of params
                            If MySqlCommand.Parameters.Count = 0 Then                                   ' if sql command does not have params saved
                                For Each param In Params                                                ' for each parameter
                                    MySqlCommand.Parameters.AddWithValue(param.Name, param.Value)       ' add the param to sql command
                                Next
                            Else                                                                        ' else sql command has params saved
                                For Each param In Params                                                                ' for each parameter
                                    For i As Integer = 0 To MySqlCommand.Parameters.Count - 1                           ' for each param in command
                                        If MySqlCommand.Parameters(i).ParameterName.ToUpper = param.Name.ToUpper Then   ' if found param name
                                            MySqlCommand.Parameters(i).Value = param.Value                              ' reset param value
                                            Exit For
                                        End If
                                    Next
                                Next
                            End If
                        End If

                        returnValue = MySqlCommand.ExecuteNonQuery()                                    ' execute the non query
                        Exit Do
                    End Using
                Catch exMySql As MySqlException
                    If exMySql.ErrorCode = ErrToHandle Then                                             ' if got the error to handle
                        returnValue = HandledReturn                                                     ' set return value
                        Exit Do
                    ElseIf (exMySql.ErrorCode = errCannotOpen OrElse exMySql.ErrorCode = errCannotModify) AndAlso TryCount < MaxTries Then
                        TryCount += 1                                                                   ' increment try count
                        If TryCount >= MaxTries Then                                                    ' if over max tries
                            _errorMessage = "Over max tries"
                            returnValue = sqlOverMaxTries                                               ' set return value                            
                            Exit Do
                        End If
                        'My.Application.DoEvents()                                                       ' make sure everything is done
                        'Debug.WriteLine(String.Format(DebugFormatStandard, "ExecuteNonQuery", TryCount, SqlText))

                        System.Threading.Thread.Sleep(WaitMilliSeconds)                                  ' wait for a time
                    Else                                                                                ' else got an unhandled error
                        returnValue = exMySql.ErrorCode                                                 ' set return error
                        Exit Do                                                                         ' ok to exit 
                    End If
                Catch ex As Exception
                    _errorMessage = String.Format(efOther, ex.Message)
                    returnValue = sqlOtherErr                                                           ' set return value
                    Exit Do
                End Try
            Loop
            Return returnValue
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            returnValue = sqlOtherErr                                                                   ' set return value                        
        Finally
            Try
                If MySqlConn IsNot Nothing Then                                                         ' if got a command
                    If MySqlConn.State <> ConnectionState.Closed Then                                   ' if connection not closed
                        MySqlConn.Close()                                                               ' close the connection
                    End If
                End If
            Catch ex As Exception
                _errorMessage = String.Format(efClosing, ex.Message, MySqlConn.DataSource)
                returnValue = sqlCouldNotClose
            End Try
        End Try
        Return returnValue
    End Function

#End Region

#Region " Execute Scalar "

    Public Shared Function ExecuteScalar(MySqlConn As MySqlConnection,
                                         SqlText As String) As Integer

        ' executes a scalar SQL command
        '
        ' vars passed:
        '   MySqlConn - MySql connection to use
        '   SqlText - SqlText to use
        '
        ' returns:
        '   >= 0 - no errors
        '   sqlNoConnection - no connection
        '   sqlCouldNotOpen - could not open connection
        '   sqlCouldNotClose - could not close connection
        '   <0 - SqlCommand.ExecuteNonQuery error value
        '   sqlOtherErr - other error in sql 

        Dim MySqlCommand As MySqlCommand = Nothing                                              ' sql command
        Dim ScalarReturn As Integer = 0                                                         ' scalar command return value
        Dim Done As Boolean = False
        Dim TryCount As Integer = 0
        Do
            Try
                If MySqlConn Is Nothing Then                                                    ' if no connection
                    Return sqlNoConnection                                                      ' return error
                End If

                MySqlCommand = New MySqlCommand(SqlText, MySqlConn)                             ' get the sql command
                MySqlCommand.Connection.Open()                                                  ' open the connection
                If MySqlCommand.Connection.State <> ConnectionState.Open Then                   ' if did not open
                    Return sqlCouldNotOpen                                                      ' return error
                End If

                ScalarReturn = Convert.ToInt32(MySqlCommand.ExecuteScalar())                    ' execute the scalar command
                Done = True                                                                     ' done with loop
            Catch exMySql As MySqlException
                If (exMySql.ErrorCode = errCannotOpen OrElse exMySql.ErrorCode = errCannotModify) AndAlso TryCount < MaxTries Then
                    TryCount += 1                                                               ' increment try count
                    'My.Application.DoEvents()                                                   ' make sure everything is done
                    'Debug.WriteLine("No DoEvents")
                    System.Threading.Thread.Sleep(WaitMilliSeconds)                              ' wait for a time
                Else                                                                            ' else got an unhandled error
                    ScalarReturn = exMySql.ErrorCode                                            ' set return error
                    Done = True                                                                 ' ok to exit 
                End If
            Catch ex As Exception
                Return sqlOtherErr
            Finally
                Try
                    If MySqlCommand IsNot Nothing Then                                          ' if got a command
                        If MySqlCommand.Connection.State <> ConnectionState.Closed Then         ' if connection not closed
                            MySqlCommand.Connection.Close()                                     ' close the connection
                        End If
                        MySqlCommand.Dispose()                                                  ' free sql command
                        MySqlCommand = Nothing                                                  ' set sql command to nothing 
                    End If
                Catch ex As Exception
                    ScalarReturn = sqlCouldNotClose
                    Done = True
                End Try
            End Try
        Loop Until Done
        Return ScalarReturn
    End Function

#End Region

#Region " DoesItExist "

    Private Shared Function DoesItExist(MySqlConn As MySqlConnection,
                                        ObjToFind As MySqlInfo,
                                        Optional ErrToHandle As Integer = sqlNoErrors) As ExistsTypes

        ' checks to see if a item exits 
        '
        ' vars passed:
        '   MySqlConn - mySql connection to use
        '   ObjToFind - object to find 
        '   ErrToHandle - error code to handle 
        '
        ' returns:
        '   ExistsTypes.Yes - it was found
        '   ExistsTypes.No - it was not found
        '   ExistsTypes.OverMaxTries - tried more than MaxTries to open connection
        '   ExistsTypes.GotError 
        '     - error getting table from MySqlConn
        '     - error closing connection
        '     - something went wrong

        _errorMessage = String.Empty                                                                            ' clear error message
        _errorCode = 0
        Dim TryCount As Integer = 0
        Dim returnValue As ExistsTypes = ExistsTypes.No
        Try
            Do
                Try
                    Using MySqlCommand = New MySqlCommand(ObjToFind.SqlText, MySqlConn)
                        MySqlCommand.Connection.Open()                                                          ' open the connection
                        If MySqlCommand.Connection.State <> ConnectionState.Open Then                           ' if did not open
                            returnValue = ExistsTypes.GotError                                                  ' return error
                            Exit Do
                        End If
                        Dim ReaderObject As MySqlDataReader = MySqlCommand.ExecuteReader                        ' get reader object                       

                        'Dim columns As New List(Of String)
                        'For i As Integer = 0 To ReaderObject.FieldCount - 1
                        '    columns.Add(ReaderObject.GetName(i))
                        '    Debug.WriteLine(String.Format("Column index: {0}, Column Name: {1}", i, columns(i)))
                        'Next

                        returnValue = ObjToFind.DoesItExits(ReaderObject, _errorMessage)                        ' see if item exists
                        Exit Do
                    End Using
                Catch exMySql As MySqlException
                    If (exMySql.ErrorCode = errCannotOpen) AndAlso (TryCount < MaxTries) Then                   ' if cant open & not over max tries
                        TryCount += 1                                                                           ' increment try count
                        If TryCount >= MaxTries Then                                                            ' if over max tries
                            _errorMessage = "Over max tries"
                            returnValue = ExistsTypes.OverMaxTries                                              ' return over max tries
                            Exit Do
                        End If
                        'My.Application.DoEvents()                                                               ' process all windows messages
                        'Debug.WriteLine(String.Format(DebugFormatFind, "DoesItExists", TryCount, ObjToFind.MySqlTypeToFind.ToString, ObjToFind.NameToFind))
                        System.Threading.Thread.Sleep(EaSql.Ea.WaitMilliSeconds)                                         ' sleep for a time
                    ElseIf exMySql.ErrorCode = ErrToHandle Then                                                 ' else if an handled error
                        If ObjToFind.MySqlTypeToFind = MySqlInfo.MySqlTypesToFindTypes.Column Then              ' if trying to find column
                            If ErrToHandle = errNoColumn Then                                                   ' if a no column error
                                returnValue = ExistsTypes.No                                                    ' return does not exits
                            Else                                                                                ' else other error
                                returnValue = ExistsTypes.GotError                                              ' return error
                            End If
                            Exit Do
                        End If
                    Else
                        _errorMessage = String.Format(efCode, CStr(exMySql.ErrorCode), exMySql.Message, MySqlConn.DataSource)
                        _errorCode = exMySql.ErrorCode
                        returnValue = ExistsTypes.GotError
                        Exit Do
                    End If
                Catch ex As Exception
                    _errorMessage = String.Format(efOther, ex.Message)
                    _errorCode = ex.HResult
                    returnValue = ExistsTypes.GotError
                    Exit Do
                End Try
            Loop
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            _errorCode = ex.HResult
            returnValue = ExistsTypes.GotError
        Finally
            Try
                If MySqlConn IsNot Nothing Then                                                                 ' if got a command
                    If MySqlConn.State <> ConnectionState.Closed Then                                           ' if connection not closed
                        MySqlConn.Close()                                                                       ' close the connection
                    End If
                End If
            Catch ex As Exception
                _errorMessage = String.Format(efClosing, ex.Message, MySqlConn.DataSource)
                _errorCode = sqlCouldNotClose
                returnValue = ExistsTypes.GotError
            End Try
        End Try
        Return returnValue
    End Function

#End Region

#Region " Database Actions "

#Region " Create Database "

    Public Shared Function CreateDatabase(MySqlConn As MySqlConnection,
                                          Database As String) As Integer

        ' creates a database
        '
        ' vars passed:
        '   MySqlConn - mySql connection to use
        '   database - database name to create
        '
        ' returns:
        '   >= 0 - database was created
        '   sqlInvalidConnStr - in connection string, database param not blank
        '   sqlCreateErr - something went wrong
        '   <0 - other error

        ' CREATE DATABASE IF NOT EXISTS `{0}`
        Const CreateDatabaseFormat As String = "CREATE DATABASE IF NOT EXISTS `{0}`"

        Try
            If Not DatabaseParamBlank(MySqlConn) Then                                               ' if got a database param
                _errorMessage = emGotDatabaseParam
                Return sqlInvalidConnStr                                                            ' return error
            End If

            Dim dbMySqlInfo As New MySqlInfo(MySqlInfo.MySqlTypesToFindTypes.Database, Database)
            If DoesItExist(MySqlConn, dbMySqlInfo) = ExistsTypes.Yes Then                           ' if found database
                Dim DropErr As Integer = DropDatabase(MySqlConn, Database)                          ' drop database
                If DropErr < 0 Then                                                                 ' if error dropping database
                    Return DropErr                                                                  ' return error
                End If
            End If

            Dim SqlText As String = String.Format(CreateDatabaseFormat, Database)                   ' get sql command text
            Return ExecuteNonQuery(MySqlConn, SqlText, NoErrors, NoErrors)                          ' create database
        Catch ex As Exception
            Return sqlOtherErr
        End Try
    End Function

#End Region

#Region " Database Param Blank "

    Private Shared Function DatabaseParamBlank(MySqlConn As MySqlConnection) As Boolean

        ' checks if there is a database identifier in the connection string, and if there is, is the parameter blank
        '
        ' vars passed:
        '   MySqlConn - connection to use
        '
        ' returns
        '   TRUE 
        '     - no database identifier
        '     - database identifier found, database param is blank
        '   FALSE
        '     - database identifier found, database param is NOT blank
        '     - something went wrong

        Const dbIndentifier As String = "database="

        Try
            Dim connStr As String = MySqlConn.ConnectionString.ToLower          ' get all lower case for connection string
            Dim index As Integer = connStr.IndexOf(dbIndentifier)               ' check if database identifier is in connection string
            If index < 0 Then                                                   ' if no database param 
                Return True                                                     ' then return TRUE
            End If
            index = connStr.IndexOf(dbIndentifier & ";")                        ' now check if database param is empty
            If index >= 0 Then                                                  ' if found blank database param
                Return True                                                     ' return TRUE
            Else                                                                ' else have database identifier and a param
                Return False                                                    ' return FALSE
            End If
        Catch ex As Exception
            Return False
        End Try
    End Function

#End Region

#Region " Database Exists "

    Public Shared Function DatabaseExists(MySqlConn As MySqlConnection,
                                          Database As String) As ExistsTypes

        ' checks to see if a database exists
        '
        ' vars passed:
        '   MySqlConn - mySql connection to use
        '   database - database name to create
        '
        ' returns:
        '   ExistsTypes.Yes - database was found
        '   ExistsTypes.No - database was not found
        '   ExistsTypes.OverMaxTries - tried more than MaxTries to open connection
        '   ExistsTypes.GorError - 
        '     - error getting dbMySqlInfo
        '     - error in DoesItExist
        '     - error closing connection
        '     - something went wrong

        _errorMessage = String.Empty                                                                ' clear error message
        Try
            Dim objToFind As New MySqlInfo(MySqlInfo.MySqlTypesToFindTypes.Database, Database)      ' get object to find info
            Return DoesItExist(MySqlConn, objToFind)                                                ' see if database exists
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return ExistsTypes.GotError
        End Try
    End Function

#End Region

#Region " Drop Database "

    Public Shared Function DropDatabase(MySqlConn As MySqlConnection,
                                        Database As String) As Integer

        ' creates a database
        '
        ' vars passed:
        '   MySqlConn - mySql connection to use
        '   database - database name to create
        '
        ' returns:
        '   >= 0 - database was deleted                
        '   sqlOtherErr - something went wrong
        '   <0 - other error

        ' DROP DATABASE IF EXISTS `{0}`
        Const DropDatabaseFormat As String = "DROP DATABASE IF EXISTS `{0}`"

        Try
            Dim dbMySqlInfo As New MySqlInfo(MySqlInfo.MySqlTypesToFindTypes.Database, Database)    ' get database info to find
            Dim foundDataBase As ExistsTypes = DoesItExist(MySqlConn, dbMySqlInfo)                  ' see if data base exists
            If foundDataBase = ExistsTypes.GotError Then                                            ' if error checking if database exists
                Return sqlOtherErr                                                                  ' return error
            ElseIf foundDataBase = ExistsTypes.No Then                                              ' else if cannot find database
                Return NoErrors                                                                     ' return no errors, no need to delete
            End If

            Dim SqlText As String = String.Format(DropDatabaseFormat, Database)                     ' get sql command text
            Return ExecuteNonQuery(MySqlConn, SqlText, NoErrors, NoErrors)                          ' drop database
        Catch ex As Exception
            Return sqlOtherErr
        End Try
    End Function

#End Region

#End Region

#Region " Table Actions "

#Region " Add Key / Drop Key "

    Public Shared Function AddKey(MySqlConn As MySqlConnection,
                                  TableName As String,
                                  MySqlColumns As List(Of MySqlColumn),
                                  Optional KeyName As String = "") As Integer

        ' adds a multi column key to a table
        ' 
        ' vars passed:
        '   MySqlConn - mySql connection to use
        '   TableName - table name to add primary key to
        '   MySqlColumns - list of column names and types
        '   KeyName - name of primary key (MySql 5 needs key name, mySql 8 KeyName is always 'PRIMARY')
        '
        ' returns: 
        '   >= 0 - no errors
        '   <0 - SqlCommand.ExecuteNonQuery error value
        '   sqlNoTableName - no table name 
        '   sqlNoKeyName - no key name
        '   sqlNoColumn - no columns
        '   sqlErr- other error in sql 
        '   sqlInvalidAlter - invalid alter option (not atAddColumn or atDropColumn)
        '   sqlAlterErr  - error in alter sql 

        ' mySql 5
        ' ALTER TABLE `<tablename>` ADD CONSTRAINT <KeyName> PRIMARY KEY (`columnname1`, `columnname2`,..>)
        Const AddKeyFormat5 As String = "ALTER TABLE `{0}` ADD CONSTRAINT {1} PRIMARY KEY ("

        ' mySql 8
        ' ALTER TABLE `<tablename>` ADD CONSTRAINT <KeyName> PRIMARY KEY (`columnname1`, `columnname2`,..>)
        Const AddKeyFormat8 As String = "ALTER TABLE `{0}` ADD CONSTRAINT PRIMARY KEY ("

        Try
            GetMySqlVersion(MySqlConn)
            Dim SqlText As String
            If MySqlVer.Major = 5 Then
                SqlText = String.Format(AddKeyFormat5, TableName.ToLower, KeyName)
            ElseIf MySqlVer.Major = 8 Then
                SqlText = String.Format(AddKeyFormat8, TableName.ToLower)
            Else
                _errorMessage = emInvalidVersion
                Return sqlVersionErr
            End If

            Dim ColCount As Integer = 0
            Dim aColCount As Integer = 0
            For Each col As MySqlColumn In MySqlColumns                             ' for each column in key columns
                SqlText &= String.Format("`{0}`", col.ColumnName)                   ' add column name
                If ColCount < MySqlColumns.Count - 1 Then                           ' if got more columns
                    SqlText &= ","                                                  ' add a comma
                End If
                ColCount += 1
            Next
            SqlText &= ")"                                                          ' add ending ")"
            Return ExecuteNonQuery(MySqlConn, SqlText)                              ' alter table to add key
        Catch ex As Exception
            Return sqlAlterErr
        End Try
    End Function

    Public Shared Function AddKey(MySqlConn As MySqlConnection,
                                  TableName As String,
                                  PrimariesView As DataView) As Integer

        ' creates a multi part primary key a table 
        '
        ' vars passed:
        '   MySqlConn - mysql connection to use
        '   TableName - name of table to create primary key for
        '   PrimariesView - data view with primary key information (table is IndexesTable created in GetTableStructure)
        '
        ' returns:
        '   NoErrors - no errors
        '   sqlNoColumnName - no column name
        '   sqlInvalidColumnType - invalid column type
        '   sqlCreatePrimaryKey - could not create primary key
        '   sqlOtherErr - something went wrong

        Try
            GetMySqlVersion(MySqlConn)
            Dim PrimaryKeyName As String = String.Empty
            If MySqlVer.Major = 5 Then                                                              ' only need pk name if mysql 5
                PrimaryKeyName = GcrcvAs(PrimariesView(0).Row, cnIndexName, String.Empty)           ' get primary key name
                If PrimaryKeyName = String.Empty Then                                               ' if no primary key name
                    Return sqlCreatePrimaryKey                                                      ' return error
                End If
                PrimaryKeyName = ValidatePrimaryKeyName(PrimaryKeyName)                             ' make sure primary key name is valid
            End If

            Dim KeyCols As New List(Of MySqlColumn)                                                 ' get list columns for key
            Dim col As MySqlColumn                                                                  ' key column
            Dim ColumnName As String
            For i As Integer = 0 To PrimariesView.Count - 1                                         ' for each row in columns view
                ColumnName = GcrcvAs(PrimariesView(i).Row, cnColumnName, String.Empty)              ' get the column name
                If ColumnName = String.Empty Then                                                   ' if no column name
                    Return sqlNoColumnName                                                          ' return error
                End If
                col = New MySqlColumn(ColumnName, MySqlDataType.dtNone, , True)                     ' create new col as a key col, no col type needed
                KeyCols.Add(col)                                                                    ' add column to list of key columns
            Next

            Dim Err = AddKey(MySqlConn, TableName, KeyCols, PrimaryKeyName)                         ' add the primary key
            If Err < 0 Then                                                                         ' if error creating primary key
                Return sqlCreatePrimaryKey                                                          ' return error
            End If

            Return NoErrors                 ' if got here, then no errors
        Catch ex As Exception
            Return sqlOtherErr
        End Try
    End Function

    Public Shared Function DropKey(MySqlConn As MySqlConnection,
                                   TableName As String,
                                   Optional KeyName As String = "") As Integer

        ' adds a multi column key to a table
        ' 
        ' vars passed:
        '   MySqlConn - mysql connection to use
        '   TableName - table to store foreign key rows to delete        
        '   KeyName - name of primary key (MySql 5 needs key name, mySql 8 KeyName is always 'PRIMARY')
        '
        ' returns: 
        '   >= 0 - no errors
        '   <0 - SqlCommand.ExecuteNonQuery error value
        '   sqlNoTableName - no table name 
        '   sqlNoKeyName - no key name
        '   sqlNoColumn - no columns
        '   sqlErr- other error in sql 
        '   sqlInvalidAlter - invalid alter option (not atAddColumn or atDropColumn)
        '   sqlAlterErr  - error in alter sql 

        Try
            ' Column param not used when dropping a key
            Return AlterTableColumn(MySqlConn, TableName, Nothing, AlterTableTypes.atDropKey, KeyName)
        Catch ex As Exception
            Return sqlOtherErr
        End Try
    End Function

    Private Shared Function ValidatePrimaryKeyName(PrimaryKeyName As String) As String

        ' makes sure Primary Key name is valid
        '
        ' vars passed:
        '   PrimaryKeyName - primary key name to check
        '
        ' returns:
        '   PrimaryKeyName - no "#" in primary key name
        '   [PrimaryKeyName] - primary key name has a "#" in it

        If PrimaryKeyName.StartsWith("["c) AndAlso PrimaryKeyName.EndsWith("]"c) Then   ' if primary key name already has square brackets []  
            Return PrimaryKeyName                                                       ' return primary key 
        End If
        If PrimaryKeyName.IndexOf("#"c) > -1 Then                                       ' if primary key name has "#" in it
            PrimaryKeyName = String.Format("[{0}]", PrimaryKeyName)                     ' surround primary key name with []
        End If
        Return PrimaryKeyName
    End Function

#End Region

#Region " Create Table "

    Public Shared Function CreateTable(MySqlConn As MySqlConnection,
                                       TableName As String,
                                       SqlText As String) As Integer

        ' create a new data table
        '
        ' vars passed:
        '   MySqlConn - connection to use
        '   TableName - table to store foreign key rows to delete
        '   SqlText - Sql Text to use
        '
        ' returns:
        '   >= 0 - no errors
        '   <0 - SqlCommand.ExecuteNonQuery error value
        '   sqlOtherErr- other error in sql 
        '   sqlDropErr - other error in dropping table
        '   sqlCreateErr - other error in creating table

        Try
            Dim CtReturn As Integer = DropTable(MySqlConn, TableName)                   ' try to drop table (if it exists)
            If CtReturn < 0 Then                                                        ' if error in drop table
                Return CtReturn                                                         ' return drop error
            End If
            Return ExecuteNonQuery(MySqlConn, SqlText)                                  ' create the table via execute non query
        Catch ex As Exception
            Return sqlCreateErr
        End Try
    End Function

    Public Shared Function CreateTable(MySqlConn As MySqlConnection,
                                       TableName As String,
                                       columns As List(Of MySqlColumn),
                                       Optional KeyWordsOk As Boolean = False) As Integer

        ' create a new data table
        '
        ' vars passed:
        '   MySqlConn - mysql connection to use
        '   TableName - table to store foreign key rows to delete
        '   columns - list of MySqlColumns
        '   KeyWordsOk - TRUE if ok to use key words in column names
        '
        ' returns:
        '   >= 0 - no errors
        '   <0 - SqlCommand.ExecuteNonQuery error value
        '   sqlNoTableName - no table name 
        '   sqlNoColumn - no columns
        '   sqlKeyWord - column name is a SQL key word
        '   sqlOtherErr - other error in sql 
        '   sqlDropErr - other error in dropping table
        '   sqlCreateErr - other error in creating table
        '
        '   CREATE TABLE `<databasename>`.`<tablename>` (
        '       `<field1name>` <Field1Type> [NULL | NOT NULL] [DEFAULT {literal | (expr)} ] [AUTO_INCREMENT] [UNIQUE [KEY]] [[PRIMARY] KEY], 
        '       `<field2name>` <Field2Type> [NULL | NOT NULL], ..
        '       [PRIMARY KEY (`<field1name>`, `<field2name>`,..)] 
        '       
        '   )
        '   if FieldType is varChar, then use VarChar(<Size>) for FieldType

        Const CreateTableInitFormat As String = "CREATE TABLE `{0}`.`{1}` ("

        Try
            ' create table + table name + start bracket for fields
            Dim SqlText As String = String.Format(CreateTableInitFormat, MySqlConn.Database.ToLower, TableName.ToLower)
            Dim ColText As String
            Dim colCount As Integer = 0
            Dim KeyCount As Integer = 0
            Dim KeyColumns As String = String.Empty

            ' count # of key columns
            For Each col As MySqlColumn In columns                                        ' for each column
                If col.KeyField Then                                                        ' if a key column   
                    KeyCount += 1                                                           ' increment key col count
                End If
            Next

            ' get field names string for SQL
            For Each col As MySqlColumn In columns                                        ' for each column
                ' no space for first col
                If colCount = 0 Then                                                        ' if first column
                    ColText = String.Empty                                                  ' no need for a space before column name
                Else                                                                        ' else not first column
                    ColText = " "                                                           ' need space before column name
                End If

                ' make sure column name is valid
                If (Not KeyWordsOk) AndAlso IsKeyWord(col.ColumnName) Then                  ' if column name is a Key word
                    col.ColumnName = GetNonKeyWord(col.ColumnName, "Column Name")           ' get non key work column name from user
                    If col.ColumnName = String.Empty Then                                   ' if no non keyword column name
                        Return sqlKeyWord                                                   ' return error
                    End If
                End If
                ColText &= String.Format("`{0}` {1}", col.ColumnName, col.SqlDataType.ToString.ToUpper)  ' get column name and data type

                If col.DataType Is MySqlDataType.dtVarChar Then                             ' if a varChar column
                    ColText &= String.Format("({0})", CStr(col.Length))                     ' add in size "(XX)"
                ElseIf col.DataType Is MySqlDataType.dtDecimal Then                         ' if a decimal column
                    ColText &= String.Format("({0},{1})", col.Length, col.Precision)        ' add in size "(M.D)"
                End If
                If Not col.IsNullable Then                                                  ' if column is not nullable (required)
                    ColText &= " NOT NULL"                                                  ' add NOT NULL 
                Else                                                                        ' else column can be null
                    ColText &= " NULL"                                                      ' add NUL
                End If
                If col.AutoInc Then                                                         ' if an auto inc column
                    ColText &= " AUTO_INCREMENT"                                            ' add AUTO_INCREMENT
                End If
                If col.DefaultValue <> String.Empty Then                                    ' if have a default value
                    ColText &= " DEFAULT " & col.DefaultValue.ToString                      ' add default and default value
                    If col.OnUpdate <> String.Empty Then                                    ' if also got an OnUpdate value (must have default value)
                        ColText &= " ON UPDATE " & col.OnUpdate                             ' add ON UPDATE and on update value
                    End If
                End If

                ' get ending comma (if needed) 
                If colCount < columns.Count - 1 Then                                        ' if not last column
                    ColText &= ","                                                          ' add comma after field type
                End If
                SqlText &= ColText                                                          ' add column text to sql text
                colCount += 1                                                               ' increment column count
            Next

            ' create list of key column names AFTER gone built list of column names to create, 
            ' so any changes to key column names from GetNonKeyWord are captured here
            If KeyCount > 0 Then                                                            ' if got a key field(s)
                KeyCount = 0
                For Each col As MySqlColumn In columns                                      ' for each column
                    If col.KeyField Then                                                    ' if a key column   
                        If KeyCount > 0 Then                                                ' if not the first key column
                            KeyColumns &= ", "                                              ' add a comma space 
                        End If
                        KeyColumns &= String.Format("`{0}`", col.ColumnName)                ' add column name to string of key columns
                        KeyCount += 1                                                       ' increment key col count
                    End If
                Next

                SqlText &= String.Format(", PRIMARY KEY ({0})", KeyColumns)                 ' add ", PRIMARY KEY ( key column names )" 
            End If
            SqlText &= ")"                                                                  ' add ending bracket for fields, Primary Key
            Return CreateTable(MySqlConn, TableName, SqlText)                               ' create the table
        Catch ex As Exception
            Return sqlOtherErr
        End Try
    End Function

    Public Shared Function GetNonKeyWord(keyWord As String,
                                         Optional UsedAs As String = "") As String

        ' asks the user for a non key word for a current key word used when creating a query
        '
        ' vars passed:
        '   keyWord - key word used in a query (could be kay word as a column name: Date, Name...)
        '   UsedAs - how the key word is being used
        '
        ' returns:
        '   <> string.empty - non key word to used (somethingName instead of name, somethingDate instead of Date)
        '   string.empty
        '     - user canceled
        '     - something went wrong

        Try
            Dim nkw As String = keyWord
            Dim prompt As String
            Do
                If UsedAs = String.Empty Then
                    prompt = String.Format("""{0}"" is a MySql Keyword.  Please enter a different value:", keyWord)
                Else
                    prompt = String.Format("{1} ""{0}"" is a MySql Keyword.  Please enter a different value:", keyWord, UsedAs)
                End If
                nkw = InputBox(prompt, "Enter MySql Non Keyword", String.Empty)     ' get user to enter non key word
            Loop Until nkw = String.Empty OrElse Not IsKeyWord(nkw)                 ' exit if new entry is not a key work or user canceled
            Return nkw
        Catch ex As Exception
            Return String.Empty
        End Try
    End Function

    Public Shared Function IsKeyWord(text As String) As Boolean

        ' checks if text is a key word (see KeyWords array for full list of words)
        '
        ' vars passed:
        '   text - text to check if a key word
        '   
        ' returns:
        '   TRUE - text is a key word
        '   FALSE - text is not a key word

        Try
            Return KeyWords.Contains(text.ToUpper)
        Catch ex As Exception
            Return False
        End Try
    End Function

#End Region

#Region " Delete Foreign Key Rows/Empty Table "

    Public Shared Function DeleteForeignKeyRows(MySqlConn As MySqlConnection,
                                                TableName As String,
                                                fkLinkFieldName As String,
                                                fkLinkId As Object) As Integer

        ' deletes foreign key rows in different, non linked table
        ' note: this is a manual deletion of foreign key rows.
        '
        ' vars passed:
        '   MySqlConn - mysql connection to use
        '   TableName - table to store foreign key rows to delete
        '   fkLinkFieldName - name of field that links foreign key table
        '   fkLinkId - value of linking field in in the foreign key table 
        '
        ' returns:
        '   >= 0 - no errors
        '   sqlDeleteErr  - error in delete sql 
        '   sqlOtherErr - other error in SQL

        ' "DELETE FROM `<tablename>` WHERE (<fkLinkFieldName> = <fkLinkIdStr>)"
        Const DelKeyFormat As String = "DELETE FROM `{0}` WHERE {1}"

        _errorMessage = String.Empty
        Try
            Dim fkLinkIdStr As String = String.Empty
            ' get fk link id as a string
            Try
                fkLinkIdStr = fkLinkId.ToString                                             ' get id string
            Catch ex1 As Exception
                Return sqlOtherErr                                                          ' if got an error, return 
            End Try

            ' create the SQL delete command text ' (" & fkLinkFieldName & " = " & fkLinkIdStr & ")"
            Dim WhereText As String = EaSql.Sql.SqlColumnValueString(String.Empty, fkLinkFieldName, fkLinkId)
            Dim SqlDelText As String = String.Format(DelKeyFormat, TableName, WhereText)    ' get sql command text

            Return ExecuteNonQuery(MySqlConn, SqlDelText)                                   ' delete the foreign key rows
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return sqlOtherErr
        End Try
    End Function

    Public Shared Function EmptyTable(MySqlConn As MySqlConnection,
                                      TableName As String) As Integer

        ' empty data from a table
        ' 
        ' vars passed:
        '   TableName - name of table with data to clear
        '   MySqlConn - mysql connection to use
        ' 
        ' returns:
        '   NoErrors 
        '   sqlNoTableName - no table name
        '   sqlOtherErr - something went wrong

        ' DELETE FROM `<database>`.`<tablename>`
        Const EmptyTableFormat As String = "DELETE FROM `{0}`.`{1}`"

        _errorMessage = String.Empty
        Try
            Dim SqlText As String = String.Format(EmptyTableFormat, MySqlConn.Database.ToLower, TableName.ToLower)
            Return ExecuteNonQuery(MySqlConn, SqlText)                          ' empty table
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return sqlOtherErr
        End Try
    End Function

    Public Shared Function TruncateTable(MySqlConn As MySqlConnection,
                                         TableName As String) As Integer

        ' clears data from a table
        ' 
        ' vars passed:
        '   TableName - name of table with data to clear
        '   MySqlConn - mysql connection to use
        ' 
        ' returns:
        '   NoErrors 
        '   sqlNoTableName - no table name
        '   sqlOtherErr - something went wrong

        ' TRUNCATE TABLE `<database>`.`<tablename>`)
        Const TruncateTableFormat As String = "TRUNCATE TABLE `{0}`.`{1}`"

        _errorMessage = String.Empty
        Try
            Dim SqlText As String = String.Format(TruncateTableFormat, MySqlConn.Database.ToLower, TableName.ToLower)
            Return ExecuteNonQuery(MySqlConn, SqlText)                          ' truncate table 
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return sqlOtherErr
        End Try
    End Function

#End Region

#Region " Drop Table "

    Public Shared Function DropTable(MySqlConn As MySqlConnection,
                                     TableName As String) As Integer

        ' drops (deletes) a data table
        '
        ' vars passed:
        '   MySqlConn - mysql connection to use
        '   TableName - table to delete
        '
        ' returns:
        '   >= 0 - no errors or Table did not exist
        '   <0 - SqlCommand.ExecuteNonQuery error value
        '   sqlNoTableName - no table name
        '   sqlOtherErr - other error in sql 
        '   sqlDropErr - other error in dropping table

        ' DROP TABLE IF EXISTS `<databasename>`.`<tablename>`
        Const DropTableFormat As String = "DROP TABLE IF EXISTS `{0}`.`{1}`"

        _errorMessage = String.Empty
        Try
            Dim SqlText = String.Format(DropTableFormat, MySqlConn.Database.ToLower, TableName.ToLower)     ' get sql text 
            Return ExecuteNonQuery(MySqlConn, SqlText, errNoTable, NoErrors)                                ' drop the table 
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return sqlDropErr
        End Try
    End Function

#End Region

#Region " Fill Table "

    Public Shared Function FillTable(Table As DataTable,
                                     MySqlDataAdapt As MySqlDataAdapter,
                                     Optional Params As List(Of MySqlParam) = Nothing,
                                     Optional ByVal DoClear As Boolean = True) As Integer

        ' fills the table by using MySqlDataAdapt.Fill(Table)
        ' 
        ' vars passed:
        '   Table - data table that will be filled
        '   MySqlDataAdapt - MySqlDataAdapter for table
        '   Params - list of parameters for MySqlDataAdapter
        '   DoClear - if true, clears the table before filling it
        '
        ' returns:
        '   >= 0 the number of data rows that will filled/loaded
        '   EaTools.Ea.teCouldNotClearErr - could not clear table
        '   < 0 - MySqlException.ErrorCode
        '   EaTools.Ea.teOtherErr - something went wrong

        Dim TryCount As Integer = 0
        _errorMessage = String.Empty
        Do
            Try
                If Params IsNot Nothing Then                                                                ' if got list of params
                    If MySqlDataAdapt.SelectCommand.Parameters.Count = 0 Then                               ' if no params in SelectCommand
                        For Each param In Params                                                            ' for each param in list
                            MySqlDataAdapt.SelectCommand.Parameters.AddWithValue(param.Name, param.Value)   ' add param to SelectCommand
                        Next
                    Else                                                                                    ' else got params in SelectCommand
                        For Each param In Params                                                            ' for each param in list
                            For i As Integer = 0 To MySqlDataAdapt.SelectCommand.Parameters.Count - 1       ' for each param in SelectCommand
                                If MySqlDataAdapt.SelectCommand.Parameters(i).ParameterName.ToUpper = param.Name.ToUpper Then ' if found param
                                    MySqlDataAdapt.SelectCommand.Parameters(i).Value = param.Value          ' set new param value
                                    Exit For                                                                ' exit for i loop
                                End If
                            Next
                        Next
                    End If
                End If
                If Table.Rows.Count > 0 AndAlso DoClear Then                                                ' if got rows, and want to clear
                    If SafeClear(Table) <> NoErrors Then                                                    ' if could not clear table
                        Return teCouldNotClearErr                                                           ' return error
                    End If
                End If
                Return MySqlDataAdapt.Fill(Table)                                                           ' fill table, return # of rows filled                
            Catch MySqlEx As MySqlException
                If MySqlEx.ErrorCode = errCannotOpen AndAlso TryCount < MaxTries Then
                    TryCount += 1
                    'My.Application.DoEvents()
                    'Debug.WriteLine("No DoEvents")
                    System.Threading.Thread.Sleep(EaSql.Ea.WaitMilliSeconds)
                Else
                    _errorMessage = String.Format("MySql Error while loading data table {0}{1}Error Code:{4}{1}Database: {2}{1}{3}",
                                                  Table.TableName, vbCrLf, MySqlDataAdapt.SelectCommand.Connection.DataSource, MySqlEx.Message, MySqlEx.ErrorCode)
                    Return MySqlEx.ErrorCode
                End If
            Catch ex As Exception
                _errorCode = teOtherErr
                _errorMessage = String.Format(efOther, ex.Message)
            End Try
        Loop
    End Function

#End Region

#Region " GetColumnNames "

    Public Shared Function GetColumnNames(MySqlConn As MySqlConnection,
                                          ChildTable As DataTable,
                                          TableNameInList As String) As String

        ' gets columns names in the actual designed table.  some of the tables in the datasets get columns from 
        ' queries spanning multiple tables, so some of the columns are not in the designed table.  the data set has
        ' only tables as they are actually designed.  The actual table columns are needed when moving non-linked rows out
        ' of a table
        '
        ' vars passed:
        '   ChildTable - table with non linked rows
        '   TableNameInList - table name to use in the column list
        '
        ' returns:
        '   IncludeTableName <> "": "TableNameInList.Column1, TableNameInList.Column2, ...
        '   IncludeTableName = "": "Column1, Column2, ..."
        '   ""
        '     - ChildTable does not exist in database
        '     - something went wrong

        Const ColNameIndex As Integer = 0   ' column name position in each row of clTable

        Try
            Dim infoTable As DataTable = TableInfo(MySqlConn, ChildTable.TableName)                 ' get info about child table
            If infoTable Is Nothing Then                                                            ' if no info
                Return String.Empty                                                                 ' return empty string
            End If
            Dim ColumnListStr As String = String.Empty

            For Each row As DataRow In infoTable.Rows                                               ' for each row in info table
                If ColumnListStr <> String.Empty Then                                               ' if not the first row
                    ColumnListStr &= ", "                                                           ' add comma and space
                End If
                If TableNameInList <> String.Empty Then                                             ' if got a TableNameInList 
                    ColumnListStr &= String.Format("{0}.{1}", TableNameInList, row(ColNameIndex))   ' format column list entry
                Else                                                                                ' else no TableNameInList
                    ColumnListStr &= row(ColNameIndex).ToString                                     ' add column name to list
                End If
            Next
            Return ColumnListStr
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            _errorCode = teOtherErr
            Return String.Empty
        End Try
    End Function

#End Region

#Region " Indexes "

#Region " Create Index "

    Public Shared Function CreateIndex(MySqlConn As MySqlConnection,
                                       TableName As String,
                                       IndexName As String,
                                       indexColsInfo As List(Of IndexColumnInfo),
                                       Unique As Boolean) As Integer

        ' adds a index to a table
        ' 
        ' vars passed:
        '   MySqlConn - mySql connection to use
        '   TableName - table name for index
        '   IndexName - name of index
        '   indexColsInfo - list of column names and types and sort order to be used in index        
        '   Unique - TRUE if index requires unique rows
        '
        ' returns: 
        '   >= 0 - no errors
        '   <0 - SqlCommand.ExecuteNonQuery error value
        '   sqlNoTableName - no table name 
        '   sqlNoIndexName - no index name
        '   sqlNoColumn - no columns
        '   sqlNoSort - no sort info
        '   sqlOtherErr - other error in sql 
        '   sqlCreateIndexErr - error in create index sql

        ' CREATE [UNIQUE] INDEX `<indexname>` ON `<tablename>` (`<ColumnName1>` [ASC|DESC], `<ColumnName2>` [ASC|DESC], ...)
        Const IndexFormat As String = "INDEX `{0}` ON `{1}` ("

        _errorMessage = String.Empty
        Try
            Dim SqlText As String = "CREATE "
            If Unique Then                                                              ' if unique rows in index
                SqlText &= "UNIQUE "                                                    ' add UNIQUE to sql text
            End If
            SqlText &= String.Format(IndexFormat, IndexName.ToLower, TableName.ToLower) ' add index's name and table name
            Dim colCount As Integer = 0
            For Each idxColInfo In indexColsInfo                                        ' for each column in index
                SqlText &= String.Format("`{0}`", idxColInfo.MySqlCol.ColumnName)       ' add column name
                If idxColInfo.SortASC Then                                              ' if ascending sort                    
                    SqlText &= " ASC"                                                   ' add sort direction
                Else                                                                    ' else descending sort
                    SqlText &= " DESC"                                                  ' add sort direction
                End If
                If colCount < indexColsInfo.Count - 1 Then                              ' if not the last column
                    SqlText &= ","                                                      ' add a space
                End If
                colCount += 1                                                           ' increment column counter
            Next
            SqlText &= ")"                                                              ' add ending bracket for column names
            Return ExecuteNonQuery(MySqlConn, SqlText)                                  ' create the index
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return sqlOtherErr
        End Try
    End Function

    Public Shared Function CreateIndex(MySqlConn As MySqlConnection,
                                       TableName As String,
                                       IndexName As String,
                                       IndexesView As DataView,
                                       iStart As Integer,
                                       iEnd As Integer) As Integer

        ' creates an index for a table
        '
        ' vars passed:
        '   MySqlConn - mySql connection to use
        '   TableName - name of table with index to create
        '   IndexName - name of index to create
        '   IndexesView - dataview of CurrentIndexes table, showing just indexes, no primary key data
        '   iStart - starting row index in IndexesView of Index to create
        '   iEnd - ending row index in IndexesView of Index to create
        '
        ' returns:
        '   NoErrors - no errors
        '   sqlNoColumnName  - no column name            
        '   sqlCreateIndexErr - error creating the index
        '   sqlOtherErr - something went wrong                    

        Const AscCollation As Integer = 1

        _errorMessage = String.Empty
        Try
            Dim indexCols As New List(Of IndexColumnInfo)
            Dim ColumnName As String
            Dim CollationInt As Integer
            Dim mCol As MySqlColumn
            Dim sortAsc As Boolean
            Dim Unique As Boolean
            Dim j As Integer = 0
            For i As Integer = iStart To iEnd                                                   ' for each row in index
                ColumnName = GcrcvAs(IndexesView(i).Row, cnColumnName, String.Empty)            ' get the column name
                If ColumnName = String.Empty Then                                               ' if no column name
                    _errorMessage = "No column name"
                    Return sqlNoColumnName                                                      ' return error
                End If
                mCol = New MySqlColumn(ColumnName, MySqlDataType.dtNone, False)                 ' get new my sql column, no data type needed                
                CollationInt = GcrcvAs(IndexesView(i).Row, cnCollation, AscCollation)           ' get the sort of the index column
                If CollationInt = AscCollation Then                                             ' if sort ascending
                    sortAsc = True                                                              ' set sort ascending as true
                Else                                                                            ' else sort descending
                    sortAsc = False                                                             ' set sort ascending as false
                End If
                indexCols.Add(New IndexColumnInfo(mCol, sortAsc))                               ' increment array index
            Next
            Unique = GcrcvAs(IndexesView(iStart).Row, cnUnique, False)                          ' see if index has unique rows
            Dim Err As Integer = CreateIndex(MySqlConn, TableName, IndexName, indexCols, Unique) ' create the index
            If Err < 0 Then                                                                     ' if error creating the index
                Return sqlCreateIndexErr                                                        ' return error
            End If
            Return NoErrors                                                                     ' if got here, then no errors
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return sqlOtherErr
        End Try
    End Function

    Public Shared Function CreateTableIndexes(MySqlConn As MySqlConnection,
                                              TableName As String,
                                              IndexesView As DataView) As Integer


        ' adds indexes to a table
        '
        ' note: uses table CurrentIndexes created in call to GetTableStructure
        '
        ' vars passed:
        '   MySqlConn - mySql connection to use
        '   TableName - name of table with indexes to create
        '   IndexesView - dataview of CurrentIndexes table, showing just indexes, no primary key data
        ' 
        ' returns:
        '   NoErrors - no errors
        '   sqlNoColumnName  - no column name            
        '   sqlCreateIndexErr - error creating the index
        '   sqlOtherErr - something went wrong

        _errorMessage = String.Empty
        Try
            Dim IndexName As String
            Dim NextIndexName As String
            Dim Err As Integer
            Dim iStart As Integer
            For i As Integer = 0 To IndexesView.Count - 1                                           ' for each index view row
                IndexName = GcrcvAs(IndexesView(i).Row, cnIndexName, String.Empty)                  ' get index name
                If IndexName = String.Empty Then                                                    ' if no index name
                    Return sqlCreateIndexErr                                                        ' return error
                End If
                If i = 0 Then                                                                       ' if the first index view row
                    iStart = i                                                                      ' set the i starting value
                End If
                If i = IndexesView.Count - 1 Then                                                   ' if the last index view row
                    Err = CreateIndex(MySqlConn, TableName, IndexName, IndexesView, iStart, i)      ' create table index
                    If Err <> NoErrors Then                                                         ' if error creating table index
                        Return Err                                                                  ' return the error
                    End If
                ElseIf i < IndexesView.Count - 1 Then                                               ' else if not the last index view row
                    NextIndexName = GcrcvAs(IndexesView(i + 1).Row, cnIndexName, String.Empty)      ' get the index name of the next row
                    If NextIndexName <> IndexName Then                                              ' if the next row's index name different
                        Err = CreateIndex(MySqlConn, TableName, IndexName, IndexesView, iStart, i)  ' create table index
                        If Err <> NoErrors Then                                                     ' if error creating table index
                            Return Err                                                              ' return error
                        End If
                        iStart = i + 1                                                              ' reset starting i to next row position
                    End If
                End If
            Next

            Return NoErrors         ' if got here, then no errors
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return sqlOtherErr
        End Try
    End Function

#End Region

#Region " Drop Index "

    Public Shared Function DropIndex(MySqlConn As MySqlConnection,
                                     TableName As String,
                                     IndexName As String) As Integer

        ' drops (deletes) a index on a table
        '
        ' vars passed:
        '   MySqlConn - mySql connection to use
        '   TableName - table with index
        '   IndexName - name of index to drop (delete)
        '
        ' returns:
        '   >= 0 - no errors 
        '   <0 - SqlCommand.ExecuteNonQuery error value
        '   sqlNoTableName - no table name
        '   sqlNoIndexName - no index name
        '   sqlOtherErr - other error in sql 
        '   sqlDropErr - other error in dropping index

        ' "DROP INDEX `<indexname>` ON `<tablename>`"
        Const DropIndexFormat As String = "DROP INDEX `{0}` ON `{1}`"

        _errorMessage = String.Empty
        Try
            If IndexExists(MySqlConn, TableName, IndexName) <> ExistsTypes.Yes Then                         ' if cannot find the index
                Return NoErrors                                                                             ' return no errors
            End If

            Dim SqlText As String = String.Format(DropIndexFormat, IndexName.ToLower, TableName.ToLower)    ' drop index sql text                
            Return ExecuteNonQuery(MySqlConn, SqlText, errNoTable, NoErrors)                                ' drop the index 
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return sqlDropErr
        End Try
    End Function

#End Region

#Region " Indexes "

    Public Shared Function Indexes(MySqlConn As MySqlConnection,
                                   TableName As String) As List(Of String)

        ' gets list index names for a table in a connection
        '
        ' vars passed:
        '   MySqlConn - mySql connection to use
        '   TableName - table name to for index
        '
        ' returns: 
        '   >= 0 - # of indexes for table
        '   -1 -
        '     - no database param in connection
        '     - error counting indexes
        '     - something went wong

        _errorMessage = String.Empty                                                                ' clear error message
        Try
            If DatabaseParamBlank(MySqlConn) Then                                                   ' if no database param in conn
                _errorMessage = emNoDatabaseParam
                Return Nothing                                                                      ' return nothing
            End If

            Dim objToFind As New MySqlInfo(MySqlInfo.MySqlTypesToFindTypes.Indexes, TableName)      ' get object to find info
            Dim exists As ExistsTypes = DoesItExist(MySqlConn, objToFind)                           ' see if indexes exits
            If exists = ExistsTypes.Yes OrElse exists = ExistsTypes.No Then                         ' if yes or no (no errors)
                Return CType(objToFind.ReaderData, List(Of String))                                 ' readerData is List (Of String)
            Else                                                                                    ' else got an error
                Return Nothing                                                                      ' return nothing
            End If
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return Nothing
        End Try
    End Function

#End Region

#Region " Index Exists "

    Public Shared Function IndexExists(MySqlConn As MySqlConnection,
                                       TableName As String,
                                       IndexName As String) As ExistsTypes

        ' checks to see if an index exists for a table in a connection
        '
        ' vars passed:
        '   MySqlConn - mySql connection to use
        '   TableName - table name to for index
        '   IndexName - name of index to find
        '
        ' returns:
        '   ExistsTypes.Yes - table contains the index
        '   ExistsTypes.No - table does not contain the index
        '   ExistsTypes.OverMaxTries - tried more than MaxTries to open connection
        '   ExistsTypes.GotError 
        '     - error getting MySqlInfo
        '     - MySqlConn is nothing
        '     - Table Is Nothing     
        '     - something went wrong

        _errorMessage = String.Empty                                                                        ' clear error message
        Try
            If DatabaseParamBlank(MySqlConn) Then                                                           ' if no database param in conn
                _errorMessage = emNoDatabaseParam
                Return ExistsTypes.GotError                                                                 ' return error
            End If

            Dim objToFind As New MySqlInfo(MySqlInfo.MySqlTypesToFindTypes.Index, IndexName, TableName)     ' get object to find info
            Return DoesItExist(MySqlConn, objToFind)                                                        ' see if index exists
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return ExistsTypes.GotError
        End Try
    End Function

#End Region

#Region " GetPrimaryKeyName "

    Public Shared Function GetPrimaryKeyName(MySqlConn As MySqlConnection,
                                             TableName As String) As String

        ' gets the primary key name for a table in a connection
        '
        ' vars passed:
        '   MySqlConn - mySql connection to use
        '   TableName - table name to for index        
        '
        ' returns:
        '   <> String.Empty - Name of the primary key
        '   String.Empty - 
        '     - table does not contain a primary key
        '     - tried more than MaxTries to open connection
        '     - error getting MySqlInfo
        '     - MySqlConn is nothing
        '     - Table Is Nothing     
        '     - something went wrong

        _errorMessage = String.Empty                                                                    ' clear error message
        Try
            If DatabaseParamBlank(MySqlConn) Then                                                       ' if no database in connection 
                _errorCode = sqlNoDatabase                                                              ' set error code
                Return String.Empty                                                                     ' return empty string
            End If

            Dim objToFind As New MySqlInfo(MySqlInfo.MySqlTypesToFindTypes.PrimaryKey, TableName)       ' get object to find info
            Dim exists As MySqlTools.ExistsTypes = DoesItExist(MySqlConn, objToFind)                       ' see if primary key exists
            If exists = ExistsTypes.Yes Then                                                            ' if primary key exists
                Return objToFind.NameToFind                                                             ' return primary key name
            Else                                                                                        ' else no primary key or error
                Return String.Empty                                                                     ' return empty string
            End If
        Catch ex As Exception
            _errorCode = sqlOtherErr
            Return String.Empty
        End Try
    End Function

#End Region

#End Region

#Region " Rename Table "

    Public Shared Function RenameTable(MySqlConn As MySqlConnection,
                                       FromTableName As String,
                                       ToTableName As String) As Integer

        ' renames a table
        '   
        ' note: any PrimaryKey or indexes will be lost
        ' 
        ' vars passed:
        '   MySqlConn - mysql connection to use
        '   FromTableName - old table name
        '   ToTableName - new table name
        '
        ' returns:
        '   >= 0 - no errors
        '   <0 - SqlCommand.ExecuteNonQuery error value
        '   sqlNoTableName - from or to table name not set
        '   sqlOtherErr- other error in sql 
        '   sqlMakeTableErr - error in make table sql 
        '   sqlDropErr - other error in dropping table
        '   sqlRenameTableErr - other error in rename

        ' RENAME TABLE  `<fromtablename>` TO  `<totablename>`
        Const RenameFormat As String = "RENAME TABLE `{0}` TO `{1}`"

        _errorMessage = String.Empty
        Try
            If DropTable(MySqlConn, ToTableName) < 0 Then                                                   ' if error dropping ToTable
                _errorMessage = String.Format("Error dropping table: {0}", ToTableName)
                Return sqlRenameTableErr                                                                    ' return error
            End If
            Dim SqlText As String = String.Format(RenameFormat, FromTableName.ToLower, ToTableName.ToLower) ' get sql command text                
            Return ExecuteNonQuery(MySqlConn, SqlText)                                                      ' rename table
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return sqlRenameTableErr
        End Try
    End Function

#End Region

#Region " Row Count "

    Public Shared Function RowCount(MySqlConn As MySqlConnection,
                                    TableName As String) As Integer

        ' gets the # or rows in a table
        '
        ' vars passed:
        '   MySqlConn - MySql connection to use
        '   TableName - name of the table
        '
        ' returns 
        '   >=0 - number of rows in table
        '   sqlNoTableErr - table not found
        '   sqlOtherErr - other error in sql             
        '   <0 - SqlCommand.ExecuteNonQuery error value

        ' SELECT COUNT(*) FROM `<tablename>`
        Const RowCountFormat As String = "SELECT COUNT(*) FROM `{0}`"

        _errorMessage = String.Empty
        Try
            If TableExists(MySqlConn, TableName) <> ExistsTypes.Yes Then                    ' if no table
                Return sqlTableNotFound                                                     ' return error
            End If
            Dim SqlText As String = String.Format(RowCountFormat, TableName.ToLower)        ' get sql text
            Return ExecuteScalar(MySqlConn, SqlText)                                        ' return # of rows in table
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return sqlOtherErr
        End Try
    End Function

#End Region

#Region " Table Exists "

    Public Shared Function TableExists(MySqlConn As MySqlConnection,
                                       TableName As String) As ExistsTypes

        ' checks to see if a table exits in a database
        '
        ' vars passed:
        '   MySqlConn - mySql connection to use
        '   TableName - table name to find
        '
        ' returns:
        '   ExistsTypes.Yes - table found
        '   ExistsTypes.No - table not found
        '   ExistsTypes.OverMaxTries - tried more than MaxTries to open connection
        '   ExistsTypes.GotError 
        '     - error getting MySqlInfo
        '     - MySqlConn is nothing
        '     - Table Is Nothing     
        '     - something went wrong

        _errorMessage = String.Empty
        Try
            Dim objToFind As New MySqlInfo(MySqlInfo.MySqlTypesToFindTypes.Table, String.Empty, TableName, MySqlConn.Database)  ' get object to find info
            Return DoesItExist(MySqlConn, objToFind)                                                                            ' see if table exists
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return ExistsTypes.GotError
        End Try
    End Function

#End Region

#Region " TableInfo "

    Public Shared Function TableInfo(MySqlConn As MySqlConnection,
                                     TableName As String) As DataTable

        ' checks to see if a procedure exits in a connection
        '
        ' vars passed:
        '   MySqlConn - connection to use
        '   TableName - name of table to get info for
        '
        ' returns:
        '   DataTable - data table with info for table TableName
        '   Nothing - 
        '     - could not open connection
        '     - over max tries
        '     - Table not found
        '     - could not get table info
        '     - something went wrong

        _errorMessage = String.Empty                                                                ' clear error message
        Try
            Dim objToFind As New MySqlInfo(MySqlInfo.MySqlTypesToFindTypes.TableInfo, TableName)    ' get object to find info
            Dim exists As ExistsTypes = DoesItExist(MySqlConn, objToFind)                           ' see if table info exists
            If exists = ExistsTypes.Yes Then
                Return CType(objToFind.ReaderData, DataTable)
            Else
                Return Nothing
            End If
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return Nothing
        End Try
    End Function

#End Region

#End Region

#Region " Column Actions "

#Region " Add Column "

    Public Shared Function AddAutoNumberColumn(MySqlConn As MySqlConnection,
                                               TableName As String,
                                               Column As MySqlColumn,
                                               Optional ByVal AutoNumStart As Integer = 1,
                                               Optional ByVal AutoNumIncrement As Integer = 1) As Integer

        ' adds a AutoNumber column to a table.  AutoNumber column is not key column
        '
        ' vars passed:
        '   MySqlConn - mysql connection to use
        '   TableName - table to alter
        '   Column - column to alter
        '   AutoNumStart - starting auto number value
        '   AutoNumIncrement - auto number increment value
        '
        ' returns:
        '   >= 0 - no errors
        '   <0 - SqlCommand.ExecuteNonQuery error value
        '   sqlNoTableName - no table name 
        '   sqlNoKeyName - no key name
        '   sqlNoColumn - no columns
        '   sqlVersionErr - invalid mySql version
        '   sqlErr- other error in sql 
        '   sqlInvalidAlter - invalid alter option (not atAddColumn or atDropColumn)
        '   sqlAlterErr  - error in alter sql 
        '   sqlOtherErr - something went wrong

        ' MySql 5
        ' ALTER TABLE `<tablename>` ADD COLUMN `<columnname>` COUNTER(<Start>,<Increment>)
        Const AddAutoNumFormat5 As String = "ALTER TABLE `{0}` ADD COLUMN `{1}` COUNTER({2},{3})"

        ' MySql 8 requires AutoInc columns to be primary key
        ' ALTER TABLE `<tablename>` ADD COLUMN `<columnname>` INT UNSIGNED NOT NULL AUTO_INCREMENT, ADD PRIMARY KEY (`<columnname>`)
        Const AddAutoNumFormat8 As String = "ALTER TABLE `{0}` ADD COLUMN `{1}` INT UNSIGNED NOT NULL AUTO_INCREMENT, ADD PRIMARY KEY (`{1}`)"

        Try
            Dim SqlText As String
            GetMySqlVersion(MySqlConn)

            Dim pkName As String = GetPrimaryKeyName(MySqlConn, TableName)                          ' get the primary key name
            If pkName <> String.Empty Then                                                          ' if got primary key name
                Dim err As Integer = DropKey(MySqlConn, TableName, pkName)                          ' drop the primary key name
                If err < 0 Then                                                                     ' if got err dropping primary key
                    Return err                                                                      ' return the err
                End If
            End If

            If MySqlVer.Major = 5 Then                                                              ' if mySql version 5
                SqlText = String.Format(AddAutoNumFormat5, TableName, Column.ColumnName, AutoNumStart, AutoNumIncrement) ' get sql command text, ver 5 format
            ElseIf MySqlVer.Major = 8 Then                                                          ' else if mySql version 8
                SqlText = String.Format(AddAutoNumFormat8, TableName, Column.ColumnName)            ' get sql command text, ver 8 format
            Else                                                                                    ' else invalid version
                _errorMessage = emInvalidVersion
                Return sqlVersionErr                                                                ' return error
            End If

            Return ExecuteNonQuery(MySqlConn, SqlText)                                              ' adds auto number column 
        Catch ex As Exception
            Return sqlOtherErr
        End Try
    End Function

    Public Shared Function AddColumn(MySqlConn As MySqlConnection,
                                     TableName As String,
                                     ColToAdd As MySqlColumn) As Integer

        ' adds a column to a table
        '
        ' vars passed:
        '   MySqlConn - mysql connection to use
        '   aTableName - name of table to add column to
        '   ColToAdd - column type to add
        '
        ' returns:
        '   >= 0 - no errors
        '   sqlNoTableName - no table name err
        '   sqlNoKeyName - no key name err
        '   sqlNoColumnName - no column name err
        '   sqlOtherErr - other sql error
        '   sqlInvalidAlter - invalid ALTER option err
        '   sqlAlterErr - sql ALTER err            
        '   <0 - Invalid SQL command err

        _errorMessage = String.Empty
        Try
            ' make new id column
            Dim AcErr As Integer = AlterTableColumn(MySqlConn, TableName, ColToAdd, AlterTableTypes.atAddColumn)
            Select Case AcErr
                Case Is >= 0
                    ' do nothing 
                Case sqlNoTableName
                    _errorMessage = String.Format("No table name in AddColumn, TableName = ""{0}"".", TableName)
                Case sqlNoKeyName
                    _errorMessage = String.Format("No key name in AddColumn, ColumnName = ""{0}"".", ColToAdd.ColumnName)
                Case sqlNoColumnName
                    _errorMessage = String.Format("No column name in AddColumn, ColumnName = ""{0}"".", ColToAdd.ColumnName)
                Case sqlErr
                    _errorMessage = String.Format("Other SQL error in AddColumn, TableName = ""{0}"", ColumnName = ""{1}"".", TableName, ColToAdd.ColumnName)
                Case sqlInvalidAlter
                    _errorMessage = String.Format("Invalid ALTER option in AddColumn, TableName = ""{0}"", ColumnName = ""{1}"".", TableName, ColToAdd.ColumnName)
                Case sqlAlterErr
                    _errorMessage = String.Format("ALTER error in AddColumn, TableName = ""{0}"", ColumnName = ""{1}"".", TableName, ColToAdd.ColumnName)
                Case Else
                    _errorMessage = String.Format("Invalid SQL in AddColumn, TableName = ""{0}"", ColumnName = ""{1}"".", TableName, ColToAdd.ColumnName)
            End Select
            Return AcErr
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return sqlOtherErr
        End Try
    End Function

#End Region

#Region " Alter Table Column "

    Public Shared Function AlterTableColumn(MySqlConn As MySqlConnection,
                                            TableName As String,
                                            Column As MySqlColumn,
                                            AlterType As AlterTableTypes,
                                            Optional KeyName As String = "") As Integer

        ' alters a data table, 
        '   adding or dropping a column
        '   adding key (one field), not multi field
        '   dropping key
        '
        ' vars passed:
        '   MySqlConn - mysql connection to use
        '   TableName - table to alter
        '   aColumn - column to alter
        '   AlterType - 
        '     atAddColumn (add column)
        '     atDropColumn (drop column)
        '     atAddKey (add key) 
        '     atDropKey (drop key)            
        '   KeyName - name of primary key (required if AlterType is atAddKey or atDropKey)
        '
        ' returns:
        '   >= 0 - no errors
        '   <0 - SqlCommand.ExecuteNonQuery error value
        '   sqlNoTableName - no table name 
        '   sqlNoKeyName - no key name
        '   sqlNoColumn - no columns
        '   sqlOtherErr- other error in sql 
        '   sqlInvalidAlter - invalid alter option (not atAddColumn or atDropColumn)
        '   sqlAlterErr  - error in alter sql 

        Try
            GetMySqlVersion(MySqlConn)                                                  ' get my sql version

            ' if version 5, and got a alter key, must have a key name
            If MySqlVer.Major = 5 AndAlso
                    ((AlterType = AlterTableTypes.atAddKey) OrElse (AlterType = AlterTableTypes.atDropKey)) Then
                If KeyName = String.Empty Then                                          ' if no key name
                    Return sqlNoKeyName                                                 ' return error
                End If
                If KeyName.IndexOfAny(CType(" #", Char())) >= 0 Then                    ' if key name has space or special chars
                    KeyName = String.Format("[{0}]", KeyName)                           ' put square brackets around KeyName
                End If
            End If

            Dim SqlText As String
            Select Case AlterType
                Case AlterTableTypes.atAddColumn

                    ' ALTER TABLE `<tablename>` ADD COLUMN `<ColumnName>` <ColumnType>
                    ' ALTER TABLE `<tablename>` ADD COLUMN `<ColumnName>` VARCHAR(<Size>)                    
                    SqlText = String.Format("ALTER TABLE `{0}` ADD COLUMN `{1}` {2}", TableName.ToLower, Column.ColumnName, Column.SqlDataType)
                    If Column.DataType Is MySqlDataType.dtVarChar Then                  ' if a vChar column
                        SqlText &= String.Format("({0})", CStr(Column.Length))          ' need to add "(size)"
                    End If

                Case AlterTableTypes.atDropColumn

                    ' ALTER TABLE `<tablename>` DROP COLUMN `<ColumnName>`
                    SqlText = String.Format("ALTER TABLE `{0}` DROP COLUMN `{1}`", TableName.ToLower, Column.ColumnName)

                Case AlterTableTypes.atAddKey

                    If MySqlVer.Major = 5 Then
                        ' ALTER TABLE `<tablename>` ADD CONSTRAINT <KeyName> PRIMARY KEY (`<ColumnName>`)
                        SqlText = String.Format("ALTER TABLE `{0}` ADD CONSTRAINT {1} PRIMARY KEY (`{2}`)", TableName.ToLower, KeyName, Column.ColumnName)
                    ElseIf MySqlVer.Major = 8 Then
                        ' ALTER TABLE `<tablename>` ADD CONSTRAINT PRIMARY KEY 
                        SqlText = String.Format("ALTER TABLE `{0}` ADD CONSTRAINT PRIMARY KEY", TableName.ToLower)
                    Else
                        _errorMessage = emInvalidVersion
                        Return sqlVersionErr
                    End If

                Case AlterTableTypes.atDropKey

                    If MySqlVer.Major = 5 Then
                        ' ALTER TABLE `<tablename>` DROP CONSTRAINT <KeyName> 
                        SqlText = String.Format("ALTER TABLE `{0}` DROP CONSTRAINT {1}", TableName.ToLower, KeyName)
                    ElseIf MySqlVer.Major = 8 Then
                        ' ALTER TABLE `<tablename>` DROP PRIMARY KEY
                        SqlText = String.Format("ALTER TABLE `{0}` DROP PRIMARY KEY", TableName.ToLower)
                    Else
                        _errorMessage = emInvalidVersion
                        Return sqlVersionErr
                    End If

                Case Else
                    Return sqlInvalidAlter
            End Select
            Return ExecuteNonQuery(MySqlConn, SqlText)
        Catch ex As Exception
            Return sqlOtherErr
        End Try
    End Function

#End Region

#Region " Alter Column Type "

    Public Shared Function AlterColumnType(MySqlConn As MySqlConnection,
                                           TableName As String,
                                           FromColumn As MySqlColumn,
                                           ToColumn As MySqlColumn) As Integer

        ' alters a column, changing the data type
        ' 
        ' note: this function DOES NOT change the column name, just the data type
        '
        ' vars passed:
        '   MySqlConn - mysql connection to use
        '   TableName - table to alter
        '   FromColumn - column to alter
        '   ToColumn - column to change to
        '
        ' returns:
        '   >= 0 - no errors
        '   <0 - SqlCommand.ExecuteNonQuery error value
        '   sqlVersionErr - invalid mySql version
        '   sqlErr- other error in sql 
        '   sqlInvalidAlter - invalid alter option (not atAddColumn or atDropColumn)
        '   sqlAlterErr  - error in alter sql 
        '   sqlOtherErr - something went wrong

        ' MySql 5.x
        ' ALTER TABLE `<tablename>` ALTER COLUMN `<ColumnName>` <ColumnType>
        ' ALTER TABLE `<tablename>` ALTER COLUMN `<ColumnName>` VARCHAR(<Size>)
        Const AlterColumnFormat5 As String = "ALTER TABLE `{0}` ALTER COLUMN `{1}` {2}"

        ' MySql 8.x
        ' ALTER TABLE `<tablename>` MODIFY COLUMN `<ColumnName>` <ColumnType>
        ' ALTER TABLE `<tablename>` MODIFY COLUMN `<ColumnName>` VARCHAR(<Size>)
        Const AlterColumnFormat8 As String = "ALTER TABLE `{0}` MODIFY `{1}` {2}"

        Try
            Dim SqlText As String
            GetMySqlVersion(MySqlConn)                                          ' get mySql version
            If MySqlVer.Major = 5 Then                                          ' if version 5, then use version 5 format
                SqlText = String.Format(AlterColumnFormat5, TableName.ToLower, FromColumn.ColumnName, ToColumn.SqlDataType.ToString.ToUpper)
            ElseIf MySqlVer.Major = 8 Then                                      ' else version 8, then use version 8 format
                SqlText = String.Format(AlterColumnFormat8, TableName.ToLower, FromColumn.ColumnName, ToColumn.SqlDataType.ToString.ToUpper)
            Else                                                                ' else invalid mySql version
                _errorMessage = emInvalidVersion
                Return sqlVersionErr                                            ' exit now
            End If
            If ToColumn.DataType Is MySqlDataType.dtVarChar Then                ' if to a VARCHAR column type
                SqlText += String.Format("({0})", CStr(ToColumn.Length))        ' add length
            End If
            Return ExecuteNonQuery(MySqlConn, SqlText)                          ' alter column 
        Catch ex As Exception
            Return sqlOtherErr
        End Try
    End Function

#End Region

#Region " Column Exists "

    Public Shared Function ColumnExists(Table As DataTable,
                                        ColumnName As String) As ExistsTypes

        ' sees if a column exists in a table
        '
        ' vars passed:
        '   Table - data table to check
        '   ColumnName - name of column to look for
        '
        ' returns:
        '   ExistsTypes.Yes - table contains the column 
        '   ExistsTypes.No - Table does not contain the column
        '   ExistsTypes.GotError 
        '     - Table Is Nothing     
        '     - something went wrong

        Try
            If Table.Columns.Contains(ColumnName) Then
                Return ExistsTypes.Yes
            Else
                Return ExistsTypes.No
            End If
        Catch ex As Exception
            Return ExistsTypes.GotError
        End Try
    End Function

    Public Shared Function ColumnExists(MySqlConn As MySqlConnection,
                                        TableName As String,
                                        ColumnName As String) As ExistsTypes

        ' sees if a column exists in a table.  Use this function if the table definition is not in the dataset for the table.  
        ' this version tries to perform a SELECT SQL query for the table and column.  The query will cause an 
        ' MySql.Data.MySqlClient.MySqlException exception if the table or column does not exist.  This exception is handled 
        ' here, and FALSE is returned.  If there is no exception thrown, then both the table and column exist, and TRUE is returned.  
        '
        ' vars passed:
        '   MySqlConn - mysql connection to use
        '   TableName - name of data table to check
        '   ColumnName - name of column to look for
        '
        ' returns:
        '   ExistsTypes.Yes - table contains the column 
        '   ExistsTypes.No - Table does not contain the column
        '   ExistsTypes.OverMaxTries - tried more than MaxTries to open connection
        '   ExistsTypes.GorError - 
        '     - error getting dbMySqlInfo
        '     - Table Is Nothing     
        '     - something went wrong

        _errorMessage = String.Empty                                                                        ' clear error message
        Try
            Dim objToFind As New MySqlInfo(MySqlInfo.MySqlTypesToFindTypes.Column, ColumnName, TableName)   ' get object to find info
            Return DoesItExist(MySqlConn, objToFind, errNoColumn)                                           ' see if column exists
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return ExistsTypes.GotError
        End Try
    End Function

#End Region

#Region " Column Select String "

    Public Shared Function ColumnSelectString(ColumnName As String,
                                              Value As Object,
                                              Optional TableName As String = "",
                                              Optional Op As String = "=",
                                              Optional UseSingleQuotes As Boolean = True) As String

        ' creates the selection string for one column
        '
        ' vars passed:
        '   ColumnName - Name of column for selection string
        '   Value - value for column
        '   TableName - if not "", then return `TableName`.`ColumnName` ...
        '   Op - operator to use in selection
        '     valid values are "=", "<", ">", "<=", ">=", "<>"
        '       "IN", "LIKE" - when aValue is a string
        '   UseSingleQuotes - TRUE if want single quotes around Value text
        '     
        ' returns:
        '   if Value is Nothing:
        '       "`ColumnName` Is Null"
        '   if Value is a boolean:
        '       "`ColumnName` = TRUE" or "`ColumnName` = FALSE" 
        '       (no single quotes for boolean value)
        '   if Value is a date:
        '       "`ColumnName` op #yyyy-mm-dd#" 
        '       where op is the operator
        '   if Value is not a boolean
        '       "`ColumnName` op 'XXX'" or "`ColumnName` Is Null"
        '       where op is the operator

        Dim ValueText As String = String.Empty

        Try
            If Value IsNot Nothing Then
                If TypeOf Value Is Date Then
                    ValueText = CDate(Value).ToString(dfShortDate)
                Else
                    ValueText = Value.ToString
                End If
            End If
        Catch ex As Exception
            ValueText = String.Empty
        End Try

        Op = Op.ToUpper()
        Select Case Op
            ' normal comparison operators
            Case "=", "<", ">", "<=", ">=", "<>"
                ' do nothing

                ' if an operator for just strings
            Case "IN", "LIKE"
                Try
                    If Not (Value Is Nothing) Then              ' if got a value
                        If Not (TypeOf Value Is String) Then    ' and value is not a string
                            Op = "="                            ' then use default operator
                        End If
                    Else                                        ' else no value
                        Op = "="                                ' then use default operator
                    End If
                Catch ex As Exception
                    Op = "="
                End Try

            Case Else
                Op = "="
        End Select

        Dim ColumnNameText As String = String.Empty
        If TableName <> String.Empty Then                           ' if got a table name
            ColumnNameText = String.Format("`{0}`.", TableName)     ' start with table name .
        End If
        ColumnNameText &= String.Format("`{0}`", ColumnName)        ' add in column name

        If TypeOf Value Is Boolean Then
            Return String.Format("{0} = {1}", ColumnNameText, ValueText)
        Else
            If ValueText = String.Empty Then
                If Op = "<>" Then
                    Return ColumnNameText & " Is Not Null"
                Else
                    Return ColumnNameText & " Is Null"
                End If
            Else
                If TypeOf Value Is Date Then
                    Return String.Format("{0} {1} '{2}'", ColumnNameText, Op, ValueText)
                ElseIf TypeOf Value Is String Then
                    If UseSingleQuotes Then
                        Return String.Format("{0} {1} '{2}'", ColumnNameText, Op, ValueText.Replace("'", "''"))
                    Else
                        Return String.Format("{0} {1} {2}", ColumnNameText, Op, ValueText.Replace("'", "''"))
                    End If
                Else
                    Return String.Format("{0} {1} {2}", ColumnNameText, Op, ValueText)
                End If
            End If
        End If
    End Function

#End Region

#Region " Drop Column "

    Public Shared Function DropColumn(MySqlConn As MySqlConnection,
                                      TableName As String,
                                      ColumnName As String) As Integer

        ' drops a column from a table 
        '
        ' vars passed:
        '   MySqlConn - MySql connection to use
        '   TableName - table to alter
        '   ColumnName - column name to drop
        '
        ' returns
        '   >= 0 - no errors
        '   <0 - SqlCommand.ExecuteNonQuery error value
        '   sqlNoTableName - no table name 
        '   sqlErr- other error in sql 
        '   sqlAlterErr  - error in delete sql 

        ' ALTER TABLE `tablename` DROP COLUMN `columnName`
        Const DropColFormat As String = "ALTER TABLE `{0}` DROP COLUMN `{1}`"
        Try
            Dim colExists As ExistsTypes = ColumnExists(MySqlConn, TableName, ColumnName)               ' see if column exists
            If colExists = ExistsTypes.GotError Then                                                    ' if got error
                Return sqlOtherErr                                                                      ' return error
            ElseIf colExists = ExistsTypes.No Then                                                      ' if column not found
                Return NoErrors                                                                         ' return no errors, no need to drop
            Else                                                                                        ' else column exists
                Dim SqlText As String = String.Format(DropColFormat, TableName.ToLower, ColumnName)     ' get drop command
                Return ExecuteNonQuery(MySqlConn, SqlText)                                              ' drop column
            End If
        Catch ex As Exception
            Return sqlAlterErr
        End Try
    End Function

#End Region

#Region " Rename Column "

    Public Shared Function RenameColumn(MySqlConn As MySqlConnection,
                                        TableName As String,
                                        FromColumnName As String,
                                        ToColumn As MySqlColumn) As Integer

        ' renames a column by 
        '   1) adding a new column (ToColumn)
        '   2) copying all data from FromColumn to ToColumn
        '   3) deleting FromColumn
        '
        ' NOTE: do not rename a column in a Primary Key or an Index
        '
        ' vars passed:
        '   MySqlConn - mysql connection to use
        '   TableName - name of table with column to rename
        '   FromColumn - column to be renamed
        '   ToColumn - new column 
        '
        ' returns:
        '   >= 0 - no errors
        '   <0 - SqlCommand.ExecuteNonQuery error value
        '   sqlNoTableName - no table name 
        '   sqlNoKeyName - no key name
        '   sqlNoColumn - no columns
        '   sqlErr- other error in sql 
        '   sqlInvalidAlter - invalid alter option (not atAddColumn or atDropColumn)
        '   sqlAlterErr  - error in alter sql 
        '   sqlRenameTableErr - something went wrong

        ' ALTER TABLE `<tableName>` CHANGE `<oldcolname>` `<newcolname>` datatype
        ' ALTER TABLE `<tableName>` CHANGE `<oldcolname>` `<newcolname>` datatype(length);
        Const ChangeColNameFormat As String = "ALTER TABLE `{0}` CHANGE `{1}` `{2}` {3}"

        Try
            Dim ToColTypeText As String = ToColumn.SqlDataType.ToUpper                                  ' get column name and data type
            If ToColumn.DataType Is MySqlDataType.dtVarChar Then                                        ' if a varChar column
                ToColTypeText &= String.Format("({0})", CStr(ToColumn.Length))                          ' add in size "(XX)"
            End If
            Dim SqlText As String = String.Format(ChangeColNameFormat, TableName.ToLower, FromColumnName, ToColumn.ColumnName, ToColTypeText)
            Return ExecuteNonQuery(MySqlConn, SqlText)                                                  ' change column type
        Catch ex As Exception
            Return sqlRenameTableErr
        End Try
    End Function

#End Region

#End Region

#Region " Row Actions "

#Region " Add/Insert Row via SQL "

    Public Shared Function AddRowViaSql(MySqlConn As MySqlConnection,
                                        TableName As String,
                                        row As DataRow) As Integer

        ' inserts a new row into a history table via sql.  This way the table does not 
        ' need to be loaded for a new row to be inserted.  
        '
        ' vars passed:
        '   MySqlConn - mysql connection to use
        '   TableName - name of table to insert into
        '   row - history data row to be inserted        
        '
        ' returns:
        '   1 - row was added        
        '   0 - no errors, but no row added
        '   <0 - SqlCommand.ExecuteNonQuery error value
        '   sqlErr - other error in sql 

        Try
            _errorMessage = String.Empty                                                                ' empty error message
            ' set starting values  
            Dim InsertInto As String = String.Format("INSERT INTO {0} (", TableName)
            Dim Values As String = "VALUES ("

            Dim c As Integer
            For c = 0 To row.Table.Columns.Count - 1                                                    ' for each column in row's table
                If Not row.IsNull(c) Then                                                               ' if got a value
                    AddColumnParamsToInsert(InsertInto, Values, row, row.Table.Columns(c).ColumnName)   ' add SQL text to InsertInto and Values 
                End If
            Next

            InsertInto &= ") "                                                                          ' add trailing ")", add a space after
            Values &= ")"                                                                               ' add trailing ")"

            Return ExecuteNonQuery(MySqlConn, InsertInto & Values)                                      ' do the add via SQL
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return sqlOtherErr
        End Try
    End Function

    Public Shared Function AddRowViaSql(MySqlConn As MySqlConnection,
                                        row As DataRow) As Integer

        ' inserts a new row into a history table via sql.  This way the table does not need to be loaded for a new row to be inserted.  
        '
        ' vars passed:
        '   MySqlConn - mysql connection to use        
        '   row - history data row to be inserted        
        '
        ' returns:
        '   1 - row was added        
        '   0 - no errors, but no row added
        '   <0 - SqlCommand.ExecuteNonQuery error value
        '   sqlErr - other error in sql 

        Try
            Return AddRowViaSql(MySqlConn, row.Table.TableName, row)
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return sqlOtherErr
        End Try
    End Function

    Private Shared Sub AddColumnParamsToInsert(ByRef InsertStr As String,
                                               ByRef ValueStr As String,
                                               row As DataRow,
                                               ColumnName As String)

        ' updated the Insert string and Values string with the column name and value
        ' When combined, the Insert and Value strings make an INSERT SQL statement
        ' 
        ' NOTE: InsertStr and ValueStr are passed ByRef
        '
        ' vars passed:
        '   InsertStr - ByRef - insert part of the INSERT SQL statement
        '   ValueStr - ByRef - values part of the INSERT SQL statement
        '   row - datarow with a value in column ColumnName
        '   ColumnName - name of a column to include in the INSERT SQL statement

        Try
            _errorMessage = String.Empty                                                                ' clear error message
            If row.IsNull(ColumnName) Then                                                              ' if no value in column
                Return                                                                                  ' exit now, do nothing
            End If

            If Not InsertStr.EndsWith("(") Then                                                         ' if not the first entry
                InsertStr &= ", "                                                                       ' add a comma separator
                ValueStr &= ", "
            End If
            InsertStr &= ColumnName                                                                     ' add in value column name
            If TypeOf row(ColumnName) Is Date Then                                                      ' if a date value
                ValueStr &= String.Format("'{0}'", CDate(row(ColumnName)).ToString(dfReallyLongDate))   ' add in value, enclosed with #
            ElseIf TypeOf row(ColumnName) Is String Then                                                ' else if a string
                ' add in value, enclosed with '.  also, if text has a ', then convert to ''
                ValueStr &= String.Format("'{0}'", row(ColumnName).ToString.Replace("'", "''"))
            Else                                                                                        ' else a regular value
                ValueStr &= row(ColumnName).ToString                                                    ' add in value 
            End If
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
        End Try
    End Sub

    Public Shared Function InsertViaSql(MySqlConn As MySqlConnection,
                                        InsertSqlText As String,
                                        Optional params As List(Of MySqlParam) = Nothing) As Integer

        ' Inserts data into a table via a SQL command.  Makes sure sql command starts with "INSERT INTO ", 
        '   and then calls ExecuteNonQuery to do the query.  
        '
        ' vars passed:
        '   MySqlConn - mysql connection to use
        '   InsertSqlText - SqlText to use
        '   params - list of parameter names and values for query               
        '
        ' returns:
        '   >= 0 - # of rows inserted
        '   sqlErr - other error in sql 
        '   sqlInvalidCommand - invalid sql command
        '   teOtherErr - other error

        Try
            _errorMessage = String.Empty                                            ' clear error message
            If Not InsertSqlText.ToUpper.StartsWith("INSERT INTO".ToUpper) Then
                _errorMessage = String.Format("The Insert SQL command does not start with ""INSERT INTO"".  SQL Command Text:{0}{1}", vbCrLf, InsertSqlText)
                Return sqlInvalidCommand
            End If

            Return ExecuteNonQuery(MySqlConn, InsertSqlText, , , params)            ' do the insert via SQL, and return the value
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return sqlErr
        End Try
    End Function

#End Region

#Region " Delete Row via SQL "

    Public Shared Function DeleteRowViaSQL(MySqlConn As MySqlConnection,
                                           TableName As String,
                                           row As DataRow) As Integer

        ' deletes a row from a table using SQL.  Note, the table must be keyed
        '
        ' vars passed:
        '   MySqlConn - mysql connection to use
        '   TableName - name of table to delete row from
        '   row - row with key values matching row to be deleted        
        '
        ' returns:
        '   >= 0 - # of rows deleted
        '   sqlNoKeyName - no key name, no row deleted
        '   <0 - SqlCommand.ExecuteNonQuery error value        
        '   sqlOtherErr - something went wrong

        Try
            _errorMessage = String.Empty                                            ' clear error message
            If row.Table.PrimaryKey.Length = 0 Then                                 ' if no key field
                Return sqlNoKeyName                                                 ' then return no rows deleted
            End If

            ' set the first line of the DELETE command 
            Dim DeleteSQL As String = "DELETE "
            Dim k As Integer = 0
            For Each KeyCol As DataColumn In row.Table.PrimaryKey                   ' for each key column 
                If k > 0 Then                                                       ' if not the first key column
                    DeleteSQL &= ", "                                               ' add a comma and a space
                End If
                DeleteSQL &= String.Format("{0}.{1}", TableName, KeyCol.ColumnName) ' add the Table.KeyColumnName
                k += 1                                                              ' increment key column counter
            Next
            DeleteSQL &= String.Format(" FROM {0} WHERE ", TableName)               ' add the 2nd line, FROM Table, and start 3rd line
            k = 0                                                                   ' reset key column counter (used as param count)
            For Each KeyCol As DataColumn In row.Table.PrimaryKey                   ' for each key column
                ' add the WHERE criteria or each key column
                DeleteSQL &= EaSql.Sql.SqlWhereColumnValueString(k, TableName, KeyCol.ColumnName, row(KeyCol), , , True)
                k += 1                                                              ' increment key column counter
            Next

            Return ExecuteNonQuery(MySqlConn, DeleteSQL)                            ' delete the row
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return sqlOtherErr
        End Try
    End Function

    Public Shared Function DeleteRowViaSQL(MySqlConn As MySqlConnection,
                                           row As DataRow) As Integer

        ' deletes a row from a table using SQL.  Note, the table must be keyed
        '
        ' vars passed:
        '   MySqlConn - mysql connection to use
        '   row - row with key values matching row to be deleted        
        '
        ' returns:
        '   >= 0 - # of rows deleted
        '   <0 - SqlCommand.ExecuteNonQuery error value        
        '   sqlOtherErr - something went wrong

        Try
            Return DeleteRowViaSQL(MySqlConn, row.Table.TableName, row)
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return sqlOtherErr
        End Try
    End Function

    Public Shared Function DeleteRowViaSQL(MySqlConn As MySqlConnection,
                                           Table As DataTable,
                                           keyValues As List(Of Object)) As Integer

        ' deletes a row from a table using SQL.  Note, the table must be keyed
        '
        ' vars passed:
        '   MySqlConn - mysql connection to use        
        '   Table - table to delete row from
        '   KeyValues - list of key values matching row to be deleted.  values MUST be in the same order as listed in Table.PrimaryKey
        '
        ' returns:
        '   >= 0 - # of rows deleted
        '   <0 - SqlCommand.ExecuteNonQuery error value
        '   sqlOtherErr - something went wrong
        Try
            _errorMessage = String.Empty                                                    ' clear error message
            If keyValues.Count = 0 Then                                                     ' if no key field(s)
                Return 0                                                                    ' then return no rows deleted
            End If

            Dim DeleteSQL As String = "DELETE "                                             ' set the first line of the DELETE command 
            Dim k As Integer = 0
            For Each KeyCol As DataColumn In Table.PrimaryKey                               ' for each key column 
                If k > 0 Then                                                               ' if not the first key column
                    DeleteSQL &= ", "                                                       ' add a comma and a space
                End If
                DeleteSQL &= String.Format("{0}.{1}", Table.TableName, KeyCol.ColumnName)   ' add the Table.KeyColumnName
                k += 1                                                                      ' increment key column counter
            Next
            DeleteSQL &= String.Format(" FROM {0} WHERE ", Table.TableName)                 ' add the 2nd line, FROM Table, and start 3rd line
            k = 0                                                                           ' reset key column counter (used as param count)
            For Each KeyCol As DataColumn In Table.PrimaryKey                               ' for each key column
                ' add the WHERE criteria or each key column
                DeleteSQL &= EaSql.Sql.SqlWhereColumnValueString(k, Table.TableName, KeyCol.ColumnName, keyValues(k), , , True)
                k += 1
            Next

            Return ExecuteNonQuery(MySqlConn, DeleteSQL)                                    ' delete the row
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return sqlOtherErr
        End Try
    End Function

#End Region

#Region " Get WhereSQL for Row "

    Public Shared Function GetWhereSqlForRow(row As DataRow) As String

        ' gets the where sql clause to select the row from the database

        Try
            If row.Table.PrimaryKey.Length = 0 Then                                                     ' if no primary key
                _errorMessage = "No primary key"                                                        ' set error message
                _errorCode = teNoPrimaryKey                                                             ' set error code
                Return String.Empty                                                                     ' return string.empty
            End If

            ' WHERE (`ColumnName` = KeyValue) 
            ' WHERE (`ColumnName1` = KeyValue1) AND (`ColumnName2` = KeyValue2) 

            Dim WhereSql As String = "WHERE ("                                                          ' get initial where text
            For i As Integer = 0 To row.Table.PrimaryKey.Length - 1                                     ' for each key value
                If i > 0 Then                                                                           ' if not the first value
                    WhereSql &= ") AND ("                                                               ' add brackets and AND 
                End If
                WhereSql &= ColumnSelectString(row.Table.PrimaryKey(i).ColumnName, row.ItemArray(i))    ' get selecting string for column
            Next
            WhereSql &= ")"                                                                             ' add ending bracket
            Return WhereSql
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            _errorCode = teOtherErr
            Return String.Empty
        End Try
    End Function

#End Region

#Region " Update via SQL "

    Public Shared Function UpdateViaSql(MySqlConn As MySqlConnection,
                                        UpdateSqlText As String) As Integer

        ' Updates a table via a SQL command.  Makes sure sql command starts with "UPDATE", and then calls
        '   ExecuteNonQuery to do the query.  
        '
        ' vars passed:
        '   MySqlConn - mysql connection to use
        '   UpdateSqlText - SqlText to use
        '
        ' returns:
        '   >= 0 - no errors
        '   sqlInvalidCommand - invalid sql command
        '   <0 - SqlCommand.ExecuteNonQuery error value
        '   sqlErr- other error in sql 

        _errorMessage = String.Empty                                        ' clear error message
        Try
            If Not UpdateSqlText.ToUpper.StartsWith("UPDATE".ToUpper) Then
                _errorMessage = String.Format("The Update SQL command does not start with ""UPDATE"".  SQL Command Text:{0}{1}", vbCrLf, UpdateSqlText)
                Return sqlInvalidCommand
            End If

            Return ExecuteNonQuery(MySqlConn, UpdateSqlText)                ' do the update via SQL, and return the value
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return sqlErr
        End Try
    End Function

#End Region

#End Region

#Region " Users "

#Region " Change Password "

    Public Shared Function ChangePassword(MySqlConn As MySqlConnection,
                                          UserName As String,
                                          HostName As String,
                                          newPassword As String,
                                          Optional auth_plugin As String = NativePassword) As Integer

        ' create a user for a connection
        '
        ' vars passed:
        '   MySqlConn - mySql connection to use.  NOTE: no database param in connection string
        '   UserName - name of user 
        '   HostName - name of host 
        '   newPassword - user's new password
        '   auth_plugin - mysql authentication plugin
        '
        ' returns:
        '   >= 0 - user created
        '   sqlNoConnection - no connection
        '   sqlNoUser - no user name
        '   <0 - error in creating user
        '   sqlOtherErr - something went wrong 

        ' ALTER USER IF EXISTS '<username>'@'<hostname>' IDENTIFIED BY '<newPassword>'
        ' ALTER USER IF EXISTS '<username>'@'<hostname>' IDENTIFIED WITH <auth_plugin> BY '<newPassword>'
        Const CreateUserFormat As String = "ALTER USER IF EXISTS '{0}'@'{1}'"
        Const PasswordNoAuthFormat As String = " IDENTIFIED BY '{0}'"
        Const PasswordWithAuthFormat As String = " IDENTIFIED WITH {0} BY '{1}'"

        _errorMessage = String.Empty
        Try
            If Not DatabaseParamBlank(MySqlConn) Then                                                   ' if got a database param
                Return sqlInvalidConnStr                                                                ' return error
            End If
            If UserName = String.Empty Then                                                             ' if no user name                     
                Return sqlNoUser                                                                        ' return error
            End If

            Dim SqlText As String = String.Format(CreateUserFormat, UserName.ToLower, HostName)         ' create initial sql command text
            If auth_plugin = String.Empty Then                                                          ' if no authentication plug in 
                SqlText &= String.Format(PasswordNoAuthFormat, newPassword)                             ' add just password 
            Else                                                                                        ' else got authentication plug in
                SqlText &= String.Format(PasswordWithAuthFormat, auth_plugin, newPassword)              ' add authentication plug in and password
            End If
            Return ExecuteNonQuery(MySqlConn, SqlText)                                                  ' change password
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return sqlOtherErr
        End Try
    End Function

#End Region

#Region " Create User "

    Public Shared Function CreateUser(MySqlConn As MySqlConnection,
                                      UserName As String,
                                      HostName As String,
                                      Optional password As String = "",
                                      Optional auth_plugin As String = NativePassword) As Integer

        ' create a user for a connection
        '
        ' vars passed:
        '   MySqlConn - mySql connection to use.  NOTE: no database param in connection string
        '   UserName - name of user to create
        '   HostName - name of host 
        '   password - user's password
        '   auth_plugin - mysql authentication plugin
        '
        ' returns:
        '   >= 0 - user created
        '   sqlNoConnection - no connection
        '   sqlNoUser - no user name
        '   <0 - error in creating user
        '   sqlOtherErr - something went wrong 

        ' CREATE USER IF NOT EXISTS '<username>'@'<hostname>' 
        ' CREATE USER IF NOT EXISTS '<username>'@'<hostname>' IDENTIFIED BY '<password>'
        ' CREATE USER IF NOT EXISTS '<username>'@'<hostname>' IDENTIFIED WITH <auth_plugin> BY '<password>''   
        Const CreateUserFormat As String = "CREATE USER IF NOT EXISTS '{0}'@'{1}'"
        Const PasswordNoAuthFormat As String = " IDENTIFIED BY '{0}'"
        Const PasswordWithAuthFormat As String = " IDENTIFIED WITH {0} BY '{1}'"

        _errorMessage = String.Empty
        Try
            If UserExists(MySqlConn, UserName) = ExistsTypes.Yes Then                                   ' if already got a user
                Return sqlAlreadyExists                                                                 ' return already exists
            End If

            If Not DatabaseParamBlank(MySqlConn) Then                                                   ' if got a database param
                Return sqlInvalidConnStr                                                                ' return error
            End If
            Dim SqlText As String = String.Format(CreateUserFormat, UserName.ToLower, HostName)         ' create initial sql command text
            If password <> String.Empty Then                                                            ' if got password
                If auth_plugin = String.Empty Then                                                      ' if no authentication plug in 
                    SqlText &= String.Format(PasswordNoAuthFormat, password)                            ' add in just password 
                Else                                                                                    ' else got authentication plug in
                    SqlText &= String.Format(PasswordWithAuthFormat, auth_plugin, password)             ' add authentication plug in and password
                End If
            End If
            Return ExecuteNonQuery(MySqlConn, SqlText)                                                  ' create user
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return sqlOtherErr
        End Try
    End Function

#End Region

#Region " Drop User "

    Public Shared Function DropUser(MySqlConn As MySqlConnection,
                                    UserName As String,
                                    HostName As String) As Integer

        ' drops a user from a connection
        '
        ' vars passed:
        '   MySqlConn - mySql connection to use.  NOTE: no database param in connection string 
        '   UserName - name of user to drop
        '   HostName - name of host 
        '
        ' returns:
        '   >= 0 - user created
        '   sqlNoConnection - no connection
        '   sqlNoUser - no user name
        '   <0 - error in creating user
        '   sqlErr - something went wrong 

        ' DROP USER IF EXISTS '<username>'@'<hostname>' 
        Const DropUserFormat As String = "DROP USER IF EXISTS '{0}'@'{1}'"

        _errorMessage = String.Empty
        Try
            If Not DatabaseParamBlank(MySqlConn) Then                                                   ' if got a database param
                Return sqlInvalidConnStr                                                                ' return error
            End If
            Dim SqlText As String = String.Format(DropUserFormat, UserName.ToLower, HostName)           ' create sql command text
            Return ExecuteNonQuery(MySqlConn, SqlText)                                                  ' drop user
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return sqlOtherErr
        End Try
    End Function

#End Region

#Region " Get User Host Name "

    Public Shared Function GetUserHostName(MySqlConn As MySqlConnection,
                                           UserName As String) As String

        ' gets the host name for a user
        '
        ' vars passed:
        '   MySqlConn - connection to use
        '   UserName - user name to get host name for
        '
        ' returns:
        '   MySqlVersion - version info Major.Minor.Build
        '   Nothing - 
        '     - version info not found
        '     - something went wrong

        _errorMessage = String.Empty
        Try
            Dim objToFind As New MySqlInfo(MySqlInfo.MySqlTypesToFindTypes.User, UserName)      ' get object to find info
            If DoesItExist(MySqlConn, objToFind) = ExistsTypes.Yes Then                         ' if got version string
                Return CStr(objToFind.ReaderData)                                               ' return view text 
            Else                                                                                ' else did not get version info
                _errorMessage = "View not found"
                Return String.Empty
            End If
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return String.Empty
        End Try
    End Function

#End Region

#Region " Grant Access "

    Public Shared Function UserGrantAllAccess(MySqlConn As MySqlConnection,
                                              Database As String,
                                              UserName As String,
                                              HostName As String) As Integer

        ' grants user privileges to a database
        ' 
        ' vars passed:
        '   MySqlConn - mySql connection to use.  
        '   Database - name of database to grant access to
        '   UserName - name of user to drop
        '   HostName - name of host 
        '
        ' returns:
        '   sqlNoConnection - no connection
        '   sqlInvalidConnStr - invalid connection string
        '   sqlNoDatabase - could not find database
        '   sqlNoUser - no user name, or cannot find user
        '   sqlOtherErr - something went wrong

        ' GRANT ALL ON <database>.* TO '<username>'@'<hostname>'
        Const GrantUserFormat As String = "GRANT ALL ON {0}.* TO '{1}'@'{2}'"

        _errorMessage = String.Empty
        Try
            If DatabaseExists(MySqlConn, Database) <> ExistsTypes.Yes Then      ' if database does not exits (returns err if database in conn string)
                Return sqlNoDatabase                                            ' return error
            End If
            If UserExists(MySqlConn, UserName) <> ExistsTypes.Yes Then          ' if cant find  user
                Return sqlNoUser                                                ' return error
            End If

            Dim SqlText As String = String.Format(GrantUserFormat, Database.ToLower, UserName.ToLower, HostName) ' create sql command text
            Return ExecuteNonQuery(MySqlConn, SqlText)                          ' grant user access
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return sqlOtherErr
        End Try
    End Function

#End Region

#Region " User Exists "

    Public Shared Function UserExists(MySqlConn As MySqlConnection,
                                      UserName As String) As ExistsTypes

        ' checks to see if a user exists for a table in a connection
        ' looks in database mysql table user (by default, user names are stored in this database/table)
        '
        ' vars passed:
        '   MySqlConn - mySql connection to use.  NOTE: no database param in connection string
        '   UserName - name of user to find
        '
        ' returns:
        '   ExistsTypes.Yes - user found
        '   ExistsTypes.No - user not found
        '   ExistsTypes.OverMaxTries - tried more than MaxTries to open connection
        '   ExistsTypes.GotError 
        '     - error getting dbMySqlInfo
        '     - MySqlConn is nothing
        '     - UserName is nothing
        '     - something went wrong


        _errorMessage = String.Empty                                                            ' clear error message
        Try
            Dim objToFind As New MySqlInfo(MySqlInfo.MySqlTypesToFindTypes.User, UserName)      ' get object to find info
            Return DoesItExist(MySqlConn, objToFind)                                            ' see if database exists
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return ExistsTypes.GotError
        End Try
    End Function

#End Region

#End Region

#Region " Views/Procedures Actions "

#Region " Notes "

    '   Views do not have parameters
    '   Procedures have parameter(s)

#End Region

#Region " Create "

    Public Shared Function CreateProcedure(MySqlConn As MySqlConnection,
                                           ProcName As String,
                                           SqlText As String,
                                           Params As List(Of MySqlParam),
                                           ByRef ProcFullSqlText As String) As Integer

        ' creates a query
        '
        ' vars passed:
        '   MySqlConn - connection to use
        '   ProcName - procedure to create
        '   SqlText - SQL command text (SQL Query text) note: must end with ";"
        '   Params - list of params in sql query text
        '   ProcFullSqlText - full procedure text - ByRef - the full procedure text will be set here
        '
        ' returns:
        '   >= 0 - no errors
        '   <0 - SqlCommand.ExecuteNonQuery error value
        '   sqlOtherErr- other error in sql 
        '   sqlDropErr - other error in dropping procedure
        '   sqlCreateErr - other error in creating procedure         

        ' USE `<database>`;
        ' DROP procedure IF EXISTS `<procname>`;
        '
        ' DELIMITER $$
        ' USE `<database>`$$
        ' CREATE PROCEDURE `<procname>` (
        '   IN <var1> <DATATYPE>,
        '   IN <var2> <DATATYPE> ...
        ')
        ' BEGIN
        '   <SqlText>;
        ' END$$
        '
        ' DELIMITER ;
        ' 
        ' ExecuteNonQuery only does ONE command, so an error is returned if the whole create procedure command text is passed.
        '   eliminate the "USE `<database>`;"  command. the database is in the MySqlConn ConnectionString property
        '   if ViewOrProcExits() call eliminates the need for "DROP procedure IF EXISTS `<procname>`;"
        '   eliminate the "DELIMITER $$" command
        '   eliminate the "USE `<database>`$$" command. the database is in the MySqlConn ConnectionString property
        '   include the whole "CREATE PROCEDURE ... BEGIN ... END" command, but exclude the ending "$$"
        '   eliminate the " DELIMITER ;", not needed because excluded "DELIMITER $$" command

        Const ProcFormat As String = "CREATE PROCEDURE `{0}` ({3}{1}{3}){3}BEGIN{3}{2}{3}END"

        _errorMessage = String.Empty
        Try
            If SqlText(SqlText.Length - 1) <> ";" Then                                              ' if sqlText does not end with semi colon
                _errorMessage = "No ending semi colon "";"" in SqlText value"
                Return sqlCreateErr                                                                 ' return error
            End If
            If Params.Count = 0 Then                                                                ' if no params
                _errorMessage = "No parameters"
                Return sqlInvalidParameters                                                         ' return error
            End If

            Dim DropErr As Integer = DropProcedure(MySqlConn, ProcName)                             ' delete the procedure
            If DropErr < 0 Then                                                                     ' if error dropping view
                Return DropErr                                                                      ' return the error
            End If

            Dim paramStr As String = String.Empty
            Dim pCount As Integer = 0
            For Each param As MySqlParam In Params                                                  ' for each param                
                If pCount > 0 Then                                                                  ' if not the first param
                    paramStr &= ", " & vbCrLf                                                       ' add a comma and a new line                    
                End If
                paramStr &= vbTab                                                                   ' formatted text, each param line starts with [TAB]

                ' rename MySqlParams to EaMySqlParams
                ' add Param Mode property to EaMySqlParams
                ' set IN, OUT, INOUT in format string based on EaMySqlParams.mode

                If param.Column.DataType Is MySqlDataType.dtDecimal Then                            ' get param format based on param type                    
                    paramStr &= String.Format("{0} {1} {2}({3}.{4})", param.Mode, param.Name, param.Column.SqlDataType.ToUpper, param.Column.Length, param.Column.Precision)
                ElseIf param.Column.DataType Is MySqlDataType.dtVarChar Then
                    paramStr &= String.Format("{0} {1} {2}({3})", param.Mode, param.Name, param.Column.SqlDataType.ToUpper, param.Column.Length)
                Else
                    paramStr &= String.Format("{0} {1} {2}", param.Mode, param.Name, param.Column.SqlDataType.ToUpper)
                End If
                pCount += 1
            Next
            ProcFullSqlText = String.Format(ProcFormat, ProcName, paramStr, SqlText, vbCrLf)        ' get the create proc MySql command Text
            Return ExecuteNonQuery(MySqlConn, ProcFullSqlText)                                      ' create the procedure
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return sqlOtherErr
        End Try
    End Function

    Public Shared Function CreateView(MySqlConn As MySqlConnection,
                                      ViewName As String,
                                      SqlText As String) As Integer

        ' creates a query
        '
        ' vars passed:
        '   MySqlConn - connection to use
        '   ViewName - view to create
        '   SqlCommand - SQL command text (SQL Query text)
        '
        ' returns:
        '   >= 0 - no errors
        '   <0 - SqlCommand.ExecuteNonQuery error value
        '   sqlOtherErr- other error in sql 
        '   sqlDropErr - other error in dropping view
        '   sqlCreateErr - other error in creating view            

        ' CREATE OR REPLACE VIEW <ViewName> AS <ViewSqlText>
        Const CreateViewFormat As String = "CREATE OR REPLACE VIEW {0} AS {1}"

        _errorMessage = String.Empty
        Try
            Dim DropErr As Integer = DropView(MySqlConn, ViewName)                      ' delete the view
            If DropErr < 0 Then                                                         ' if error dropping view
                Return DropErr                                                          ' return the error
            End If
            Dim ViewSql As String = String.Format(CreateViewFormat, ViewName, SqlText)  ' set the SQL to create the view
            Return ExecuteNonQuery(MySqlConn, ViewSql)                                  ' create the view
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return sqlOtherErr
        End Try
    End Function

#End Region

#Region " Drop "

    Public Shared Function DropProcedure(MySqlConn As MySqlConnection,
                                         ProcName As String) As Integer

        ' drops (deletes) a query (procedure)
        '
        ' vars passed:
        '   MySqlConn - connection to use
        '   ProcName - query procedure to drop
        '
        ' returns:
        '   >= 0 - no errors or query did not exist
        '   <0 - SqlCommand.ExecuteNonQuery error value
        '   sqlNoProcName - no query name
        '   sqlOtherErr - other error in sql 
        '   sqlDropErr - other error in dropping query

        ' DROP PROCEDURE IF EXISTS `<procname>`
        Const DropProcFormat As String = "DROP PROCEDURE IF EXISTS `{0}`"

        _errorMessage = String.Empty
        Try
            Dim SqlText As String = String.Format(DropProcFormat, ProcName.ToLower)         ' set sql text to drop procedure
            Return ExecuteNonQuery(MySqlConn, SqlText, errNoTable, NoErrors)                ' drop the view, and handle no table error
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return sqlDropErr
        End Try
    End Function

    Public Shared Function DropView(MySqlConn As MySqlConnection,
                                    ViewName As String) As Integer

        ' drops (deletes) a query (procedure or view)
        '
        ' vars passed:
        '   MySqlConn - connection to use
        '   ProcName - query procedure to create
        '
        ' returns:
        '   >= 0 - no errors or query did not exist
        '   <0 - SqlCommand.ExecuteNonQuery error value
        '   sqlNoProcName - no query name
        '   sqlOtherErr - other error in sql 
        '   sqlDropErr - other error in dropping query

        ' DROP VIEW IF EXISTS `<viewname>`
        Const DropViewFormat As String = "DROP VIEW IF EXISTS `{0}`"

        _errorMessage = String.Empty
        Try
            Dim SqlText As String = String.Format(DropViewFormat, ViewName.ToLower)         ' set sql text to drop view
            Return ExecuteNonQuery(MySqlConn, SqlText, errNoView, NoErrors)                 ' drop the view, and handle no view error
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return sqlDropErr
        End Try
    End Function

#End Region

#Region " Exists "

    Public Shared Function ProcExits(MySqlConn As MySqlConnection,
                                     ProcName As String) As ExistsTypes

        ' checks to see if a procedure exits in a connection
        '
        ' vars passed:
        '   MySqlConn - connection to use
        '   ProcName - procedure name to find
        '
        ' returns:
        '   ExistsTypes.Yes - procedure found
        '   ExistsTypes.No - procedure not found
        '   ExistsTypes.OverMaxTries - tried more than MaxTries to open connection
        '   ExistsTypes.GotError 
        '     - error getting dbMySqlInfo
        '     - MySqlConn is nothing
        '     - ProcName is nothing
        '     - something went wrong

        _errorMessage = String.Empty
        Try
            Dim objToFind As New MySqlInfo(MySqlInfo.MySqlTypesToFindTypes.Procedure, ProcName, String.Empty, MySqlConn.Database)   ' get object to find info
            Return DoesItExist(MySqlConn, objToFind)                                                                                ' see if procedure exists
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return ExistsTypes.GotError
        End Try
    End Function

    Public Shared Function ViewExits(MySqlConn As MySqlConnection,
                                     ViewName As String) As ExistsTypes

        ' checks to see if a view exits in a connection
        '
        ' vars passed:
        '   MySqlConn - connection to use
        '   ViewName - view name to find
        '
        ' returns:
        '   ExistsTypes.Yes - view found
        '   ExistsTypes.No - view not found
        '   ExistsTypes.OverMaxTries - tried more than MaxTries to open connection
        '   ExistsTypes.GotError 
        '     - error getting dbMySqlInfo
        '     - MySqlConn is nothing
        '     - viewName is nothing
        '     - something went wrong

        _errorMessage = String.Empty
        Try
            Dim objToFind As New MySqlInfo(MySqlInfo.MySqlTypesToFindTypes.View, ViewName, String.Empty, MySqlConn.Database)    ' get object to find info
            Return DoesItExist(MySqlConn, objToFind)                                                                            ' see if view exists
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return ExistsTypes.GotError
        End Try
    End Function

#End Region

#Region " GetProcParams "

    Private Shared Function GetNextModePosition(sqlText As String, Optional start As Integer = 0) As Integer

        ' gets index of next mode value in sql text
        '
        ' NOTE: it is assumed sqlText is UPPERCASE
        '
        ' vars passed:
        '   sqlText - the sql procedure text;  it is assumed sqlText is UPPERCASE
        '   start - starting position for mode search
        '
        ' returns:
        '   >= 0 - starting position of next mode value
        '   -1 
        '     - no more mode values found
        '     - something went wrong

        Try
            ' find all three mode types
            Dim inPos As Integer = sqlText.IndexOf("IN ", start)
            Dim outPos As Integer = sqlText.IndexOf("OUT ", start)
            Dim inOutPos As Integer = sqlText.IndexOf("INOUT ", start)

            If inPos < 0 AndAlso outPos < 0 AndAlso inOutPos < 0 Then           ' if did not find any mode type
                Return -1                                                       ' return -1
            End If
            If inPos < 0 Then inPos = Integer.MaxValue                          ' if in mode not found, reset inPos to max int
            If outPos < 0 Then outPos = Integer.MaxValue                        ' if out mode not found, reset outPos to max int
            If inOutPos < 0 Then inOutPos = Integer.MaxValue                    ' if inOut mode not found, reset inOutPos to max int

            ' return FIRST mode position found

            If inPos < outPos AndAlso inPos < inOutPos Then                     ' if in mode position first
                Return inPos                                                    ' return inPos
            ElseIf outPos < inPos AndAlso outPos < inOutPos Then                ' else if out mode position first
                Return outPos                                                   ' return outPos
            Else                                                                ' else inOut mode position is first
                Return inOutPos                                                 ' return inOutPos
            End If
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return -1
        End Try
    End Function

    Public Shared Function GetProcParams(SqlText As String) As List(Of MySqlParam)

        ' gets the parameters from an Access Sql query.  Params are surrounded by [] also converts params to lower case
        '
        ' vars passed        
        '   SqlText - SQL command (SQL Query text)
        '
        ' returns:
        '   list of parameters.  empty list is OK
        '   Nothing - something went wrong

        '1) find first "("
        '2) find matching ending ")"
        '3) get next "IN" or "OUT" or "INOUT". if past ending ")", then done, if none found, then done
        '4) get next word, will be the parameter
        '5) go back to step 3.

        Const leftBracket As Char = "("c
        Const rightBracket As Char = ")"c

        Try
            Dim Params As New List(Of MySqlParam)

            '1) find first "("
            Dim lb As Integer = SqlText.IndexOf(leftBracket)
            If lb < 0 Then
                Return Nothing
            End If

            '2) find matching ending ")"
            Dim bracketCount As Integer = 1
            Dim endParams As Integer = lb + 1
            While bracketCount > 0 And endParams < SqlText.Length - 1                       ' while not found ending bracket and not past end
                If SqlText(endParams) = leftBracket Then                                    ' if got another left bracket
                    bracketCount += 1                                                       ' add 1 to bracket count
                ElseIf SqlText(endParams) = rightBracket Then                               ' if got a right bracket
                    bracketCount -= 1                                                       ' subtract 1 from bracket count
                End If
                endParams += 1                                                              ' go to next char in sql text
            End While
            If endParams >= SqlText.Length Then                                             ' if did not find ending bracket
                Return Nothing                                                              ' return nothing
            End If

            Dim sqlUpper As String = SqlText.ToUpper
            Dim modePos As Integer = lb
            Dim paramStart As Integer
            Dim paramEnd As Integer
            Dim paramName As String
            Do
                '3) get next "IN" or "OUT" or "INOUT". 
                modePos = GetNextModePosition(sqlUpper, modePos)                            ' get next mode position
                If modePos > 0 AndAlso modePos < endParams Then                             ' if found mode string
                    '4) get next word, will be the parameter
                    paramStart = SqlText.IndexOf(" "c, modePos)                             ' get next space in sql text
                    If paramStart < 0 Then                                                  ' if did not find space
                        Return Nothing                                                      ' return nothing
                    End If
                    paramStart += 1                                                         ' move to start of next word
                    paramEnd = SqlText.IndexOf(" "c, paramStart)                            ' find next space (end of param in sql text)
                    If paramEnd < 0 Then                                                    ' if did not find end of param
                        Return Nothing                                                      ' return nothing
                    End If
                    paramName = SqlText.Substring(paramStart, paramEnd - paramStart)        ' get param name from sql text
                    Params.Add(New MySqlParam(paramName))                                   ' add param name to list of params
                    modePos = paramEnd                                                      ' restart next mode search after param
                End If
            Loop Until modePos >= endParams OrElse modePos < 0
            Return Params
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return Nothing
        End Try
    End Function

#End Region

#Region " Get Proc Text / View Text  "

    Public Shared Function GetProcedureInfo(MySqlConn As MySqlConnection,
                                            ProcName As String,
                                            ByRef SqlText As String,
                                            ByRef Params As List(Of MySqlParam)) As ExistsTypes

        ' gets the command text for a procedure, and the list of parameters
        '        
        '
        ' vars passed:
        '   MySqlConn - connection to use
        '   ProcName - procedure name to get command text for
        '   SqlText - SQL command text (SQL Query text) note: must end with ";"
        '   Params - list of params in sql query text
        '
        ' returns:
        '   ExistsTypes.Yes - procedure found
        '   ExistsTypes.No - procedure not found or procedure parameters not found
        '   ExistsTypes.OverMaxTries - tried more than MaxTries to open connection
        '   ExistsTypes.GotError 
        '     - error getting dbMySqlInfo
        '     - MySqlConn is nothing
        '     - ProcName is nothing
        '     - something went wrong


        ' 1) get procedure text
        ' 2) get list of procedure variables

        _errorMessage = String.Empty
        Try
            Dim objToFind As New MySqlInfo(MySqlInfo.MySqlTypesToFindTypes.ProcedureSqlText, ProcName, String.Empty, MySqlConn.Database) ' get proc sql obj to find 
            Dim exists As ExistsTypes = DoesItExist(MySqlConn, objToFind)                       ' check if procedure exists
            If exists <> ExistsTypes.Yes Then                                                   ' if exists type not YES
                _errorMessage = "Procedure SQL command not found"
                SqlText = String.Empty                                                          ' clear by ref params
                Params = Nothing
                Return exists                                                                   ' return exists value
            End If
            Dim paramObjToFind As New MySqlInfo(MySqlInfo.MySqlTypesToFindTypes.ProcedureParams, ProcName, String.Empty, MySqlConn.Database) ' get param obj to find 
            exists = DoesItExist(MySqlConn, paramObjToFind)                                     ' check if parameters exists
            If exists <> ExistsTypes.Yes Then                                                   ' if exists type not YES
                _errorMessage = "Procedure parameters not found"
                SqlText = String.Empty                                                          ' clear by ref params
                Params = Nothing
                Return exists                                                                   ' return exists value
            End If
            SqlText = CStr(objToFind.ReaderData)
            Params = CType(paramObjToFind.ReaderData, List(Of MySqlParam))
            Return ExistsTypes.Yes
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            SqlText = String.Empty
            Params = Nothing
            Return ExistsTypes.GotError
        End Try
    End Function

    Public Shared Function GetViewText(MySqlConn As MySqlConnection,
                                       ViewName As String) As String

        ' gets the command text for a view
        '
        ' vars passed:
        '   MySqlConn - connection to use
        '   ViewName - view name to get command text for
        '
        ' returns:
        '   <> String.Empty - test for view command
        '   String.Empty - 
        '     - version info not found
        '     - something went wrong

        _errorMessage = String.Empty
        Try
            Dim objToFind As New MySqlInfo(MySqlInfo.MySqlTypesToFindTypes.ViewSqlText, ViewName, String.Empty, MySqlConn.Database) ' get view text object to find
            If DoesItExist(MySqlConn, objToFind) = ExistsTypes.Yes Then             ' if got view/proc 
                Return CStr(objToFind.ReaderData)                                   ' return view/proc text 
            Else                                                                    ' else did not get view/proc
                _errorMessage = "View not found"
                Return String.Empty
            End If
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            Return String.Empty
        End Try
    End Function

#End Region

#End Region

#Region " Version "

    Shared ReadOnly Property MySqlVer As MySqlVersion
        Get
            Return _mySqlVer
        End Get
    End Property

    Public Shared Sub GetMySqlVersion(MySqlConn As MySqlConnection,
                                      Optional resetVersion As Boolean = False)

        ' gets the MySql version info
        '
        ' vars passed:
        '   MySqlConn - connection to use
        '   resetVersion - set to TRUE to reset the version info
        '
        ' returns:
        '   MySqlVersion - version info Major.Minor.Build
        '   Nothing - 
        '     - version info not found
        '     - something went wrong

        _errorMessage = String.Empty
        Try
            If resetVersion Then                                                                        ' if want to reset the version
                _mySqlVer = Nothing                                                                     ' free the version info
            End If
            If _mySqlVer IsNot Nothing Then                                                             ' if already set my sql version
                Return                                                                                  ' exit now
            End If
            Dim objToFind As New MySqlInfo(MySqlInfo.MySqlTypesToFindTypes.Version, String.Empty)       ' get version object to find
            If DoesItExist(MySqlConn, objToFind) = ExistsTypes.Yes Then                                 ' if got version string
                _mySqlVer = New MySqlVersion(CStr(objToFind.ReaderData))                                ' get version info string, and sets MySqlVer data
            Else                                                                                        ' else did not get version info
                _errorMessage = "Version info not found"
            End If
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
        End Try
    End Sub

#End Region

End Class