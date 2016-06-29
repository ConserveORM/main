/*******************************************************************************
 * Copyright (c) 2009, 2016 Erik Berglund.
 *    
 *        This file is part of Conserve.
 *    
 *        Conserve is free software: you can redistribute it and/or modify
 *        it under the terms of the GNU Affero General Public License as published by
 *        the Free Software Foundation, either version 3 of the License, or
 *        (at your option) any later version.
 *    
 *        Conserve is distributed in the hope that it will be useful,
 *        but WITHOUT ANY WARRANTY; without even the implied warranty of
 *        MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *        GNU Affero General Public License for more details.
 *    
 *        You should have received a copy of the GNU Affero General Public License
 *        along with Conserve.  If not, see <https://www.gnu.org/licenses/agpl.html>.
 *******************************************************************************/
package org.conserve.tools.generators;

import java.lang.reflect.Method;

import org.conserve.adapter.AdapterBase;
import org.conserve.annotations.ColumnName;
import org.conserve.annotations.TableName;
import org.conserve.tools.Defaults;

/**
 * Get the database name (column name, table name) for various entities.
 * 
 * @author Erik Berglund
 * 
 */
public class NameGenerator
{
	private static final String[] FORBIDDEN_COLUMN_NAMES = new String[] { "A", "ABORT", "ABS", "ABSENT", "ABSOLUTE",
			"ACCESS", "ACCESSIBLE", "ACCORDING", "ACCOUNT", "ACTION", "ADA", "ADD", "ADMIN", "AFTER", "AGAINST",
			"AGGREGATE", "ALGORITHM", "ALIAS", "ALL", "ALL ", "ALLOCATE", "ALSO", "ALTER", "ALWAYS", "ANALYSE",
			"ANALYZE", "AND", "ANY", "ARE", "ARRAY", "ARRAY_AGG", "AS", "ASC", "ASCII", "ASENSITIVE", "ASSERTION",
			"ASSIGNMENT", "ASYMMETRIC", "AT", "ATOMIC", "ATTRIBUTE", "ATTRIBUTES", "AUTHORIZATION", "AUTOEXTEND_SIZE",
			"AUTO_INCREMENT", "AVG", "AVG_ROW_LENGTH", "BACKUP", "BACKWARD", "BASE64", "BEFORE", "BEGIN", "BERNOULLI",
			"BETWEEN", "BIGINT", "BINARY", "BINARY ", "BINLOG", "BIT", "BITVAR", "BIT_LENGTH", "BLOB", "BLOB ", "BLOCK",
			"BLOCKED", "BOM", "BOOL", "BOOLEAN", "BOTH", "BREADTH", "BREAK", "BROWSE", "BTREE", "BULK", "BY", "BY ",
			"BYTE", "C", "CACHE", "CALL", "CALL ", "CALLED", "CARDINALITY", "CASCADE", "CASCADED", "CASE", "CASE ",
			"CAST", "CATALOG", "CATALOG_NAME", "CEIL", "CEILING", "CHAIN", "CHANGE ", "CHANGED", "CHANNEL", "CHAR",
			"CHAR ", "CHARACTER", "CHARACTERISTICS", "CHARACTERS", "CHARACTER_LENGTH", "CHARACTER_SET_CATALOG",
			"CHARACTER_SET_NAME", "CHARACTER_SET_SCHEMA", "CHARSET", "CHAR_LENGTH", "CHECK", "CHECK ", "CHECKED",
			"CHECKPOINT", "CHECKSUM", "CIPHER", "CLASS", "CLASS_ORIGIN", "CLIENT", "CLOB", "CLOSE", "CLUSTER",
			"CLUSTERED", "COALESCE", "COBOL", "CODE", "COLLATE", "COLLATION", "COLLATION_CATALOG", "COLLATION_NAME",
			"COLLATION_SCHEMA", "COLLECT", "COLUMN", "COLUMNS", "COLUMN_FORMAT", "COLUMN_NAME", "COMMAND_FUNCTION",
			"COMMAND_FUNCTION_CODE", "COMMENT", "COMMENTS", "COMMIT", "COMMITTED", "COMPACT", "COMPLETION",
			"COMPRESSED", "COMPRESSION", "COMPUTE", "CONCURRENT", "CONCURRENTLY", "CONDITION", "CONDITION_NUMBER",
			"CONFIGURATION", "CONNECT", "CONNECTION", "CONNECTION_NAME", "CONSISTENT", "CONSTRAINT", "CONSTRAINTS",
			"CONSTRAINT_CATALOG", "CONSTRAINT_NAME", "CONSTRAINT_SCHEMA", "CONSTRUCTOR", "CONTAINS", "CONTAINSTABLE",
			"CONTENT", "CONTEXT", "CONTINUE", "CONTROL", "CONVERSION", "CONVERT", "COPY", "CORR", "CORRESPONDING",
			"COST", "COUNT", "COVAR_POP", "COVAR_SAMP", "CPU", "CREATE", "CREATEDB", "CREATEROLE", "CREATEUSER",
			"CROSS", "CSV", "CUBE", "CUME_DIST", "CURRENT", "CURRENT_CATALOG", "CURRENT_DATE",
			"CURRENT_DEFAULT_TRANSFORM_GROUP", "CURRENT_PATH", "CURRENT_ROLE", "CURRENT_SCHEMA", "CURRENT_TIME",
			"CURRENT_TIME ", "CURRENT_TIMESTAMP", "CURRENT_TRANSFORM_GROUP_FOR_TYPE", "CURRENT_USER", "CURSOR",
			"CURSOR ", "CURSOR_NAME", "CYCLE", "DATA", "DATABASE", "DATABASE ", "DATABASES", "DATAFILE", "DATALINK",
			"DATE", "DATETIME", "DATETIME_INTERVAL_CODE", "DAY", "DAY_HOUR ", "DAY_MICROSECOND", "DAY_MINUTE",
			"DAY_SECOND ", "DB", "DBCC", "DEALLOCATE", "DEC", "DECIMAL", "DECIMAL ", "DECLARE", "DEFAULT", "DEFAULTS",
			"DEFAULT_AUTH", "DEFERRABLE", "DEFERRED", "DEFINED", "DEFINER", "DEGREE", "DELAYED", "DELAY_KEY_WRITE",
			"DELETE", "DELIMITER", "DELIMITERS", "DENSE_RANK", "DENY", "DEPTH", "DEREF", "DERIVED", "DESC", "DESCRIBE",
			"DESCRIBE ", "DESCRIPTOR", "DESTROY", "DESTRUCTOR", "DES_KEY_FILE", "DETERMINISTIC", "DIAGNOSTICS",
			"DICTIONARY", "DIRECTORY", "DISABLE", "DISCARD", "DISCONNECT", "DISK", "DISPATCH", "DISTINCT",
			"DISTINCTROW ", "DISTRIBUTED", "DIV", "DLNEWCOPY", "DLPREVIOUSCOPY", "DLURLCOMPLETE", "DLURLCOMPLETEONLY",
			"DLURLCOMPLETEWRITE", "DLURLPATH", "DLURLPATHONLY", "DLURLPATHWRITE", "DLURLSCHEME", "DLURLSERVER",
			"DLVALUE", "DO", "DOCUMENT", "DOMAIN", "DOUBLE", "DOUBLE ", "DROP", "DUAL", "DUMP", "DUMPFILE", "DUPLICATE",
			"DYNAMIC", "DYNAMIC_FUNCTION", "DYNAMIC_FUNCTION_CODE", "EACH", "EACH ", "ELEMENT", "ELSE", "ELSEIF",
			"EMPTY", "ENABLE", "ENCLOSED", "ENCODING", "ENCRYPTED", "ENCRYPTION", "END", "END-EXEC", "ENDS", "ENGINE",
			"ENGINES", "ENUM", "EQUALS", "ERRLVL", "ERROR", "ERRORS", "ESCAPE", "ESCAPED", "EVENT", "EVENTS", "EVERY",
			"EXCEPT", "EXCEPTION", "EXCHANGE", "EXCLUDE", "EXCLUDING", "EXCLUSIVE", "EXEC", "EXECUTE", "EXISTING",
			"EXISTS", "EXIT", "EXIT ", "EXP", "EXPANSION", "EXPIRE", "EXPLAIN", "EXPLAIN ", "EXPORT", "EXTENDED",
			"EXTENT_SIZE", "EXTERNAL", "EXTRACT", "FALSE", "FAMILY", "FAST", "FAULTS", "FETCH", "FIELDS", "FILE",
			"FILE_BLOCK_SIZE", "FILLFACTOR", "FILTER", "FINAL", "FIRST", "FIRST_VALUE", "FIXED", "FLAG", "FLOAT",
			"FLOAT4 ", "FLOAT8", "FLOOR", "FLUSH", "FOLLOWING", "FOLLOWS", "FOR", "FORCE", "FOREIGN", "FOREIGN ",
			"FORMAT", "FORTRAN", "FORWARD", "FOUND", "FREE", "FREETEXT", "FREETEXTTABLE", "FREEZE", "FROM", "FROM ",
			"FS", "FULL", "FULLTEXT", "FULLTEXTTABLE", "FUNCTION", "FUNCTIONS", "FUSION", "G", "GENERAL", "GENERATED",
			"GEOMETRY", "GEOMETRYCOLLECTION", "GET", "GET_FORMAT", "GLOBAL", "GO", "GOTO", "GRANT", "GRANTED", "GRANTS",
			"GREATEST", "GROUP", "GROUPING", "GROUP_REPLICATION", "HANDLER", "HASH", "HAVING", "HEADER", "HELP", "HEX",
			"HIERARCHY", "HIGH_PRIORITY", "HOLD", "HOLDLOCK", "HOST", "HOSTS", "HOUR", "HOUR_MICROSECOND",
			"HOUR_MINUTE ", "HOUR_SECOND", "ID", "IDENTIFIED", "IDENTITY", "IDENTITYCOL", "IDENTITY_INSERT", "IF",
			"IF ", "IGNORE", "IGNORE_SERVER_IDS", "ILIKE", "IMMEDIATE", "IMMUTABLE", "IMPLEMENTATION", "IMPLICIT",
			"IMPORT", "IN", "INCLUDE", "INCLUDING", "INCREMENT", "INDENT", "INDEX", "INDEXES", "INDICATOR", "INFILE",
			"INFIX", "INHERIT", "INHERITS", "INITIALIZE", "INITIALLY", "INITIAL_SIZE", "INLINE", "INNER", "INNER ",
			"INOUT", "INPUT", "INSENSITIVE", "INSERT", "INSERT ", "INSERT_METHOD", "INSTALL", "INSTANCE",
			"INSTANTIABLE", "INSTEAD", "INT", "INT1", "INT2 ", "INT3", "INT4", "INT8 ", "INTEGER", "INTEGRITY",
			"INTERSECT", "INTERSECTION", "INTERVAL", "INTO", "INTO ", "INVOKER", "IO", "IO_AFTER_GTIDS ",
			"IO_BEFORE_GTIDS", "IO_THREAD", "IPC", "IS", "ISNULL", "ISOLATION", "ISSUER", "ITERATE", "JOIN", "JSON",
			"K", "KEY", "KEYS", "KEY_BLOCK_SIZE", "KEY_MEMBER", "KEY_TYPE", "KILL", "LAG", "LANGUAGE", "LARGE", "LAST",
			"LAST_VALUE", "LATERAL", "LC_COLLATE", "LC_CTYPE", "LEAD", "LEADING", "LEAST", "LEAVE", "LEAVES", "LEFT",
			"LENGTH", "LESS", "LEVEL", "LIBRARY", "LIKE", "LIKE_REGEX", "LIMIT", "LINEAR ", "LINENO", "LINES",
			"LINESTRING", "LINK", "LIST", "LISTEN", "LN", "LOAD", "LOCAL", "LOCALTIME", "LOCALTIME ", "LOCALTIMESTAMP",
			"LOCATION", "LOCATOR", "LOCK", "LOCKS", "LOGFILE", "LOGIN", "LOGS", "LONG ", "LONGBLOB", "LONGTEXT",
			"LOOP ", "LOWER", "LOW_PRIORITY", "M", "MAP", "MAPPING", "MASTER", "MASTER_AUTO_POSITION", "MASTER_BIND",
			"MASTER_CONNECT_RETRY", "MASTER_DELAY", "MASTER_HEARTBEAT_PERIOD", "MASTER_HOST", "MASTER_LOG_FILE",
			"MASTER_LOG_POS", "MASTER_PASSWORD", "MASTER_PORT", "MASTER_RETRY_COUNT", "MASTER_SERVER_ID", "MASTER_SSL",
			"MASTER_SSL_CA", "MASTER_SSL_CAPATH", "MASTER_SSL_CERT", "MASTER_SSL_CIPHER", "MASTER_SSL_CRL",
			"MASTER_SSL_CRLPATH", "MASTER_SSL_KEY", "MASTER_SSL_VERIFY_SERVER_CERT", "MASTER_TLS_VERSION",
			"MASTER_USER", "MATCH", "MATCHED", "MAX", "MAXVALUE", "MAXVALUE ", "MAX_CARDINALITY",
			"MAX_CONNECTIONS_PER_HOUR", "MAX_QUERIES_PER_HOUR", "MAX_ROWS", "MAX_SIZE", "MAX_STATEMENT_TIME",
			"MAX_UPDATES_PER_HOUR", "MAX_USER_CONNECTIONS", "MEDIUM", "MEDIUMBLOB ", "MEDIUMINT", "MEDIUMTEXT",
			"MEMBER", "MEMORY", "MERGE", "MESSAGE_LENGTH", "MESSAGE_OCTET_LENGTH", "MESSAGE_TEXT", "METHOD",
			"MICROSECOND", "MIDDLEINT", "MIGRATE", "MIN", "MINUTE", "MINUTE_MICROSECOND", "MINUTE_SECOND", "MINVALUE",
			"MIN_ROWS", "MOD", "MODE", "MODIFIES", "MODIFIES ", "MODIFY", "MODULE", "MONTH", "MORE", "MOVE",
			"MULTILINESTRING", "MULTIPOINT", "MULTIPOLYGON", "MULTISET", "MUMPS", "MUTEX", "MYSQL_ERRNO", "NAME",
			"NAMES", "NAMESPACE", "NATIONAL", "NATURAL", "NCHAR", "NCLOB", "NDB", "NDBCLUSTER", "NESTING", "NEVER",
			"NEW", "NEXT", "NFC", "NFD", "NFKC", "NFKD", "NIL", "NO", "NOCHECK", "NOCREATEDB", "NOCREATEROLE",
			"NOCREATEUSER", "NODEGROUP", "NOINHERIT", "NOLOGIN", "NONBLOCKING", "NONCLUSTERED", "NONE", "NORMALIZE",
			"NORMALIZED", "NOSUPERUSER", "NOT", "NOTHING", "NOTIFY", "NOTNULL", "NOWAIT", "NO_WAIT",
			"NO_WRITE_TO_BINLOG ", "NTH_VALUE", "NTILE", "NULL", "NULLABLE", "NULLIF", "NULLS", "NUMBER", "NUMERIC",
			"NUMERIC ", "NVARCHAR", "OBJECT", "OCCURRENCES_REGEX", "OCTETS", "OCTET_LENGTH", "OF", "OFF", "OFFSET",
			"OFFSETS", "OIDS", "OLD", "OLD_PASSWORD", "ON", "ONE", "ONLY", "OPEN", "OPENDATASOURCE", "OPENQUERY",
			"OPENROWSET", "OPENXML", "OPERATION", "OPERATOR", "OPTIMIZE", "OPTIMIZER_COSTS ", "OPTION", "OPTIONALLY",
			"OPTIONS", "OR", "ORDER", "ORDERING", "ORDINALITY", "OTHERS", "OUT", "OUT ", "OUTER", "OUTFILE", "OUTPUT",
			"OVER", "OVERLAPS", "OVERLAY", "OVERRIDING", "OWNED", "OWNER", "P", "PACK_KEYS", "PAD", "PAGE", "PARAMETER",
			"PARAMETERS", "PARAMETER_MODE", "PARAMETER_NAME", "PARAMETER_SPECIFIC_NAME", "PARAMETER_SPECIFIC_SCHEMA",
			"PARSER", "PARSE_GCOL_EXPR", "PARTIAL", "PARTITION", "PARTITION ", "PARTITIONING", "PARTITIONS", "PASCAL",
			"PASSING", "PASSTHROUGH", "PASSWORD", "PATH", "PERCENT", "PERCENTILE_CONT", "PERCENTILE_DISC",
			"PERCENT_RANK", "PERMISSION", "PHASE", "PIVOT", "PLACING", "PLAN", "PLANS", "PLI", "PLUGIN", "PLUGINS",
			"PLUGIN_DIR", "POINT", "POLYGON", "PORT", "POSITION", "POSITION_REGEX", "POSTFIX", "POWER", "PRECEDES",
			"PRECEDING", "PRECISION", "PRECISION ", "PREFIX", "PREORDER", "PREPARE", "PREPARED", "PRESERVE", "PREV",
			"PRIMARY", "PRINT", "PRIOR", "PRIVILEGES", "PROC", "PROCEDURAL", "PROCEDURE", "PROCEDURE ", "PROCESSLIST",
			"PROFILE", "PROFILES", "PROXY", "PUBLIC", "PURGE", "QUARTER", "QUERY", "QUICK", "QUOTE", "RAISERROR",
			"RANGE", "RANGE ", "RANK", "READ", "READS", "READTEXT", "READ_ONLY", "READ_WRITE", "REAL", "REASSIGN",
			"REBUILD", "RECHECK", "RECONFIGURE", "RECOVER", "RECOVERY", "RECURSIVE", "REDOFILE", "REDO_BUFFER_SIZE",
			"REDUNDANT", "REF", "REFERENCES", "REFERENCING", "REGEXP ", "REGR_AVGX", "REGR_AVGY", "REGR_COUNT",
			"REGR_INTERCEPT", "REGR_R2", "REGR_SLOPE", "REGR_SXX", "REGR_SXY", "REGR_SYY", "REINDEX", "RELATIVE",
			"RELAY", "RELAYLOG", "RELAY_LOG_FILE", "RELAY_LOG_POS", "RELAY_THREAD", "RELEASE", "RELEASE ", "RELOAD",
			"REMOVE", "RENAME", "RENAME ", "REORGANIZE", "REPAIR", "REPEAT ", "REPEATABLE", "REPLACE", "REPLICA",
			"REPLICATE_DO_DB", "REPLICATE_DO_TABLE", "REPLICATE_REWRITE_DB", "REPLICATION", "REQUIRE", "REQUIRING",
			"RESET", "RESIGNAL", "RESPECT", "RESTART", "RESTORE", "RESTRICT", "RESTRICT ", "RESULT", "RESUME", "RETURN",
			"RETURNED_CARDINALITY", "RETURNED_LENGTH", "RETURNED_OCTET_LENGTH", "RETURNED_SQLSTATE", "RETURNING",
			"RETURNS", "REVERSE", "REVERT", "REVOKE", "REVOKE ", "RIGHT", "RLIKE", "ROLE", "ROLLBACK", "ROLLUP",
			"ROTATE", "ROUTINE", "ROUTINE_CATALOG", "ROUTINE_NAME", "ROUTINE_SCHEMA", "ROW", "ROWCOUNT", "ROWGUIDCOL",
			"ROWS", "ROW_COUNT", "ROW_FORMAT", "ROW_NUMBER", "RTREE", "RULE", "SAVE", "SAVEPOINT", "SCALE", "SCHEDULE",
			"SCHEMA", "SCHEMAS ", "SCHEMA_NAME", "SCOPE", "SCOPE_CATALOG", "SCOPE_NAME", "SCOPE_SCHEMA", "SCROLL",
			"SEARCH", "SECOND", "SECOND_MICROSECOND ", "SECTION", "SECURITY", "SECURITYAUDIT", "SELECT", "SELECTIVE",
			"SELF", "SEMANTICKEYPHRASETABLE", "SEMANTICSIMILARITYDETAILSTABLE", "SEMANTICSIMILARITYTABLE", "SENSITIVE",
			"SENSITIVE ", "SEPARATOR", "SEQUENCE", "SEQUENCES", "SERIAL", "SERIALIZABLE", "SERVER", "SERVER_NAME",
			"SESSION", "SESSION_USER", "SET", "SET ", "SETOF", "SETS", "SETUSER", "SHARE", "SHOW", "SHUTDOWN", "SIGNAL",
			"SIGNED", "SIMILAR", "SIMPLE", "SIZE", "SKIP", "SLAVE", "SLOW", "SMALLINT", "SMALLINT ", "SNAPSHOT",
			"SOCKET", "SOME", "SONAME", "SOUNDS", "SOURCE", "SPACE", "SPATIAL", "SPECIFIC", "SPECIFICTYPE",
			"SPECIFIC_NAME", "SQL", "SQL ", "SQLCA", "SQLCODE", "SQLERROR", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING",
			"SQLWARNING ", "SQL_AFTER_GTIDS", "SQL_AFTER_MTS_GAPS", "SQL_BEFORE_GTIDS", "SQL_BIG_RESULT",
			"SQL_BUFFER_RESULT", "SQL_CACHE", "SQL_CALC_FOUND_ROWS", "SQL_NO_CACHE", "SQL_SMALL_RESULT ", "SQL_THREAD",
			"SQL_TSI_DAY", "SQL_TSI_HOUR", "SQL_TSI_MINUTE", "SQL_TSI_MONTH", "SQL_TSI_QUARTER", "SQL_TSI_SECOND",
			"SQL_TSI_WEEK", "SQL_TSI_YEAR", "SQRT", "SSL", "STABLE", "STACKED", "STANDALONE", "START", "STARTING",
			"STARTS", "STATE", "STATEMENT", "STATIC", "STATISTICS", "STATS_AUTO_RECALC", "STATS_PERSISTENT",
			"STATS_SAMPLE_PAGES", "STATUS", "STDDEV_POP", "STDDEV_SAMP", "STDIN", "STDOUT", "STOP", "STORAGE",
			"STORED ", "STRAIGHT_JOIN", "STRICT", "STRING", "STRIP", "STRUCTURE", "STYLE", "SUBCLASS_ORIGIN", "SUBJECT",
			"SUBLIST", "SUBMULTISET", "SUBPARTITION", "SUBPARTITIONS", "SUBSTRING", "SUBSTRING_REGEX", "SUM", "SUPER",
			"SUPERUSER", "SUSPEND", "SWAPS", "SWITCHES", "SYMMETRIC", "SYSID", "SYSTEM", "SYSTEM_USER", "T", "TABLE",
			"TABLES", "TABLESAMPLE", "TABLESPACE", "TABLE_CHECKSUM", "TABLE_NAME", "TEMP", "TEMPLATE", "TEMPORARY",
			"TEMPTABLE", "TERMINATE", "TERMINATED ", "TEXT", "TEXTSIZE", "THAN", "THEN", "THEN ", "TIES", "TIME",
			"TIMESTAMP", "TIMESTAMPADD", "TIMESTAMPDIFF", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TINYBLOB", "TINYINT ",
			"TINYTEXT", "TO", "TOKEN", "TOP", "TOP_LEVEL_COUNT", "TRAILING", "TRAILING ", "TRAN", "TRANSACTION",
			"TRANSACTIONS_COMMITTED", "TRANSACTIONS_ROLLED_BACK", "TRANSACTION_ACTIVE", "TRANSFORM", "TRANSFORMS",
			"TRANSLATE", "TRANSLATE_REGEX", "TRANSLATION", "TREAT", "TRIGGER", "TRIGGERS", "TRIGGER_CATALOG",
			"TRIGGER_NAME", "TRIGGER_SCHEMA", "TRIM", "TRIM_ARRAY", "TRUE", "TRUNCATE", "TRUSTED", "TRY_CONVERT",
			"TSEQUAL", "TYPE", "TYPES", "UESCAPE", "UNBOUNDED", "UNCOMMITTED", "UNDEFINED", "UNDER", "UNDO", "UNDOFILE",
			"UNDO_BUFFER_SIZE", "UNENCRYPTED", "UNICODE", "UNINSTALL", "UNION", "UNION ", "UNIQUE", "UNKNOWN", "UNLINK",
			"UNLISTEN", "UNLOCK ", "UNNAMED", "UNNEST", "UNPIVOT", "UNSIGNED", "UNTIL", "UNTYPED", "UPDATE", "UPDATE ",
			"UPDATETEXT", "UPGRADE", "UPPER", "URI", "USAGE", "USE", "USE ", "USER", "USER_DEFINED_TYPE_CATALOG",
			"USER_DEFINED_TYPE_CODE", "USER_DEFINED_TYPE_NAME", "USER_DEFINED_TYPE_SCHEMA", "USER_RESOURCES", "USE_FRM",
			"USING", "UTC_DATE", "UTC_TIME ", "UTC_TIMESTAMP", "VACUUM", "VALID", "VALIDATION", "VALIDATOR", "VALUE",
			"VALUES", "VARBINARY", "VARCHAR", "VARCHAR ", "VARCHARACTER", "VARIABLE", "VARIABLES", "VARIADIC",
			"VARYING", "VARYING ", "VAR_POP", "VAR_SAMP", "VERBOSE", "VERSION", "VIEW", "VIRTUAL", "VOLATILE", "WAIT",
			"WAITFOR", "WARNINGS", "WEEK", "WEIGHT_STRING", "WHEN", "WHENEVER", "WHERE", "WHILE", "WHILE ",
			"WHITESPACE", "WIDTH_BUCKET", "WINDOW", "WITH", "WITHIN", "WITHIN GROUP", "WITHOUT", "WORK", "WRAPPER",
			"WRITE", "WRITETEXTYEAR", "X509", "XA", "XID", "XML", "XMLAGG", "XMLATTRIBUTES", "XMLBINARY", "XMLCAST",
			"XMLCOMMENT", "XMLCONCAT", "XMLDECLARATION", "XMLDOCUMENT", "XMLELEMENT", "XMLEXISTS", "XMLFOREST",
			"XMLITERATE", "XMLNAMESPACES", "XMLPARSE", "XMLPI", "XMLQUERY", "XMLROOT", "XMLSCHEMA", "XMLSERIALIZE",
			"XMLTABLE", "XMLTEXT", "XMLVALIDATE", "XOR", "YEAR", "YEAR_MONTH ", "YES", "ZEROFILL ", "ZONE" };

	/**
	 * Get the name of a column, based on the accessor name or the annotation,
	 * if present.
	 * 
	 * @param m
	 * @return the column name.
	 */
	public static String getColumnName(Method m)
	{

		if (m.isAnnotationPresent(ColumnName.class))
		{
			return m.getAnnotation(ColumnName.class).value().toUpperCase();
		}
		else
		{
			String methodName = m.getName();
			// assume the method starts with isXXX
			String res = methodName.substring(2);
			if (methodName.startsWith("get"))
			{
				// if instead it starts with getXXX, chop of the first letter
				res = res.substring(1);
			}
			StringBuilder candidateName = new StringBuilder(res.toUpperCase());
			while (isForbiddenColumnName(candidateName.toString()))
			{
				candidateName.append("_");
			}
			return candidateName.toString();
		}
	}


	/*
	 * Get a table name based on the canonical name of the class or annotation,
	 * if it exists.
	 */
	public static String getTableName(Object obj, AdapterBase adapter)
	{
		return getTableName(obj.getClass(), adapter);
	}

	/*
	 * Get a table name based on the canonical name of the class or annotation,
	 * if it exists.
	 */
	public static String getTableName(Class<?> c, AdapterBase adapter)
	{
		// use the annotation table name if it exists.
		if (c == null)
		{
			return null;
		}
		String res = null;
		TableName tn = c.getAnnotation(TableName.class);
		if (tn != null)
		{
			res = tn.value().toUpperCase();
		}
		else if (c.isArray())
		{
			res = Defaults.ARRAY_TABLENAME;
		}
		else
		{
			String name = c.getCanonicalName().toUpperCase();
			res = name.replaceAll("\\.", "_");
		}
		//ensure maximum length is respected, do not allow to start with underscore
		while (res.length() > adapter.getMaximumNameLength() || res.startsWith("_"))
		{
			res = res.substring(1);
		}
		if(adapter.getTableNamesAreLowerCase())
		{
			res = res.toLowerCase();
		}
		return res;
	}

	public static String getArrayMemberTableName(Class<?> compType,
			AdapterBase adapter)
	{
		String res = null;
		if (compType.isArray())
		{
			res = Defaults.ARRAY_MEMBER_TABLE_NAME_ARRAY;
		}
		else
		{
			String tableName = getTableName(compType,adapter);
			res = Defaults.ARRAY_MEMBER_TABLENAME + tableName;
			// make sure the name is not too long
			int count = 1;
			while (res.length() > adapter.getMaximumNameLength())
			{
				res = Defaults.ARRAY_MEMBER_TABLENAME
						+ tableName.substring(count);
				count++;
			}
		}
		if(adapter.getTableNamesAreLowerCase())
		{
			res = res.toLowerCase();
		}
		return res;
	}

	private static boolean isForbiddenColumnName(String columnName)
	{
		for (int x = 0; x < FORBIDDEN_COLUMN_NAMES.length; x++)
		{
			if (FORBIDDEN_COLUMN_NAMES[x].equals(columnName))
			{
				return true;
			}
		}
		return false;
	}

	/**
	 * Return a name that lets the classloader load the class. In the case of
	 * top-level classes this is just the canonical name.
	 * 
	 * @param clazz
	 * @return the unique name of the class.
	 */
	public static String getSystemicName(Class<?> clazz)
	{
		String res = clazz.getCanonicalName();
		if (clazz.getEnclosingClass() != null)
		{
			res = getSystemicName(clazz.getEnclosingClass()) + "$" + clazz.getSimpleName();
		}
		return res;
	}

	/**
	 * Get the name of the table that stores arrays.
	 * @param adapter
	 * @return
	 */
	public static String getArrayTablename(AdapterBase adapter)
	{
		String res = Defaults.ARRAY_TABLENAME;
		if(adapter.getTableNamesAreLowerCase())
		{
			res = res.toLowerCase();
		}
		return res;
	}

}
