/*******************************************************************************
 * Copyright (c) 2009, 2013 Erik Berglund.
 *   
 *      This file is part of Conserve.
 *   
 *       Conserve is free software: you can redistribute it and/or modify
 *       it under the terms of the GNU Lesser General Public License as published by
 *       the Free Software Foundation, either version 3 of the License, or
 *       (at your option) any later version.
 *   
 *       Conserve is distributed in the hope that it will be useful,
 *       but WITHOUT ANY WARRANTY; without even the implied warranty of
 *       MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *       GNU Lesser General Public License for more details.
 *   
 *       You should have received a copy of the GNU Lesser General Public License
 *       along with Conserve.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.conserve.adapter;

import java.lang.reflect.Method;
import java.sql.Blob;
import java.sql.Clob;

import org.conserve.Persist;
import org.conserve.annotations.MaxLength;
import org.conserve.tools.Defaults;
import org.conserve.tools.NameGenerator;

/**
 * Base adapter. This class is the base for all adapters. Adapters are objects
 * that generate native SQL code intended for a given database.
 * 
 * The base adapter should be adequate for almost all situations. Only override
 * this class if you are connecting to a database that requires non-standard
 * syntax.
 * 
 * @author Erik Berglund
 * 
 */
public class AdapterBase
{
	private Persist persist;

	public AdapterBase(Persist persist)
	{
		this.persist = persist;
	}

	/**
	 * Get the name of the JDBC driver this adapter is associated with.
	 * 
	 * @return a String, default is the empty string.
	 */
	public String getDriverName()
	{
		return "";
	}

	/**
	 * Return the field type of a given class.
	 * 
	 * @param c
	 *            the class.
	 * @param m
	 *            the method that returns the object.
	 * @return a String representation of the SQL data type of the column
	 *         representation of the given class.
	 */
	public String getColumnType(Class<?> c, Method m)
	{

		if (c.equals(boolean.class) || c.equals(Boolean.class))
		{
			return getBooleanTypeKeyword();
		}
		else if (c.equals(byte.class) || c.equals(Byte.class))
		{
			return getByteTypeKeyword();
		}
		else if (c.equals(short.class) || c.equals(Short.class))
		{
			return getShortTypeKeyword();
		}
		else if (c.equals(char.class) || c.equals(Character.class))
		{
			return getCharacterTypeKeyword();
		}
		else if (c.equals(int.class) || c.equals(Integer.class))
		{
			return getIntegerTypeKeyword();
		}
		else if (c.equals(long.class) || c.equals(Long.class))
		{
			return getLongTypeKeyword();
		}
		else if (c.equals(float.class) || c.equals(Float.class))
		{
			return getFloatTypeKeyword();
		}
		else if (c.equals(double.class) || c.equals(Double.class))
		{
			return getDoubleTypeKeyword();
		}
		else if (c.equals(String.class))
		{
			if (m != null && m.isAnnotationPresent(MaxLength.class))
			{
				int length = m.getAnnotation(MaxLength.class).value();
				return getVarCharKeyword(length);
			}
			else
			{
				return getVarCharKeyword();
			}
		}
		else if (c.isEnum())
		{
			return getVarCharIndexed();
		}
		else if (c.equals(java.sql.Date.class))
		{
			return getDateKeyword();
		}
		else if (c.equals(java.sql.Time.class))
		{
			return getTimeKeyword();
		}
		else if (c.equals(java.sql.Timestamp.class))
		{
			return getTimestampKeyword();
		}
		else if (c.equals(Clob.class))
		{
			return getClobKeyword();
		}
		else if (c.equals(Blob.class))
		{
			return getBlobTypeKeyword();
		}
		else
		{
			// handle all other objects by inserting a reference
			return getReferenceType(c);
		}
	}

	/**
	 * Get the keyword that indicates a column holds dates.
	 * 
	 * @return
	 */
	private String getDateKeyword()
	{
		return "DATE";
	}

	/**
	 * Get the keyword that indicates a column holds times.
	 * 
	 * @return
	 */
	private String getTimeKeyword()
	{
		return "TIME";
	}

	/**
	 * Get the keyword that indicates a column holds timestamps.
	 * 
	 * @return
	 */
	private String getTimestampKeyword()
	{
		return "TIMESTAMP";
	}

	public String getByteTypeKeyword()
	{
		return "TINYINT";
	}

	/**
	 * Get the SQL keyword that represents single-precision floating point
	 * numbers.
	 * 
	 * @return an SQL keyword.
	 */
	public String getFloatTypeKeyword()
	{
		return "FLOAT";
	}

	/**
	 * Get the SQL keyword that represents 64-bit (or larger) integers.
	 * 
	 * @return The keyword that indicates a 64-bit (or larger) integer.
	 */
	public String getLongTypeKeyword()
	{
		return "BIGINT";
	}

	/**
	 * Get the SQL keyword that represents double-precision floating point
	 * numbers.
	 * 
	 * @return an SQL keyword.
	 */
	public String getDoubleTypeKeyword()
	{
		return "DOUBLE";
	}

	/**
	 * Get the SQL keyword that represents a column with one single-precision
	 * integer.
	 * 
	 * @return the keyword used to indicate a type compatible with
	 *         java.lang.Integer.
	 */
	public String getIntegerTypeKeyword()
	{
		return "INTEGER";
	}

	/**
	 * Get the SQL keyword that represents a column with one character data.
	 * 
	 * @return the keyword used to indicate a type compatible with
	 *         java.lang.Character.
	 */
	public String getCharacterTypeKeyword()
	{
		return getIntegerTypeKeyword();
	}

	/**
	 * Get the SQL keyword that represents one byte.
	 * 
	 * @return the keyword used to indicate a type compatible with
	 *         java.lang.Byte.
	 */
	public String getShortTypeKeyword()
	{
		return getByteTypeKeyword();
	}

	public Persist getPersist()
	{
		return persist;
	}

	/**
	 * Define how objects of another class will be referenced
	 * 
	 * @param c
	 *            the class to reference.
	 * @return a string describing how objects of the given class will be
	 *         referenced.
	 */
	public String getReferenceType(Class<?> c)
	{
		if (c.isArray())
		{
			return getReferenceType(Defaults.ARRAY_TABLENAME);
		}
		else
		{
			return getReferenceType(NameGenerator.getTableName(c, this));
		}
	}

	/**
	 * Define how another table will be referenced
	 * 
	 * @param tableName
	 *            the name of the table to reference.
	 * @return a string describing how rows in the given table will be
	 *         referenced.
	 */
	public String getReferenceType(String tableName)
	{
		return getLongTypeKeyword();
	}

	/**
	 * Return a string describing the SQL varchar column definition.
	 * 
	 */
	public String getVarCharKeyword()
	{
		return "VARCHAR";
	}

	/**
	 * Return a string describing the SQL varchar column definition, with a
	 * defined length.
	 * 
	 * 
	 * @param length
	 * @return the VARCHAR keyword, with appropriate length modifier.
	 */
	public String getVarCharKeyword(int length)
	{
		return "VARCHAR(" + Integer.toString(length) + ")";
	}

	/**
	 * Some SQL implementations demand that a key length is specified. If so,
	 * override this method and indicate it.
	 * 
	 * @return a parenthesis-enclosed numerical value (e.g. "(500)") or an empty
	 *         string.
	 */
	public String getKeyLength()
	{
		return "";
	}

	/**
	 * Get whether triggers are supported.
	 * 
	 * @return true if triggers are supported, false otherwise.
	 */
	public boolean isSupportsTriggers()
	{
		return true;
	}

	/**
	 * Get whether auto-incrementing columns are supported.
	 * 
	 * @return true if auto-increment is supported, false otherwise.
	 */
	public boolean isSupportsIdentity()
	{
		return true;
	}

	/**
	 * Get the string used during table creation to indicate that a column
	 * should auto-increment.
	 * 
	 * @return a string.
	 */
	public String getIdentity()
	{
		return "BIGINT IDENTITY";
	}

	/**
	 * Get the string that, along with parentheses, is used to get the last
	 * added auto-incrementing id.
	 * 
	 * @return the name of the function without parentheses.
	 */
	public String getLastInsertedIdentity(String tableName)
	{
		return "SELECT IDENTITY()";
	}

	/**
	 * Check if a name can be used as column name.
	 */
	public boolean isValidColumnName(String name)
	{
		// default is allow all.
		return true;
	}

	/**
	 * Check if database supports the EXISTS keyword.
	 */
	public boolean isSupportsExistsKeyword()
	{
		// default is to support the EXISTS keyword.
		return true;
	}

	/**
	 * Return true if the database internally handles column names as lower
	 * case.
	 * 
	 * @return true if TABLE_NAME should be converted to table_name before use.
	 */
	public boolean tableNamesAreLowerCase()
	{
		return false;
	}

	/**
	 * True return values indicate that a transaction must be committed after
	 * creating a table and before a table can be used.
	 * 
	 * The default is false;
	 * 
	 * @return true or false;
	 */
	public boolean isRequiresCommitAfterTableCreation()
	{
		return false;
	}

	/**
	 * Get the keyword used in JOIN-statements.
	 * 
	 * @return the JOIN keyword.
	 */
	public Object getJoinKeyword()
	{
		return ",";
	}

	/**
	 * Returns true iff this database handles insert statements of the type
	 * "insert into TABLE () values ()", false otherwise.
	 * 
	 * @return true if empty insert statements can be used.
	 */
	public boolean allowsEmptyStatements()
	{
		return true;
	}

	/**
	 * Return true if the Character Large OBject (CLOB) data type is fully
	 * supported. Default is true.
	 * 
	 * @return true if CLOB is supported.
	 */
	public boolean isSupportsClob()
	{
		return true;
	}

	/**
	 * Get the keyword used to denote a CLOB type.
	 * 
	 * @return the CLOB keyword.
	 */
	public String getClobKeyword()
	{
		return "CLOB";
	}

	/**
	 * Return true if the Binary Large OBject (BLOB) data type is fully
	 * supported. Default is true.
	 * 
	 * @return true if BLOB is supported.
	 */
	public boolean isSupportsBlob()
	{
		return true;
	}

	/**
	 * Get the keyword used to denote a BLOB type.
	 * 
	 * @return the BLOB keyword.
	 */
	public String getBlobTypeKeyword()
	{
		return "BLOB";
	}

	/**
	 * Get the command used to shut down the database engine (usual in cases of
	 * embedded databases). This will, if non-null, be executed in SQL
	 * statement. The default implementation returns null.
	 * 
	 * @return the shutdown command.
	 */
	public String getShutdownCommand()
	{
		return null;
	}

	/**
	 * Get the command used to shut down the database engine (usual in cases of
	 * embedded databases) in the driverManager.
	 * 
	 * This will, if non-null, be executed in a DriverManager.getConnection(...)
	 * statement. The default implementation returns null.
	 * 
	 * @return the shutdown command.
	 */
	public String getDriverManagerShutdownCommand()
	{
		return null;
	}

	/**
	 * Get the varchar for indexed varchar arrays, as this may be different from
	 * the max size of non-indexed varchars.
	 * 
	 * @return the definition for varchar columns.
	 */
	public String getVarCharIndexed()
	{
		return getVarCharKeyword();
	}

	/**
	 * Return whether the database supports the LIMIT and OFFSET keywords. These
	 * are convenience keywords, and non-complying databases will be assumed to
	 * use the more verbose ROW_NUM method. The default is true, since most
	 * databases support these keywords.
	 * 
	 * @return true if the database supports LIMIT and OFFSET, false otherwise.
	 */
	public boolean isSupportsLimitOffsetKeywords()
	{
		return true;
	}

	/**
	 * Get the string to append to the query to limit the number of returned
	 * rows. The ? will be replaced with the actual limit.
	 * 
	 * @return the query return limit/fetch string.
	 */
	public String getLimitString()
	{
		return "LIMIT ?";
	}

	/**
	 * Get the string to append to the query to skip a number of returned rows.
	 * The ? will be replaced with the actual number of rows to skip.
	 * 
	 * @return the query return offset string.
	 */
	public String getOffsetString()
	{
		return "OFFSET ?";
	}

	/**
	 * Do we put LIMIT/FETCH or OFFSET first?
	 * 
	 * @return true if LIMIT/FETCH goes before OFFSET.
	 */
	public boolean isPutLimitBeforeOffset()
	{
		return true;
	}

	/**
	 * Do we put limit/offset before the list of columns?
	 * 
	 * @return true if "select limit n offset m col1, col2, where... is the
	 *         correct form, false otherwise.
	 */
	public boolean isPutLimitOffsetBeforeColumns()
	{
		return false;
	}

	/**
	 * Return true if we can use SELECT
	 * DISTINCT(NON_BLOB_OR_CLOB_COLUMN),BLOB_OR_CLOB_COlUMN ... in queries,
	 * false otherwise.
	 */
	public boolean handlesDistinctWithClobsAndBlobsCorrectly()
	{
		return true;
	}

	/**
	 * Get the keyword used to indicate that a column is to store values of type
	 * boolean.
	 * 
	 */
	public String getBooleanTypeKeyword()
	{
		return "BOOLEAN";
	}

	/**
	 * Return the maximum length of a table, procedure, index, or sequence name.
	 * 
	 * @return Get the maximum length of a name.
	 */
	public int getMaximumNameLength()
	{
		return Integer.MAX_VALUE;
	}

	/**
	 * Check if the row size is limited. Default implementation returns false.
	 * 
	 * If this method returns true it will trigger the engine to query the
	 * maximum size of the row.
	 * 
	 * @see #getMaximumRowSize()
	 * 
	 * @return true if there is a limit to the size of a row.
	 */
	public boolean isRowSizeLimited()
	{
		return false;
	}

	/**
	 * This method is ONLY called if isRowSizeLimited() returns true. If
	 * isRowSizeLimited() returns false, the value this method returns is of no
	 * consequence.
	 * 
	 * @see #isRowSizeLimited()
	 * 
	 * @return the maximum size, in bytes, of one row.
	 */
	public int getMaximumRowSize()
	{
		return 0;
	}

	/**
	 * Return true if the underlying database engine can rename a column.
	 * 
	 * @return false if column renaming is not supported.
	 */
	public boolean canRenameColumn()
	{
		return true;
	}

	/**
	 * Create an SQL statement that will rename a column. The table name, old
	 * column name, and new column name are placeholders.
	 * 
	 * @return the SQL statement used to rename a column.
	 */
	public String getRenameColumnStatement()
	{
		StringBuilder statement = new StringBuilder("ALTER TABLE ");
		statement.append(Defaults.TABLENAME_PLACEHOLDER);
		statement.append(" ALTER COLUMN ");
		statement.append(Defaults.OLD_COLUMN_NAME_PLACEHOLDER);
		statement.append(" RENAME TO ");
		statement.append(Defaults.NEW_COLUMN_NAME_PLACEHOLDER);
		return statement.toString();
	}

	/**
	 * Check if the two column types are considered equal by this DB engine.
	 * 
	 * @param columnType
	 * @param value
	 * @return
	 */
	public boolean columnTypesEqual(String columnType, String value)
	{

		return columnType.equalsIgnoreCase(value);
	}

	/**
	 * Get the statement to rename a table.
	 * 
	 * @param oldTableName
	 *            the name of the table to rename.
	 * @param newTableName
	 *            the new name of the table.
	 */
	public String getTableRenameStatement(String oldTableName, String newTableName)
	{
		StringBuilder sb = new StringBuilder("ALTER TABLE ");
		sb.append(oldTableName);
		sb.append(" RENAME TO ");
		sb.append(newTableName);
		return sb.toString();

	}

	/**
	 * If the underlying database can change the type of a column, return true.
	 * Otherwise, return false. This method also returns true if the underlying
	 * database can perform some, but not all, column type changes, as long as
	 * the allowed changes include all changes used by Conserve.
	 * 
	 * 
	 * @return
	 */
	public boolean canChangeColumnType()
	{
		return true;
	}

	/**
	 * Return true if the underlying database allows us to drop a column, false otherwise.
	 * @return
	 */
	public boolean canDropColumn()
	{
		return true;
	}

	/**
	 * Get the statement for dropping an index from a table.
	 * 
	 * @param table the table to drop the index from
	 * @param indexName the index to drop.
	 * @return
	 */
	public String getDropIndexStatement(String table, String indexName)
	{
		StringBuilder sb = new StringBuilder("DROP INDEX IF EXISTS ");
		sb.append(indexName);
		return sb.toString();
	}

	/**
	 * Return true if the underlying database can handle joins in UPDATE statements.
	 * @return
	 */
	public boolean isSupportsJoinInUpdate()
	{
		return true;
	}
}
