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
package org.conserve.tools;

import java.io.CharArrayReader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.conserve.adapter.AdapterBase;

public class Tools
{
	private static final Logger LOGGER = Logger.getLogger("org.conserve");

	/**
	 * Fill in the values of the prepared statement, based on the class of the
	 * argument.
	 * 
	 * @param ps
	 * @param i
	 * @param value
	 * @throws SQLException
	 */
	public static void setParameter(PreparedStatement ps, Class<?> clazz, int i, Object value) throws SQLException
	{
		if (clazz.isPrimitive())
		{
			if (clazz.equals(boolean.class))
			{
				ps.setBoolean(i, (Boolean) value);
			} else if (clazz.equals(byte.class))
			{
				ps.setByte(i, (Byte) value);
			} else if (clazz.equals(short.class))
			{
				ps.setShort(i, (Short) value);
			} else if (clazz.equals(char.class))
			{
				ps.setInt(i, (Character) value);
			} else if (clazz.equals(int.class))
			{
				ps.setInt(i, (Integer) value);
			} else if (clazz.equals(long.class))
			{
				ps.setLong(i, (Long) value);
			} else if (clazz.equals(float.class))
			{
				ps.setFloat(i, (Float) value);
			} else if (clazz.equals(double.class))
			{
				ps.setDouble(i, (Double) value);
			}
		} else
		{
			if (clazz.equals(Boolean.class))
			{
				ps.setBoolean(i, (Boolean) value);
			} else if (clazz.equals(Byte.class))
			{
				ps.setByte(i, (Byte) value);
			} else if (clazz.equals(Short.class))
			{
				ps.setShort(i, (Short) value);
			} else if (clazz.equals(Character.class))
			{
				ps.setInt(i, (Character) value);
			} else if (clazz.equals(Integer.class))
			{
				ps.setInt(i, (Integer) value);
			} else if (clazz.equals(Long.class))
			{
				ps.setLong(i, (Long) value);
			} else if (clazz.equals(Float.class))
			{
				ps.setFloat(i, (Float) value);
			} else if (clazz.equals(Double.class))
			{
				ps.setDouble(i, (Double) value);
			} else if (clazz.equals(String.class) || clazz.equals(Enum.class))
			{
				ps.setString(i, value.toString());
			} else if (clazz.equals(java.sql.Date.class))
			{
				ps.setDate(i, (java.sql.Date) value);
			} else if (clazz.equals(java.sql.Time.class))
			{
				ps.setTime(i, (java.sql.Time) value);
			} else if (clazz.equals(java.sql.Timestamp.class))
			{
				ps.setTimestamp(i, (java.sql.Timestamp) value);
			} else if (clazz.equals(Clob.class))
			{
				CharArrayReader reader = new CharArrayReader((char[]) value);
				ps.setCharacterStream(i, reader);
			} else if (clazz.equals(Blob.class))
			{
				ps.setBytes(i, (byte[]) value);
			}

		}

	}

	/**
	 * Create the id sequence name for a given table name.
	 * 
	 * @param tableName
	 * @return the name of the id sequence for the given table.
	 */
	public static String getSequenceName(String tableName, AdapterBase adapter)
	{
		String res = tableName + "__seq";
		while (res.length() > adapter.getMaximumNameLength() || res.startsWith("_"))
		{
			res = res.substring(1);
		}
		return res;
	}

	/**
	 * Create the id trigger name for a given table name.
	 * 
	 * @param tableName
	 * @return the name of the id sequence for the given table.
	 */
	public static String getTriggerName(String tableName, AdapterBase adapter)
	{
		String res = tableName + "__tr";
		while (res.length() > adapter.getMaximumNameLength() || res.startsWith("_"))
		{
			res = res.substring(1);
		}
		return res;
	}

	public static void logFine(PreparedStatement stmt)
	{
		if (LOGGER.isLoggable(Level.FINE))
		{
			LOGGER.fine(stmt.toString());
		}
	}

	/**
	 * Copy value i from ResultSet source to value j in PreparedStatement dest.
	 * 
	 * @param source
	 * @param i
	 * @param dest
	 * @param j
	 * @throws SQLException
	 */
	public static void copyValue(ResultSet source, int i, PreparedStatement dest, int j) throws SQLException
	{
		ResultSetMetaData metaData = source.getMetaData();
		int colType = metaData.getColumnType(i);
		switch (colType)
		{
		case Types.BIGINT:
			dest.setLong(j, source.getLong(i));
			break;
		case Types.INTEGER:
		case Types.SMALLINT:
		case Types.TINYINT:
			dest.setInt(j, source.getInt(i));
			break;
		case Types.DOUBLE:
			dest.setDouble(j, source.getDouble(i));
			break;
		case Types.FLOAT:
			dest.setFloat(j, source.getFloat(i));
			break;
		case Types.BLOB:
			dest.setBlob(j, source.getBlob(i));
			break;
		case Types.BOOLEAN:
			dest.setBoolean(j, source.getBoolean(i));
			break;
		case Types.CLOB:
			dest.setClob(j, source.getClob(i));
			break;
		case Types.DATE:
			dest.setDate(j, source.getDate(i));
			break;
		case Types.DECIMAL:
			dest.setBigDecimal(j, source.getBigDecimal(i));
			break;
		case Types.LONGNVARCHAR:
		case Types.LONGVARCHAR:
		case Types.VARCHAR:
		case Types.NVARCHAR:
			dest.setString(j, source.getString(i));
			break;
		case Types.TIME:
		case Types.TIME_WITH_TIMEZONE:
			dest.setTime(j, source.getTime(i));
			break;
		case Types.TIMESTAMP:
		case Types.TIMESTAMP_WITH_TIMEZONE:
			dest.setTimestamp(j, source.getTimestamp(i));
			break;

		default:
			throw new SQLException("Don't know how to handle data type: " + colType);
		}

	}
}
