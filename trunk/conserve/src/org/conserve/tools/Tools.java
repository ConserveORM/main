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
package org.conserve.tools;

import java.io.CharArrayReader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.conserve.adapter.AdapterBase;

public class Tools
{
	private static final Logger LOGGER = Logger.getLogger("org.conserve");

	/**
	 * Fill in the values of the prepared statement, based on the class of the argument.
	 * @param ps
	 * @param i
	 * @param value
	 * @throws SQLException 
	 */
	public static void setParameter(PreparedStatement ps, Class<?>clazz,  int i, Object value) throws SQLException
	{
		if (clazz.isPrimitive())
		{
			if (clazz.equals(boolean.class))
			{
				ps.setBoolean(i, (Boolean)value);
			}
			else if (clazz.equals(byte.class))
			{
				ps.setByte(i, (Byte)value);
			}
			else if (clazz.equals(short.class))
			{
				ps.setShort(i, (Short)value);
			}
			else if (clazz.equals(char.class))
			{
				ps.setInt(i, (Character)value);
			}
			else if (clazz.equals(int.class))
			{
				ps.setInt(i, (Integer)value);
			}
			else if (clazz.equals(long.class))
			{
				ps.setLong(i, (Long)value);
			}
			else if (clazz.equals(float.class))
			{
				ps.setFloat(i, (Float)value);
			}
			else if (clazz.equals(double.class))
			{
				ps.setDouble(i, (Double)value);
			}
		}
		else
		{
			if (clazz.equals(Boolean.class))
			{
				ps.setBoolean(i, (Boolean) value);
			}
			else if (clazz.equals(Byte.class))
			{
				ps.setByte(i, (Byte) value);
			}
			else if (clazz.equals(Short.class))
			{
				ps.setShort(i, (Short) value);
			}
			else if (clazz.equals(Character.class))
			{
				ps.setInt(i, (Character) value);
			}
			else if (clazz.equals(Integer.class))
			{
				ps.setInt(i, (Integer) value);
			}
			else if (clazz.equals(Long.class))
			{
				ps.setLong(i, (Long) value);
			}
			else if (clazz.equals(Float.class))
			{
				ps.setFloat(i, (Float)value);
			}
			else if (clazz.equals(Double.class))
			{
				ps.setDouble(i, (Double) value);
			}
			else if (clazz.equals(String.class) || clazz.equals(Enum.class))
			{
				ps.setString(i, value.toString());
			}
			else if (clazz.equals(java.sql.Date.class))
			{
				ps.setDate(i, (java.sql.Date)value);
			}
			else if (clazz.equals(java.sql.Time.class))
			{
				ps.setTime(i, (java.sql.Time)value);
			}
			else if (clazz.equals(java.sql.Timestamp.class))
			{
				ps.setTimestamp(i, (java.sql.Timestamp)value);
			}
			else if(clazz.equals(Clob.class))
			{
				CharArrayReader reader = new CharArrayReader((char[])value);
				ps.setCharacterStream(i, reader);
			}
			else if (clazz.equals(Blob.class))
			{
				ps.setBytes(i, (byte[])value);
			}
				
		}
		
	}

	/**
	 * Create the id sequence name for a given table name.
	 * @param tableName
	 * @return the name of the id sequence for the given table.
	 */
	public static String getSequenceName(String tableName,AdapterBase adapter)
	{
		String res= tableName+"__seq";
		while(res.length()>adapter.getMaximumNameLength() || res.startsWith("_"))
		{
			res = res.substring(1);
		}
		return res;
	}

	/**
	 * Create the id trigger name for a given table name.
	 * @param tableName
	 * @return the name of the id sequence for the given table.
	 */
	public static String getTriggerName(String tableName,AdapterBase adapter)
	{
		String res= tableName+"__tr";
		while(res.length()>adapter.getMaximumNameLength()|| res.startsWith("_"))
		{
			res = res.substring(1);
		}
		return res;
	}

	public static void logFine(PreparedStatement stmt)
	{
		if(LOGGER.isLoggable(Level.FINE))
		{
			LOGGER.fine(stmt.toString());
		}
	}
}