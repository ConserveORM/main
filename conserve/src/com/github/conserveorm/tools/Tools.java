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
package com.github.conserveorm.tools;

import java.io.CharArrayReader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import com.github.conserveorm.adapter.AdapterBase;

public class Tools
{
	private static final Logger LOGGER = Logger.getLogger(Defaults.LOGGER_NAME);

	/**
	 * Fill in the values of the prepared statement, based on the class of the
	 * argument.
	 * 
	 * @param ps
	 * @param i
	 * @param value
	 * @throws SQLException
	 */
	public static void setParameter(PreparedStatement ps, Class<?> clazz, int i,
			Object value, AdapterBase adapter) throws SQLException
	{
		if (clazz.equals(Boolean.class) || clazz.equals(boolean.class))
		{
			if (!adapter.setBooleanIsBroken())
			{
				ps.setBoolean(i, (Boolean) value);
			}
			else
			{
				// the database engine can't handle setBoolean properly,
				// circumvent it by converting to int.
				ps.setInt(i, ((Boolean) value) ? 1 : 0);
			}
		}
		else if (clazz.equals(Byte.class) || clazz.equals(byte.class))
		{
			ps.setByte(i, (Byte) value);
		}
		else if (clazz.equals(Short.class) || clazz.equals(short.class))
		{
			ps.setShort(i, (Short) value);
		}
		else if (clazz.equals(Character.class) || clazz.equals(char.class))
		{
			ps.setInt(i, (Character) value);
		}
		else if (clazz.equals(Integer.class) || clazz.equals(int.class))
		{
			ps.setInt(i, (Integer) value);
		}
		else if (clazz.equals(Long.class) || clazz.equals(long.class))
		{
			ps.setLong(i, (Long) value);
		}
		else if (clazz.equals(Float.class) || clazz.equals(float.class))
		{
			ps.setFloat(i, (Float) value);
		}
		else if (clazz.equals(Double.class) || clazz.equals(double.class))
		{
			ps.setDouble(i, (Double) value);
		}
		else if (clazz.equals(String.class) || clazz.equals(Enum.class))
		{
			ps.setString(i, value.toString());
		}
		else if (clazz.equals(java.sql.Date.class))
		{
			ps.setDate(i, (java.sql.Date) value);
		}
		else if (clazz.equals(java.sql.Time.class))
		{
			ps.setTime(i, (java.sql.Time) value);
		}
		else if (clazz.equals(java.sql.Timestamp.class))
		{
			ps.setTimestamp(i, (java.sql.Timestamp) value);
		}
		else if (clazz.equals(Clob.class))
		{
			char[] array = (char[]) value;
			CharArrayReader reader = new CharArrayReader(array);
			ps.setCharacterStream(i, reader, array.length);
		}
		else if (clazz.equals(Blob.class))
		{
			ps.setBytes(i, (byte[]) value);
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
		String res = tableName + "__SEQ";
		if(adapter.getTableNamesAreLowerCase())
		{
			res = res.toLowerCase();
		}
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
			LogRecord lr = new Tools.SqlStatementLogRecord(Level.FINE, stmt.toString());
			LOGGER.log(lr);
		}
	}
	
	public static void logFine(String message)
	{
		if (LOGGER.isLoggable(Level.FINE))
		{
			LogRecord lr = new Tools.SqlStatementLogRecord(Level.FINE, message);
			LOGGER.log(lr);
		}
	}

	/**
	 * A subclass of LogRecord that allows Tools.logFine to print the name of
	 * the calling method instead of itself.
	 * 
	 * This class contains a lot of code cut-n-pasted from LogRecord, since we
	 * need to replace some private methods and that can only be done by
	 * overriding the public methods that call them.
	 * 
	 * @author Erik Berglund
	 *
	 */
	private static class SqlStatementLogRecord extends LogRecord
	{
		private boolean needToInferCaller = true;
		private String sourceClassName;
		private String sourceMethodName;
		/**
		 * 
		 */
		private static final long serialVersionUID = 8482668962889077710L;

		/**
		 * @param level
		 * @param msg
		 */
		public SqlStatementLogRecord(Level level, String msg)
		{
			super(level, msg);
		}

		/**
		 * Get the name of the class that (allegedly) issued the logging
		 * request.
		 * <p>
		 * Note that this sourceClassName is not verified and may be spoofed.
		 * This information may either have been provided as part of the logging
		 * call, or it may have been inferred automatically by the logging
		 * framework. In the latter case, the information may only be
		 * approximate and may in fact describe an earlier call on the stack
		 * frame.
		 * <p>
		 * May be null if no information could be obtained.
		 *
		 * @return the source class name
		 */
		public String getSourceClassName()
		{
			if (needToInferCaller)
			{
				inferCaller();
			}
			return sourceClassName;
		}

		/**
		 * Set the name of the class that (allegedly) issued the logging
		 * request.
		 *
		 * @param sourceClassName
		 *            the source class name (may be null)
		 */
		public void setSourceClassName(String sourceClassName)
		{
			this.sourceClassName = sourceClassName;
			needToInferCaller = false;
		}

		/**
		 * Get the name of the method that (allegedly) issued the logging
		 * request.
		 * <p>
		 * Note that this sourceMethodName is not verified and may be spoofed.
		 * This information may either have been provided as part of the logging
		 * call, or it may have been inferred automatically by the logging
		 * framework. In the latter case, the information may only be
		 * approximate and may in fact describe an earlier call on the stack
		 * frame.
		 * <p>
		 * May be null if no information could be obtained.
		 *
		 * @return the source method name
		 */
		public String getSourceMethodName()
		{
			if (needToInferCaller)
			{
				inferCaller();
			}
			return sourceMethodName;
		}

		/**
		 * Set the name of the method that (allegedly) issued the logging
		 * request.
		 *
		 * @param sourceMethodName
		 *            the source method name (may be null)
		 */
		public void setSourceMethodName(String sourceMethodName)
		{
			this.sourceMethodName = sourceMethodName;
			needToInferCaller = false;
		}

		// Private method to infer the caller's class and method names
		private void inferCaller()
		{
			needToInferCaller = false;
			StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
			int depth = stackTrace.length;

			boolean lookingForLogger = true;
			for (int ix = 0; ix < depth; ix++)
			{
				// Calling getStackTraceElement directly prevents the VM
				// from paying the cost of building the entire stack frame.
				StackTraceElement frame = stackTrace[ix];
				String cname = frame.getClassName();
				boolean isLoggerImpl = isLoggerImplFrame(cname);
				if (lookingForLogger)
				{
					// Skip all frames until we have found the first logger
					// frame.
					if (isLoggerImpl)
					{
						lookingForLogger = false;
					}
				}
				else
				{
					if (!isLoggerImpl)
					{
						// skip reflection call
						if (!cname.startsWith("java.lang.reflect.") && !cname.startsWith("sun.reflect."))
						{
							// We've found the relevant frame.
							setSourceClassName(cname);
							setSourceMethodName(frame.getMethodName());
							return;
						}
					}
				}
			}
			// We haven't found a suitable frame, so just punt. This is
			// OK as we are only committed to making a "best effort" here.
		}

		private boolean isLoggerImplFrame(String cname)
		{
			// the log record could be created for a platform logger
			//this is where we've changed from the original LogRecord implementation:
			return (cname.equals("java.util.logging.Logger") || cname.equals("com.github.conserveorm.tools.Tools") || cname.startsWith("java.util.logging.LoggingProxyImpl")
					|| cname.startsWith("sun.util.logging."));
		}
	}

}
