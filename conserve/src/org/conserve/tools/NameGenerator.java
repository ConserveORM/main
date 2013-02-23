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

import java.lang.reflect.Method;

import org.conserve.adapter.AdapterBase;
import org.conserve.annotations.ColumnName;
import org.conserve.annotations.TableName;

/**
 * Get the database name (column name, table name) for various entities.
 * 
 * @author Erik Berglund
 * 
 */
public class NameGenerator
{
	private static final String[] FORBIDDEN_COLUMN_NAMES = new String[] { "IS",
			"IF", "AS", "ON", "IN", "OF", "OR", "AND", "NOT", "NULL", "FROM",
			"AVG", "COUNT", "VALUE", "TIME", "DATE","TIMESTAMP","YEAR","MONTH","FIRST","LIMIT","SKIP","LOCALTIME","ARRAY" };

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
			else if (methodName.startsWith("set"))
			{
				// if instead it starts with setXXX, chop of the first letter
				res = res.substring(1);
				// find the matching accessor in the hirearchy tree
				Method accessor = getAccessor(m.getDeclaringClass(), res);
				if (accessor != null)
				{
					res = getColumnName(accessor);
				}
			}
			StringBuilder candidateName = new StringBuilder(res.toUpperCase());
			while (isForbiddenColumnName(candidateName.toString()))
			{
				candidateName.append("_");
			}
			return candidateName.toString();
		}
	}

	private static Method getAccessor(Class<?> declaringClass, String subName)
	{
		Method res = null;
		if (declaringClass != null)
		{
			Method[] methods = declaringClass.getDeclaredMethods();
			for (Method m : methods)
			{
				if (m.getName().compareTo("is" + subName) == 0
						|| m.getName().compareTo("get" + subName) == 0)
				{
					res = m;
					break;
				}
			}
			if (res == null)
			{
				res = getAccessor(declaringClass.getSuperclass(), subName);
			}
		}
		return res;
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
		//ensure maximum lenght is respected, do not allow to start with underscore
		while (res.length() > adapter.getMaximumNameLength() || res.startsWith("_"))
		{
			res = res.substring(1);
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

}
