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
package com.github.conserveorm.aggregate;

/**
 * AggregateFunction subclass that implements the SQL SUM(...) function.
 * 
 * @author Erik Berglund
 *
 */
public class Sum extends AggregateFunction
{

	/**
	 * @param fieldName
	 */
	public Sum(String fieldName)
	{
		super(fieldName);
	}

	/**
	 * @see com.github.conserveorm.aggregate.AggregateFunction#getFunctionName()
	 */
	@Override
	protected String getFunctionName()
	{
		return "SUM";
	}

	/**
	 * @see com.github.conserveorm.aggregate.AggregateFunction#translateReturnType(java.lang.Class)
	 */
	@Override
	protected Class<? extends Number> translateReturnType(Class<?> clazz)
	{
		Class<? extends Number> res = null;
		// all integer type results are widened to Long
		if (clazz == Long.class || clazz == Integer.class || clazz == Short.class || clazz == Byte.class || clazz == Character.class
				|| clazz == long.class || clazz == int.class || clazz == short.class || clazz == byte.class || clazz == char.class)
		{
			res = Long.class;
		}
		// all floating point results are widened to Double
		else if (clazz == Double.class || clazz == Float.class || clazz == double.class || clazz == float.class)
		{
			res = Double.class;
		}
		return res;
	}
}
