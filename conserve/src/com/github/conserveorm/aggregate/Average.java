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

import java.lang.reflect.Method;

import com.github.conserveorm.tools.generators.NameGenerator;
import com.github.conserveorm.tools.metadata.ObjectRepresentation;
import com.github.conserveorm.tools.metadata.ObjectStack;


/**
 * Subclass of AggregateFunction that represents the SQL AVG(...) function.
 * 
 * @author Erik Berglund
 *
 */
public class Average extends AggregateFunction
{

	/**
	 * @param fieldName
	 */
	public Average(String fieldName)
	{
		super(fieldName);
	}

	/**
	 * Override the default implementation so that we can add a cast to double
	 * in the query.
	 * 
	 * This causes the data type to be widened and/or cast to floating point for
	 * the maximum accuracy under Java.
	 * 
	 * Get the SQL string that represents the aggregate function on a field.
	 * 
	 * @param stack
	 *            the stack representing the type to query.
	 * @return a String that can be used as part of an SQL select statement.
	 * @throws SecurityException
	 * @throws NoSuchMethodException
	 */
	@Override
	public String getStringRepresentation(ObjectStack stack) throws NoSuchMethodException, SecurityException
	{
		Method method = stack.getActualRepresentation().getRepresentedClass().getMethod(methodName);
		String colname = NameGenerator.getColumnName(method);
		ObjectRepresentation rep = stack.getRepresentation(colname);

		StringBuilder res = new StringBuilder(getFunctionName());
		if (stack.getAdapter().averageRequiresCast())
		{
			res.append("(CAST(");
			res.append(rep.getAsName());
			res.append(".");
			res.append(colname);
			res.append(" AS ");
			res.append(stack.getAdapter().getDoubleTypeKeyword());
			res.append("))");
		}
		else
		{

			res.append("(");
			res.append(rep.getAsName());
			res.append(".");
			res.append(colname);
			res.append(")");
		}
		return res.toString();
	}

	/**
	 * @see com.github.conserveorm.aggregate.AggregateFunction#getFunctionName()
	 */
	@Override
	protected String getFunctionName()
	{
		return "AVG";
	}

	/**
	 * @see com.github.conserveorm.aggregate.AggregateFunction#translateReturnType(java.lang.Class)
	 */
	@Override
	protected Class<? extends Number> translateReturnType(Class<?> clazz)
	{
		// averages are always Double
		return Double.class;
	}

}
