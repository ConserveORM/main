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
 * Class that represent one of the SQL aggregate functions.
 * 
 * @author Erik Berglund
 * 
 */
public abstract class AggregateFunction
{
	protected String methodName;

	/**
	 * Constructor that names takes name of method to aggregate over, e.g.
	 * "getFoo", "getBar".
	 * 
	 * @param methodName
	 *            the name of the function to aggregate over.
	 */
	public AggregateFunction(String methodName)
	{
		this.methodName = methodName;
	}


	/**
	 * Get the SQL string that represents the aggregate function on a field.
	 * 
	 * @param stack
	 *            the stack representing the type to query.
	 * @return a String that can be used as part of an SQL select statement.
	 * @throws SecurityException
	 * @throws NoSuchMethodException
	 */
	public String getStringRepresentation(ObjectStack stack) throws NoSuchMethodException, SecurityException
	{
		StringBuilder res = new StringBuilder(getFunctionName());
		if (methodName != null)
		{
			//count specific row
			Method method = stack.getActualRepresentation().getRepresentedClass().getMethod(methodName);
			String colname = NameGenerator.getColumnName(method);
			ObjectRepresentation rep = stack.getRepresentation(colname);
			res.append("(");
			res.append(rep.getAsName());
			res.append(".");
			res.append(colname);
			res.append(")");
		}
		else
		{
			//no name given, use wildcard
			res.append("(*)");
		}
		return res.toString();
	}

	/**
	 * Find the return type of this type of operation on the particular field
	 * and object.
	 * 
	 * @param stack
	 * @throws NoSuchMethodException
	 * @throws SecurityException
	 */
	public Class<? extends Number> getReturnType(ObjectStack stack) throws NoSuchMethodException, SecurityException
	{
		Class<?> clazz = null;
		if (methodName != null)
		{
			Method method = stack.getActualRepresentation().getRepresentedClass().getMethod(methodName);
			String colname = NameGenerator.getColumnName(method);
			ObjectRepresentation rep = stack.getRepresentation(colname);
			clazz = rep.getReturnType(colname);
		}
		//if methodName is null, hopefully the subclass will handle it correctly
		return translateReturnType(clazz);
	}

	/**
	 * Return a class that indicates how the specific return type will be
	 * handled by the aggregate function.
	 * 
	 * Usually, the result will be subject to a widening cast or no cast at all,
	 * e.g. Float will return Float or Double.
	 * 
	 * @param clazz
	 */
	protected abstract Class<? extends Number> translateReturnType(Class<?> clazz);

	/**
	 * Get the name of the SQL aggregate function this class represents.
	 */
	protected abstract String getFunctionName();
}
