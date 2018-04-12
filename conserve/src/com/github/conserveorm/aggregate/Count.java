/*******************************************************************************
 *  
 * Copyright (c) 2009, 2018 Erik Berglund.
 *    
 *       This file is part of Conserve.
 *   
 *       Conserve is free software: you can redistribute it and/or modify
 *       it under the terms of the GNU Affero General Public License as published by
 *       the Free Software Foundation, either version 3 of the License, or
 *       (at your option) any later version.
 *   
 *       Conserve is distributed in the hope that it will be useful,
 *       but WITHOUT ANY WARRANTY; without even the implied warranty of
 *       MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *       GNU Affero General Public License for more details.
 *   
 *       You should have received a copy of the GNU Affero General Public License
 *       along with Conserve.  If not, see <https://www.gnu.org/licenses/agpl.html>.
 *       
 *******************************************************************************/
package com.github.conserveorm.aggregate;


/**
 * @author Erik Berglund
 * 
 */
public class Count extends AggregateFunction
{

	/**
	 * Default constructor that generates a count of number of rows returned,
	 * not a specific field.
	 */
	public Count()
	{
		super(null);
	}

	/**
	 * @param methodName
	 */
	public Count(String methodName)
	{
		super(methodName);
	}

	/**
	 * @see com.github.conserveorm.aggregate.AggregateFunction#translateReturnType(java.lang.Class)
	 */
	@Override
	protected Class<? extends Number> translateReturnType(Class<?> clazz)
	{
		//COUNT always returns Long
		return Long.class;
	}

	/**
	 * @see com.github.conserveorm.aggregate.AggregateFunction#getFunctionName()
	 */
	@Override
	protected String getFunctionName()
	{
		return "COUNT";
	}
}
