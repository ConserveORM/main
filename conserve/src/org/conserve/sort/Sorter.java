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
package org.conserve.sort;


/**
 * Base class for ascending or descending sorters.
 * 
 * @author Erik Berglund
 * 
 */
public abstract class Sorter extends Order
{
	private Object sortObject;
	private Class<?>sortClass;

	public Sorter(Object sortBy)
	{
		this.sortObject = sortBy;
	}
	public Sorter(Object sortBy, Class<?>sortClass)
	{
		this(sortBy);
		setSortClass(sortClass);
	}

	public Object getSortObject()
	{
		return sortObject;
	}
	
	public Class<?> getSortClass()
	{
		if(sortClass!=null)
		{
			return sortClass;
		}
		else
		{
			return getSortObject().getClass();
		}
	}
	
	public void setSortClass(Class<?>clazz)
	{
		this.sortClass = clazz;
	}
	
	public abstract String getKeyWord();
}
