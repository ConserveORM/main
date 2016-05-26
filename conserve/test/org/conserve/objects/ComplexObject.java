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
package org.conserve.objects;

/**
 * @author Erik Berglund
 *
 */
public class ComplexObject
{
	private double [] data;
	private Object object;
	private SimplestObject simplestObject;
	
	/**
	 * Default constructor, required by interface.
	 */
	public ComplexObject()
	{
	}

	public ComplexObject(double[] ds)
	{
		setData(ds);
	}

	public void setData(double [] data)
	{
		this.data = data;
	}

	public double [] getData()
	{
		return data;
	}

	public void setObject(Object object)
	{
		this.object = object;
	}

	public Object getObject()
	{
		return object;
	}

	public void setSimplestObject(SimplestObject simplestObject)
	{
		this.simplestObject = simplestObject;
	}

	public SimplestObject getSimplestObject()
	{
		return simplestObject;
	}

}
