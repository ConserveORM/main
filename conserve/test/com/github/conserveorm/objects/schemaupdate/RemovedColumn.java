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
package com.github.conserveorm.objects.schemaupdate;

/**
 * This class is identical to OriginalObject but lacks the redundantObject property.
 * 
 * @author Erik Berglund
 *
 */
public class RemovedColumn
{
	private String name;
	private int value;
	private Object otherObject;
	private long[] array;

	/**
	 * @return the name
	 */
	public String getName()
	{
		return name;
	}
	/**
	 * @param name the name to set
	 */
	public void setName(String name)
	{
		this.name = name;
	}
	/**
	 * @return the value
	 */
	public int getValue()
	{
		return value;
	}
	/**
	 * @param value the value to set
	 */
	public void setValue(int value)
	{
		this.value = value;
	}
	/**
	 * @return the otherObject
	 */
	public Object getOtherObject()
	{
		return otherObject;
	}
	/**
	 * @param otherObject the otherObject to set
	 */
	public void setOtherObject(Object otherObject)
	{
		this.otherObject = otherObject;
	}
	/**
	 * @return the array
	 */
	public long[] getArray()
	{
		return array;
	}
	/**
	 * @param array the array to set
	 */
	public void setArray(long[] array)
	{
		this.array = array;
	}
}
