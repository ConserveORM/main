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
package org.conserve.objects.schemaupdate;

/**
 * Identical to OriginalObject, but the name of the redundantObject property is
 * changed to renamedObject.
 * 
 * @author Erik Berglund
 * 
 */
public class RenamedColumn
{
	private String name;
	private int value;
	private Object otherObject;
	private Object renamedObject;
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
	 * @return the renamedObject
	 */
	public Object getRenamedObject()
	{
		return renamedObject;
	}
	/**
	 * @param renamedObject the renamedObject to set
	 */
	public void setRenamedObject(Object renamedObject)
	{
		this.renamedObject = renamedObject;
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
