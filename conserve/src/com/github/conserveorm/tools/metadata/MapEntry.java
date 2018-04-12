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
package com.github.conserveorm.tools.metadata;

/**
 * This object is needed to work around Java bug <a href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4071957">4071957</a>
 * 
 * This object represents one entry in a Map, with a key and a corresponding value.
 * 
 * @author Erik Berglund
 *
 */
public class MapEntry
{
	/**
	 * The map key.
	 */
	private Object key;
	/**
	 * The value corresponding to the key.
	 */
	private Object value;
	
	/**
	 * @return the key
	 */
	public Object getKey()
	{
		return key;
	}
	/**
	 * @param key the key to set
	 */
	public void setKey(Object key)
	{
		this.key = key;
	}
	/**
	 * @return the value
	 */
	public Object getValue()
	{
		return value;
	}
	/**
	 * @param value the value to set
	 */
	public void setValue(Object value)
	{
		this.value = value;
	}
}
