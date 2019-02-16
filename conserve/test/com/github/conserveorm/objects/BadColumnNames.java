/*******************************************************************************
 *  
 * Copyright (c) 2009, 2019 Erik Berglund.
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
package com.github.conserveorm.objects;

/**
 * An object with property names that map to banned column names.
 * @author Erik Berglund
 *
 */
public class BadColumnNames
{
	private long count;
	private String key;
	public long getCount()
	{
		return count;
	}
	public void setCount(long count)
	{
		this.count = count;
	}
	public String getKey()
	{
		return key;
	}
	public void setKey(String key)
	{
		this.key = key;
	}
}
