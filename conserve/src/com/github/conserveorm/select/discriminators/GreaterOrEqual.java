/*******************************************************************************
 *  
 * Copyright (c) 2009, 2017 Erik Berglund.
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
package com.github.conserveorm.select.discriminators;

/**
 * Selector that matches any property that *is not* null in the search object to database entries where the same property is greater
 * than or equal to the property in the search object.
 * 
 * @author Erik Berglund
 *
 */
public class GreaterOrEqual extends Selector
{

	/**
	 * @param sel
	 */
	public GreaterOrEqual(Object sel)
	{
		super(sel);
	}
	public GreaterOrEqual(Object sel, Class<?> clazz)
	{
		super(sel,clazz);
	}
	public GreaterOrEqual(Object sel,boolean strictInheritance)
	{
		super(sel,strictInheritance);
	}
	
	public GreaterOrEqual(Object sel, Class<?> clazz,boolean strictInheritance)
	{
		super(sel,clazz,strictInheritance);
	}

	@Override
	public String getRelationalRepresentation()
	{
		return " >= ";
	}
}
