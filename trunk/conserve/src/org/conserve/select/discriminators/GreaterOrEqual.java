/*******************************************************************************
 * Copyright (c) 2009, 2012 Erik Berglund.
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
package org.conserve.select.discriminators;

/**
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
