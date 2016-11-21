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
package org.conserve.select.discriminators;

/**
 * A Selector subclass that implements the SQL "LIKE" query.
 * @author Erik Berglund
 *
 */
public class Like extends Selector
{

	/**
	 * @param sel
	 * @param strictInheritance
	 */
	public Like(Object sel, boolean strictInheritance)
	{
		super(sel, strictInheritance);
	}

	/**
	 * @param sel
	 * @param clazz
	 * @param strictInheritance
	 */
	public Like(Object sel, Class<?> clazz, boolean strictInheritance)
	{
		super(sel, clazz, strictInheritance);
	}

	/**
	 * @param sel
	 */
	public Like(Object sel)
	{
		super(sel);
	}

	/**
	 * @param sel
	 * @param clazz
	 */
	public Like(Object sel, Class<?> clazz)
	{
		super(sel, clazz);
	}

	/**
	 * @see org.conserve.select.discriminators.Selector#getRelationalRepresentation()
	 */
	@Override
	public String getRelationalRepresentation()
	{
		return " LIKE ";
	}

}
