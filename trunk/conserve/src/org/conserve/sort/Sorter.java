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
package org.conserve.sort;

import org.conserve.select.Clause;

/**
 * Base class for ascending or descending sorters.
 * 
 * @author Erik Berglund
 * 
 */
public abstract class Sorter extends Order
{
	private Object sortObject;

	public Sorter(Object sortBy)
	{
		this.sortObject = sortBy;
	}

	
	

	/**
	 * @see org.conserve.select.Clause#getSubclauses()
	 */
	@Override
	public Clause[] getSubclauses()
	{
		return new Clause[]{this};
	}

	public Object getSortObject()
	{
		return sortObject;
	}
	
	public abstract String getKeyWord();
}
