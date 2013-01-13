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
 * Orders the result according to a group of Ascending and/or Descending sorters.
 * Ordering can optionally be combined with a limit and offset.
 * 
 * The limit value determines the maximum number of entries to return.
 * 
 * The offset value determines the number of entries to skip from the start of the ordered search result.
 * 
 * @author Erik Berglund
 *
 */
public class Order extends Clause
{
	protected Integer limit = null;
	protected Integer offset = null;

	public Order(Sorter ... sorters)
	{
		super(sorters);
	}
	public Order(Integer limit,Sorter ... sorters)
	{
		this(sorters);
		this.limit = limit;
	}
	public Order(Integer limit, Integer offset, Sorter ... sorters)
	{
		this(sorters);
		this.limit = limit;
		this.offset = offset;
	}
	
	/**
	 * @return the limit
	 */
	public Integer getLimit()
	{
		return limit;
	}
	
	/**
	 * @return the offset
	 */
	public Integer getOffset()
	{
		return offset;
	}
	
	
}
