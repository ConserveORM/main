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
	protected Long limit = null;
	protected Long offset = null;

	public Order(Sorter ... sorters)
	{
		super(sorters);
	}
	public Order(long limit,Sorter ... sorters)
	{
		this(sorters);
		this.limit = limit;
	}
	public Order(long limit, long offset, Sorter ... sorters)
	{
		this(sorters);
		this.limit = limit;
		this.offset = offset;
	}
	
	/**
	 * @return the limit
	 */
	public Long getLimit()
	{
		return limit;
	}
	
	/**
	 * @return the offset
	 */
	public Long getOffset()
	{
		return offset;
	}
	/**
	 * @see org.conserve.select.Clause#getQueryClass()
	 */
	@Override
	public Class<?> getQueryClass()
	{
		return null;
	}	
}
