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

import java.util.ArrayList;
import java.util.List;

/**
 * @author Erik Berglund
 *
 */
public class InfiniteRepititionCollection
{
	private List<InfiniteRepititionCollection> subcategories;
	private String name;
	
	/**
	 * 
	 */
	public InfiniteRepititionCollection()
	{
	}
	
	public InfiniteRepititionCollection(String name)
	{
		this.name = name;
	}
	
	public void addSubCollection(InfiniteRepititionCollection sub)
	{
		if(subcategories==null)
		{
			this.subcategories = new ArrayList<>();
		}
		subcategories.add(sub);
	}

	public List<InfiniteRepititionCollection> getSubcategories()
	{
		return subcategories;
	}

	public void setSubcategories(List<InfiniteRepititionCollection> subcategories)
	{
		this.subcategories = subcategories;
	}

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}
	
	@Override
	public String toString()
	{
		return this.name +"["+subcategories+"]";
	}
	
}
