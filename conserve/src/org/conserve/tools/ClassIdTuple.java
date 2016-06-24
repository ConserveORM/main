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
package org.conserve.tools;

import org.conserve.adapter.AdapterBase;

/**
 * A tuple containing a table name and the corresponding id.
 * 
 * @author Erik Berglund
 *
 */
public class ClassIdTuple
{
	private Class<?>representedClass;
	private Long id;
	
	
	public ClassIdTuple(Class<?> c,Long id)
	{
		this.representedClass = c;
		this.id = id;
	}
	
	public Long getId()
	{
		return id;
	}

	public Class<?> getRepresentedClass()
	{
		return representedClass;
	}


	public String getTableName(AdapterBase adapter)
	{
		return NameGenerator.getTableName(representedClass,adapter);
	}
}
