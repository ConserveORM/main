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
package com.github.conserveorm.objects.schemaupdate;

import java.io.Serializable;

/**
 * An object that contains objects that will change inheritance.
 * Identical to ContainerObject but has a Serializable instead of an OriginalObject property.
 * 
 * @author Erik Berglund
 *
 */
public class ChangedInheritanceContainer
{
	private Serializable foo;
	public ChangedInheritanceContainer()
	{	
	}
	
	/**
	 * @return the foo
	 */
	public Serializable getFoo()
	{
		return foo;
	}
	
	/**
	 * @param foo the foo to set
	 */
	public void setFoo(Serializable foo)
	{
		this.foo = foo;
	}
	
}
