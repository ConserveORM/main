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
package com.github.conserveorm.objects.polymorphism;

/**
 * @author Erik Berglund
 *
 */
public class ConcreteBar1 extends AbstractBar
{

	private String abstractName;
	private String fooProperty;

	/**
	 * @see com.github.conserveorm.objects.polymorphism.AbstractBar#getAbstractName()
	 */
	@Override
	public String getAbstractName()
	{
		return abstractName;
	}

	/**
	 * @see com.github.conserveorm.objects.polymorphism.AbstractBar#setAbstractName(java.lang.String)
	 */
	@Override
	public void setAbstractName(String abstractName)
	{
		this.abstractName = abstractName;
	}

	/**
	 * @param fooProperty the fooProperty to set
	 */
	public void setFooProperty(String fooProperty)
	{
		this.fooProperty = fooProperty;
	}

	/**
	 * @return the fooProperty
	 */
	public String getFooProperty()
	{
		return fooProperty;
	}

}
