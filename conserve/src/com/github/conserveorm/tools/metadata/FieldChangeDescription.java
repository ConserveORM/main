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
package com.github.conserveorm.tools.metadata;

/**
 * 
 * Describes a change in a property.
 * 
 * @author Erik Berglund
 *
 */
public class FieldChangeDescription
{
	//old name of a property
	private String fromName;
	//new name of a property
	private String toName;
	//old type of the property
	private Class<?> fromClass;
	//new type of the property
	private Class<?> toClass;
	//name of the table a property is moved from
	private String fromTable;
	//name of the table a property is moved to
	private String toTable;
	//true if at least one index has changed
	private boolean isIndexChange = false;
	//the old size of a resized varchar field
	private Long fromSize;
	//the new size of a resized varchar field
	private Long toSize;
	
	
	/**
	 * @return the fromTable
	 */
	public String getFromTable()
	{
		return fromTable;
	}
	/**
	 * @param fromTable the fromTable to set
	 */
	public void setFromTable(String fromTable)
	{
		this.fromTable = fromTable;
	}
	/**
	 * @return the toTable
	 */
	public String getToTable()
	{
		return toTable;
	}
	/**
	 * @param toTable the toTable to set
	 */
	public void setToTable(String toTable)
	{
		this.toTable = toTable;
	}
	
	/**
	 * @return the fromName
	 */
	public String getFromName()
	{
		return fromName;
	}
	/**
	 * @param fromName the fromName to set
	 */
	public void setFromName(String fromName)
	{
		this.fromName = fromName;
	}
	/**
	 * @return the toName
	 */
	public String getToName()
	{
		return toName;
	}
	/**
	 * @param toName the toName to set
	 */
	public void setToName(String toName)
	{
		this.toName = toName;
	}
	/**
	 * @return the fromClass
	 */
	public Class<?> getFromClass()
	{
		return fromClass;
	}
	/**
	 * @param fromClass the fromClass to set
	 */
	public void setFromClass(Class<?> fromClass)
	{
		this.fromClass = fromClass;
	}
	/**
	 * @return the toClass
	 */
	public Class<?> getToClass()
	{
		return toClass;
	}
	/**
	 * @param toClass the toClass to set
	 */
	public void setToClass(Class<?> toClass)
	{
		this.toClass = toClass;
	}
	
	public boolean isDeletion()
	{
		return !isIndexChange & toName == null;
	}
	
	public boolean isCreation()
	{
		return !isIndexChange & fromName == null;
	}
	
	public boolean isNameChange()
	{
		return !isDeletion() && !isCreation() && toName != null && !toName.equals(fromName);
	}
	
	/**
	 * Returns true if this change is a field moved from one table to another.
	 */
	public boolean isMove()
	{
		return (fromTable!=null && toTable!=null&& toName != null && fromName!= null && toName.equals(fromName));
	}
	
	public boolean isTypeChange()
	{
		return !isDeletion() && !isCreation() && !isNameChange() && !isMove() && toClass!=null && !toClass.equals(fromClass);
	}
	/**
	 * 
	 */
	public void setIsIndexChange()
	{
		isIndexChange = true;
	}
	
	public boolean isIndexChange()
	{
		return this.isIndexChange;
	}
	
	public Long getToSize()
	{
		return toSize;
	}
	
	public void setToSize(Long toSize)
	{
		this.toSize = toSize;
	}
	
	public Long getFromSize()
	{
		return fromSize;
	}
	
	public void setFromSize(Long fromSize)
	{
		this.fromSize = fromSize;
	}
	
	public boolean isSizeChange()
	{
		return ((toSize != null )||(fromSize!=null))&&(toSize!=fromSize);
	}
}
