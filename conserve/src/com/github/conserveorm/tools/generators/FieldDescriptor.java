/*******************************************************************************
 *  
 * Copyright (c) 2009, 2017 Erik Berglund.
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
package com.github.conserveorm.tools.generators;

/**
 * The short name is only valid within a given query construction context and should not be reused.
 * 
 * 
 * @author Erik Berglund
 *
 */
public class FieldDescriptor
{
	private String fieldName;
	private String shortName;
	private String tableName;

	public FieldDescriptor(String tableName, String shortName, String fieldName)
	{
		this.tableName=tableName;
		this.shortName = shortName;
		this.fieldName=fieldName;
	}
	
	public String toShortString()
	{
		return getShortName()+"."+getFieldName();
	}
	public String toFullString()
	{
		return getTableName()+"."+getFieldName();
	}

	/**
	 * @return the fieldName
	 */
	public String getFieldName()
	{
		return fieldName;
	}

	/**
	 * @return the shortName
	 */
	public String getShortName()
	{
		return shortName;
	}

	/**
	 * @return the tableName
	 */
	public String getTableName()
	{
		return tableName;
	}
}
