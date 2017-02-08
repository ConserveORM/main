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
package com.github.conserveorm.adapter;

import com.github.conserveorm.Persist;

/**
 * @author Erik Berglund
 *
 */
public class MonetDbAdapter extends AdapterBase
{

	/**
	 * @param persist
	 */
	public MonetDbAdapter(Persist persist)
	{
		super(persist);
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#getVarCharKeyword()
	 */
	@Override
	public String getVarCharKeyword()
	{
		return getVarCharKeyword(32672);
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#getLastInsertedIdentity(java.lang.String)
	 */
	@Override
	public String getLastInsertedIdentity(String tableName)
	{
		return "SELECT MAX(C__ID) FROM " + tableName;
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#getIdentity()
	 */
	@Override
	public String getIdentity()
	{
		return "BIGINT AUTO_INCREMENT";
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#getAllowsEmptyStatements()
	 */
	@Override
	public boolean getAllowsEmptyStatements()
	{
		return false;
	}
	

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#getTableNamesAreLowerCase()
	 */
	@Override
	public boolean getTableNamesAreLowerCase()
	{
		return true;
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#isSupportsClob()
	 */
	@Override
	public boolean isSupportsClob()
	{
		return false;
	}


	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#isSupportsExistsKeyword()
	 */
	@Override
	public boolean isSupportsExistsKeyword()
	{
		return false;
	}
	
	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#getTableRenameStatements(java.lang.String, java.lang.Class, java.lang.String,java.lang.Class)
	 */
	@Override
	public String[] getTableRenameStatements(String oldTableName,Class<?>oldClass, String newTableName,Class<?>newClass)
	{
		String [] res = new String[2];
		res[0] = "CREATE TABLE "+newTableName+" AS SELECT * FROM " +oldTableName+" WITH DATA";
		res[1] = "DROP TABLE " + oldTableName;
		return res;
	}
	
	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#indicesMustBeRecreatedAfterRename()
	 */
	@Override
	public boolean indicesMustBeRecreatedAfterRename()
	{
		//MonetDB renames a table by copying the data into a new table and dropping the old one.
		//therefore indices must be recreated.
		return true;
	}
	
	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#canChangeColumnType()
	 */
	@Override
	public boolean canChangeColumnType()
	{
		return false;
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#canRenameColumn()
	 */
	@Override
	public boolean canRenameColumn()
	{
		return false;
	}
}
