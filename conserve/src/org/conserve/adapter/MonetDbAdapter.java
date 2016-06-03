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
package org.conserve.adapter;

import org.conserve.Persist;

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
	 * @see org.conserve.adapter.AdapterBase#getVarCharKeyword()
	 */
	@Override
	public String getVarCharKeyword()
	{
		return getVarCharKeyword(32672);
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getLastInsertedIdentity(java.lang.String)
	 */
	@Override
	public String getLastInsertedIdentity(String tableName)
	{
		return "SELECT MAX(C__ID) FROM " + tableName;
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getIdentity()
	 */
	@Override
	public String getIdentity()
	{
		return "BIGINT AUTO_INCREMENT";
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#isValidColumnName(java.lang.String)
	 */
	@Override
	public boolean isValidColumnName(String name)
	{
		if (name.equalsIgnoreCase("KEY"))
		{
			return false;
		}
		else
		{
			return super.isValidColumnName(name);
		}
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#allowsEmptyStatements()
	 */
	@Override
	public boolean allowsEmptyStatements()
	{
		return false;
	}
	

	/**
	 * @see org.conserve.adapter.AdapterBase#tableNamesAreLowerCase()
	 */
	@Override
	public boolean tableNamesAreLowerCase()
	{
		return true;
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#isSupportsClob()
	 */
	@Override
	public boolean isSupportsClob()
	{
		return false;
	}


	/**
	 * @see org.conserve.adapter.AdapterBase#isSupportsExistsKeyword()
	 */
	@Override
	public boolean isSupportsExistsKeyword()
	{
		return false;
	}
	
	/**
	 * @see org.conserve.adapter.AdapterBase#getTableRenameStatements(java.lang.String, java.lang.String,java.lang.Class)
	 */
	@Override
	public String[] getTableRenameStatements(String oldTableName, String newTableName,Class<?>oldClass)
	{
		String [] res = new String[2];
		res[0] = "CREATE TABLE "+newTableName+" AS SELECT * FROM " +oldTableName+" WITH DATA";
		res[1] = "DROP TABLE " + oldTableName;
		return res;
	}
	
	/**
	 * @see org.conserve.adapter.AdapterBase#indicesMustBeRecreatedAfterRename()
	 */
	@Override
	public boolean indicesMustBeRecreatedAfterRename()
	{
		//MonetDB renames a table by copying the data into a new table and dropping the old one.
		//therefore indices must be recreated.
		return true;
	}
	
	/**
	 * @see org.conserve.adapter.AdapterBase#canChangeColumnType()
	 */
	@Override
	public boolean canChangeColumnType()
	{
		return false;
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#canRenameColumn()
	 */
	@Override
	public boolean canRenameColumn()
	{
		return false;
	}
}
