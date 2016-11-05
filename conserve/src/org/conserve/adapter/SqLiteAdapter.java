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
 * Adapter that defines behaviour and properties specific to SQLite databases.
 * 
 * @author Erik Berglund
 *
 */
public class SqLiteAdapter extends AdapterBase
{

	/**
	 * @param persist
	 */
	public SqLiteAdapter(Persist persist)
	{
		super(persist);
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getLastInsertedIdentity(java.lang.String)
	 */
	@Override
	public String getLastInsertedIdentity(String tableName)
	{
		return "select last_insert_rowid()";
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getIdentity()
	 */
	@Override
	public String getIdentity()
	{
		//the actual keyword is "integer primary key", but the "primary key" part is added by caller
		return "INTEGER";
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
	 * @see org.conserve.adapter.AdapterBase#getAllowsEmptyStatements()
	 */
	@Override
	public boolean getAllowsEmptyStatements()
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

	/**
	 * @see org.conserve.adapter.AdapterBase#canDropColumn()
	 */
	@Override
	public boolean canDropColumn()
	{
		return false;
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
	 * @see org.conserve.adapter.AdapterBase#isSupportsJoinInUpdate()
	 */
	@Override
	public boolean isSupportsJoinInUpdate()
	{
		return false;
	}
	
	/**
	 * @see org.conserve.adapter.AdapterBase#getMaximumIdNumber()
	 */
	@Override
	public long getMaximumIdNumber()
	{
		return Integer.MAX_VALUE;
	}

}
