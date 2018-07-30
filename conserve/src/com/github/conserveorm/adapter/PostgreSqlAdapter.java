/*******************************************************************************
 *  
 * Copyright (c) 2009, 2018 Erik Berglund.
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
import com.github.conserveorm.tools.Defaults;

/**
 * @author Erik Berglund
 *
 */
public class PostgreSqlAdapter extends AdapterBase
{

	public PostgreSqlAdapter(Persist persist)
	{
		super(persist);
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#getIdentity()
	 */
	@Override
	public String getIdentity()
	{
		return "bigserial";
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
	 * @see com.github.conserveorm.adapter.AdapterBase#getDoubleTypeKeyword()
	 */
	@Override
	public String getDoubleTypeKeyword()
	{
		return "double precision";
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#getFloatTypeKeyword()
	 */
	@Override
	public String getFloatTypeKeyword()
	{
		return "real";
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#getLastInsertedIdentity(String)
	 */
	@Override
	public String getLastInsertedIdentity(String tableName)
	{
		return "select currval('"+tableName+"_"+Defaults.ID_COL+"_seq')";
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
	 * @see com.github.conserveorm.adapter.AdapterBase#isSupportsClob()
	 */
	@Override
	public boolean isSupportsClob()
	{
		return false;
	}
	

	public String getByteTypeKeyword()
	{
		return "smallint";
	}

	public String getBlobTypeKeyword()
	{
		return "BYTEA";
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#averageRequiresCast()
	 */
	@Override
	public boolean averageRequiresCast()
	{
		return false;
	}
	
	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#getColumnModificationTypeKeyword()
	 */
	@Override
	public Object getColumnModificationTypeKeyword()
	{
		return "TYPE";
	}
	

	/**
	 * Create an SQL statement that will rename a column. The table name, old
	 * column name, and new column name are placeholders.
	 * 
	 * @return the SQL statement used to rename a column.
	 */
	public String getRenameColumnStatement()
	{
		StringBuilder statement = new StringBuilder("ALTER TABLE ");
		statement.append(Defaults.TABLENAME_PLACEHOLDER);
		statement.append(" RENAME ");
		statement.append(Defaults.OLD_COLUMN_NAME_PLACEHOLDER);
		statement.append(" TO ");
		statement.append(Defaults.NEW_COLUMN_NAME_PLACEHOLDER);
		return statement.toString();
	}
	
	@Override
	public int getMaximumNameLength()
	{
		//actual maximum is 63, but we must allow for the name of generated sequences
		return 58-Defaults.ID_COL.length();
	}
}
