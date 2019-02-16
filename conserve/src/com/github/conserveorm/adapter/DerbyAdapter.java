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
package com.github.conserveorm.adapter;

import com.github.conserveorm.Persist;
import com.github.conserveorm.tools.Defaults;

/**
 * @author Erik Berglund
 * 
 */
public class DerbyAdapter extends AdapterBase
{

	public DerbyAdapter(Persist persist)
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
		return "SELECT IDENTITY_VAL_LOCAL() FROM " + tableName;
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#getIdentity()
	 */
	@Override
	public String getIdentity()
	{
		return "BIGINT GENERATED BY DEFAULT AS IDENTITY";
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
	 * @see com.github.conserveorm.adapter.AdapterBase#getLimitString()
	 */
	@Override
	public String getLimitString()
	{
		return "FETCH NEXT ? ROW ONLY";
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#getOffsetString()
	 */
	@Override
	public String getOffsetString()
	{
		return "OFFSET ? ROW";
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#isPutLimitBeforeOffset()
	 */
	@Override
	public boolean isPutLimitBeforeOffset()
	{
		// Derby requires OFFSET to come before FETCH
		return false;
	}

	@Override
	public boolean handlesDistinctWithClobsAndBlobsCorrectly()
	{
		// derby does not handle DISTINCT on queries that return CLOB or BLOB,
		// even if the the CLOB or BLOB columns are not part of the DISTINCT()
		// part.
		return false;
	}

	/**
	 * Derby does not support the EXISTS keyword.
	 * 
	 * @see com.github.conserveorm.adapter.AdapterBase#isSupportsExistsKeyword()
	 */
	@Override
	public boolean isSupportsExistsKeyword()
	{
		return false;
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#getBooleanTypeKeyword()
	 */
	@Override
	public String getBooleanTypeKeyword()
	{
		return "SMALLINT";
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#getByteTypeKeyword()
	 */
	@Override
	public String getByteTypeKeyword()
	{
		return "SMALLINT";
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#getRenameColumnStatement()
	 */
	@Override
	public String getRenameColumnStatement()
	{
		StringBuilder statement = new StringBuilder("RENAME COLUMN ");
		statement.append(Defaults.TABLENAME_PLACEHOLDER);
		statement.append(".");
		statement.append(Defaults.OLD_COLUMN_NAME_PLACEHOLDER);
		statement.append(" TO ");
		statement.append(Defaults.NEW_COLUMN_NAME_PLACEHOLDER);
		return statement.toString();
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#getTableRenameStatements(java.lang.String,java.lang.Class,
	 *      java.lang.String,java.lang.Class)
	 */
	@Override
	public String[] getTableRenameStatements(String oldTableName, Class<?> oldClass, String newTableName, Class<?> newClass)
	{
		StringBuilder sb = new StringBuilder("RENAME TABLE ");
		sb.append(oldTableName);
		sb.append(" TO ");
		sb.append(newTableName);
		return new String[] { sb.toString() };

	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#canChangeColumnType()
	 */
	@Override
	public boolean canChangeColumnType()
	{
		// Derby only allows string widening, which is not enough.
		return false;
	}

}
