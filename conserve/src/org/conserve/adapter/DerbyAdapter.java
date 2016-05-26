/*******************************************************************************
 * Copyright (c) 2009, 2013 Erik Berglund.
 *   
 *      This file is part of Conserve.
 *   
 *       Conserve is free software: you can redistribute it and/or modify
 *       it under the terms of the GNU Lesser General Public License as published by
 *       the Free Software Foundation, either version 3 of the License, or
 *       (at your option) any later version.
 *   
 *       Conserve is distributed in the hope that it will be useful,
 *       but WITHOUT ANY WARRANTY; without even the implied warranty of
 *       MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *       GNU Lesser General Public License for more details.
 *   
 *       You should have received a copy of the GNU Lesser General Public License
 *       along with Conserve.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.conserve.adapter;

import org.conserve.Persist;
import org.conserve.tools.Defaults;

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
		return "SELECT IDENTITY_VAL_LOCAL() FROM " + tableName;
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getIdentity()
	 */
	@Override
	public String getIdentity()
	{
		return "BIGINT GENERATED ALWAYS AS IDENTITY";
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
	 * @see org.conserve.adapter.AdapterBase#getLimitString()
	 */
	@Override
	public String getLimitString()
	{
		return "FETCH NEXT ? ROW ONLY";
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getOffsetString()
	 */
	@Override
	public String getOffsetString()
	{
		return "OFFSET ? ROW";
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#isPutLimitBeforeOffset()
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
	 * @see org.conserve.adapter.AdapterBase#isSupportsExistsKeyword()
	 */
	@Override
	public boolean isSupportsExistsKeyword()
	{
		return false;
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getBooleanTypeKeyword()
	 */
	@Override
	public String getBooleanTypeKeyword()
	{
		return "SMALLINT";
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getByteTypeKeyword()
	 */
	@Override
	public String getByteTypeKeyword()
	{
		return "SMALLINT";
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getRenameColumnStatement()
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
	 * @see org.conserve.adapter.AdapterBase#getTableRenameStatements(java.lang.String, java.lang.String)
	 */
	@Override
	public String [] getTableRenameStatements(String oldTableName, String newTableName)
	{
		StringBuilder sb = new StringBuilder("RENAME TABLE ");
		sb.append(oldTableName);
		sb.append(" TO ");
		sb.append(newTableName);
		return new String[]{sb.toString()};
		
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#canChangeColumnType()
	 */
	@Override
	public boolean canChangeColumnType()
	{
		//Derby only allows string widening, which is not enough.
		return false;
	}

}
