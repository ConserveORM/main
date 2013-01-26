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
 * Adapter for MySQL databases. Specifies behaviour and dialects specific to the
 * MySQL database engine.
 * 
 * @author Erik Berglund
 * 
 */
public class MySqlAdapter extends AdapterBase
{

	public MySqlAdapter(Persist persist)
	{
		super(persist);
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getVarCharKeyword()
	 */
	@Override
	public String getVarCharKeyword()
	{
		return "LONGTEXT";
	}

	@Override
	public String getVarCharKeyword(int length)
	{
		return getVarCharKeyword();
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getKeyLength()
	 */
	@Override
	public String getKeyLength()
	{
		// maximum allowed key length.
		return "(900)";
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
	 * @see org.conserve.adapter.AdapterBase#getLastInsertedIdentity(String)
	 */
	@Override
	public String getLastInsertedIdentity(String tableName)
	{
		return "SELECT LAST_INSERT_ID()";
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
	 * @see org.conserve.adapter.AdapterBase#isSupportsClob()
	 */
	@Override
	public boolean isSupportsClob()
	{
		return false;
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getRenameColumnStatement()
	 */
	@Override
	public String getRenameColumnStatement()
	{
		StringBuilder statement = new StringBuilder("ALTER TABLE ");
		statement.append(Defaults.TABLENAME_PLACEHOLDER);
		statement.append(" CHANGE ");
		statement.append(Defaults.OLD_COLUMN_NAME_PLACEHOLDER);
		statement.append(" ");
		statement.append(Defaults.NEW_COLUMN_NAME_PLACEHOLDER);
		statement.append(" ");
		statement.append(Defaults.NEW_COLUMN_DESCRIPTION_PLACEHOLDER);
		return statement.toString();
	}
}
