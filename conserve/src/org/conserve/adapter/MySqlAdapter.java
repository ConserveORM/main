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
		return "(255)";
	}

	/**
	 * MySQL and MariaDB don't have a way to check if an index exists before dropping it, and dropping a non-existing index causes an error.
	 * Therefore we have this rather convoluted way of checking for the index before dropping it.
	 * 
	 * @see org.conserve.adapter.AdapterBase#getDropIndexStatements(java.lang.String,
	 *      java.lang.String)
	 */
	@Override
	public String [] getDropIndexStatements(String table, String indexName)
	{
		String [] res = new String[4];
		
		StringBuilder sb = new StringBuilder();
		sb.append(
				"set @exist := (select count(*) from information_schema.statistics where table_name = '");
		sb.append(table);
		sb.append("' and index_name = '");
		sb.append(indexName);
		sb.append("' and table_schema = database())");
		res[0]=sb.toString();
		
		sb = new StringBuilder();
		sb.append("set @sqlstmt := if( @exist > 0, 'DROP INDEX ");
		sb.append(indexName);
		sb.append(" ON ");
		sb.append(table);
		sb.append("', 'select 0')");
		res[1]=sb.toString();
		
		res[2]="PREPARE stmt FROM @sqlstmt";
		res[3]= "EXECUTE stmt";

		return res;
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

	/**
	 * @see org.conserve.adapter.AdapterBase#averageRequiresCast()
	 */
	@Override
	public boolean averageRequiresCast()
	{
		return false;
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getColumnModificationKeyword()
	 */
	@Override
	public Object getColumnModificationKeyword()
	{
		return "MODIFY";
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getMaximumNameLength()
	 */
	@Override
	public int getMaximumNameLength()
	{
		return 64;
	}
}
