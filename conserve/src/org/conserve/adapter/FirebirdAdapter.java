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
 * @author Erik Berglund
 * 
 */
public class FirebirdAdapter extends AdapterBase
{

	public FirebirdAdapter(Persist persist)
	{
		super(persist);
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getVarCharKeyword()
	 */
	@Override
	public String getVarCharKeyword()
	{
		return getVarCharKeyword(8000);
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getVarCharIndexed()
	 */
	@Override
	public String getVarCharIndexed()
	{
		return  getVarCharKeyword(256);
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getDoubleTypeKeyword()
	 */
	@Override
	public String getDoubleTypeKeyword()
	{
		return "DOUBLE PRECISION";
	}
	
	

	/**
	 * 
	 * Firebird requires transaction to be committed before a new table can be used.
	 * 
	 * @see org.conserve.adapter.AdapterBase#isRequiresCommitAfterTableCreation()
	 */
	@Override
	public boolean isRequiresCommitAfterTableCreation()
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
	 * @see org.conserve.adapter.AdapterBase#getLongTypeKeyword()
	 */
	@Override
	public String getLongTypeKeyword()
	{
		return "BIGINT";
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#isSupportsIdentity()
	 */
	@Override
	public boolean isSupportsIdentity()
	{
		return false;
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getLastInsertedIdentity(java.lang.String)
	 */
	@Override
	public String getLastInsertedIdentity(String tableName)
	{
		return "execute block\n" +
				"returns (id bigint)\n" +
				"as\n" +
				"begin\n" +
				"  id = RDB$GET_CONTEXT('USER_SESSION', 'LAST__INSERT__ID');\n" +
				"suspend;\n" +
				"end";
	}
	
	
	/**
	 * Firebird can't even rename tables.
	 * 
	 * @see org.conserve.adapter.AdapterBase#canRenameTable()
	 */
	@Override
	public boolean canRenameTable()
	{
		return false;
	}

	@Override
	public boolean indicesMustBeRecreatedAfterRename()
	{
		return true;
	}
	
	/**
	 * @see org.conserve.adapter.AdapterBase#getTableRenameStatements(java.lang.String, java.lang.String)
	 */
	@Override
	public String[] getTableRenameStatements(String oldTableName, String newTableName)
	{
		String [] res = new String[2];
		res[0]= "INSERT INTO "+newTableName+" SELECT * FROM " +oldTableName + " ORDER BY " + Defaults.ID_COL;
		res[1]="DROP TABLE " + oldTableName;
		return res;
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getMaximumNameLength()
	 */
	@Override
	public int getMaximumNameLength()
	{
		return 31;
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#isRowSizeLimited()
	 */
	@Override
	public boolean isRowSizeLimited()
	{
		return true;
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getMaximumRowSize()
	 */
	@Override
	public int getMaximumRowSize()
	{
		return 8192;
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
	 * @see org.conserve.adapter.AdapterBase#isSupportsExistsKeyword()
	 */
	@Override
	public boolean isSupportsExistsKeyword()
	{
		return false;
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getLimitString()
	 */
	@Override
	public String getLimitString()
	{
		return "FIRST ?";
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getOffsetString()
	 */
	@Override
	public String getOffsetString()
	{
		return "SKIP ?";
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#isPutLimitOffsetBeforeColumns()
	 */
	@Override
	public boolean isPutLimitOffsetBeforeColumns()
	{
		return true;
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getBooleanTypeKeyword()
	 */
	@Override
	public String getBooleanTypeKeyword()
	{
		return "CHAR(1)";
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getRenameColumnStatement()
	 */
	@Override
	public String getRenameColumnStatement()
	{
		StringBuilder statement = new StringBuilder("ALTER TABLE ");
		statement.append(Defaults.TABLENAME_PLACEHOLDER);
		statement.append(" ALTER COLUMN ");
		statement.append(Defaults.OLD_COLUMN_NAME_PLACEHOLDER);
		statement.append(" TO ");
		statement.append(Defaults.NEW_COLUMN_NAME_PLACEHOLDER);
		return statement.toString();
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getByteTypeKeyword()
	 */
	@Override
	public String getByteTypeKeyword()
	{
		return "SMALLINT";
	}	

	
}
