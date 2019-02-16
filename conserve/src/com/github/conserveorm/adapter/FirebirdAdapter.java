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
import com.github.conserveorm.tools.metadata.ObjectRepresentation;
import com.github.conserveorm.tools.metadata.ObjectStack;

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
	 * @see com.github.conserveorm.adapter.AdapterBase#getVarCharKeyword()
	 */
	@Override
	public String getVarCharKeyword()
	{
		return getVarCharKeyword(8191);// 32765 bytes, 4 byte chars
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#getVarCharIndexed()
	 */
	@Override
	public String getVarCharIndexed()
	{
		return getVarCharKeyword(253);
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#getDoubleTypeKeyword()
	 */
	@Override
	public String getDoubleTypeKeyword()
	{
		return "DOUBLE PRECISION";
	}

	/**
	 * 
	 * Firebird requires transaction to be committed before a new table can be
	 * used.
	 * 
	 * @see com.github.conserveorm.adapter.AdapterBase#isRequiresCommitAfterSchemaAlteration()
	 */
	@Override
	public boolean isRequiresCommitAfterSchemaAlteration()
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
	 * @see com.github.conserveorm.adapter.AdapterBase#getLongTypeKeyword()
	 */
	@Override
	public String getLongTypeKeyword()
	{
		return "BIGINT";
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#isSupportsIdentity()
	 */
	@Override
	public boolean isSupportsIdentity()
	{
		return false;
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#getLastInsertedIdentity(java.lang.String)
	 */
	@Override
	public String getLastInsertedIdentity(String tableName)
	{
		return "execute block\n" + "returns (id bigint)\n" + "as\n" + "begin\n"
				+ "  id = RDB$GET_CONTEXT('USER_SESSION', 'LAST__INSERT__ID');\n" + "suspend;\n" + "end";
	}

	/**
	 * Firebird can't even rename tables.
	 * 
	 * @see com.github.conserveorm.adapter.AdapterBase#canRenameTable()
	 */
	@Override
	public boolean canRenameTable()
	{
		return false;
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#getTableRenameStatements(java.lang.String, java.lang.Class,
	 *      java.lang.String, java.lang.Class)
	 */
	@Override
	public String[] getTableRenameStatements(String oldTableName, Class<?> oldClass, String newTableName,
			Class<?> newClass)
	{
		// get a comma-separated list of implemented fields
		ObjectStack oldStack = new ObjectStack(this, oldClass);
		ObjectRepresentation oldRep = oldStack.getActualRepresentation();
		StringBuilder paramList = new StringBuilder(Defaults.ID_COL);
		paramList.append(",").append(Defaults.REAL_CLASS_COL);
		for (int x = 0; x < oldRep.getPropertyCount(); x++)
		{
			String prop = oldRep.getPropertyName(x);
			paramList.append(",");
			paramList.append(prop);
		}
		String params = paramList.toString();

		String[] res = new String[3];
		res[0] = "INSERT INTO " + newTableName + "(" + params + ") SELECT " + params + " FROM " + oldTableName;
		res[1] = "DROP TABLE " + oldTableName;
		// remove old entries in TYPE_TABLENAME
		res[2] = "DELETE FROM " + Defaults.TYPE_TABLENAME + " WHERE OWNER_TABLE = '" + oldTableName + "'";

		return res;
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#getMaximumNameLength()
	 */
	@Override
	public int getMaximumNameLength()
	{
		return 31;
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
	 * @see com.github.conserveorm.adapter.AdapterBase#isSupportsExistsKeyword()
	 */
	@Override
	public boolean isSupportsExistsKeyword()
	{
		return false;
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#getLimitString()
	 */
	@Override
	public String getLimitString()
	{
		return "FIRST ?";
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#getOffsetString()
	 */
	@Override
	public String getOffsetString()
	{
		return "SKIP ?";
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#isPutLimitOffsetBeforeColumns()
	 */
	@Override
	public boolean isPutLimitOffsetBeforeColumns()
	{
		return true;
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#getBooleanTypeKeyword()
	 */
	@Override
	public String getBooleanTypeKeyword()
	{
		return "CHAR(1)";
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#getRenameColumnStatement()
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
	 * @see com.github.conserveorm.adapter.AdapterBase#getByteTypeKeyword()
	 */
	@Override
	public String getByteTypeKeyword()
	{
		return "SMALLINT";
	}

	@Override
	public String getSequenceExistsStatement(String sequenceName)
	{
		return "SELECT COUNT(*)  FROM RDB$GENERATORS WHERE RDB$GENERATOR_NAME='" + sequenceName + "'";
	}

	/**
	 * Firebird won't automatically drop indices when a table is dropped.
	 */
	@Override
	public boolean indicesMustBeManuallyDropped()
	{
		return true;
	}

	@Override
	public Object getColumnModificationKeyword()
	{
		return "ALTER";
	}

	@Override
	public Object getColumnModificationTypeKeyword()
	{
		return "TYPE";
	}
	
	
	@Override
	public Integer getMaxMatchingValues() {
		return 1500;
	}

}
