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
public class PostgreSqlAdapter extends AdapterBase
{

	public PostgreSqlAdapter(Persist persist)
	{
		super(persist);
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getIdentity()
	 */
	@Override
	public String getIdentity()
	{
		return "bigserial";
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
	 * @see org.conserve.adapter.AdapterBase#getDoubleTypeKeyword()
	 */
	@Override
	public String getDoubleTypeKeyword()
	{
		return "double precision";
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getFloatTypeKeyword()
	 */
	@Override
	public String getFloatTypeKeyword()
	{
		return "real";
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getLastInsertedIdentity(String)
	 */
	@Override
	public String getLastInsertedIdentity(String tableName)
	{
		return "select last_value from " + tableName+"_"+Defaults.ID_COL+"_seq";
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
	 * @see org.conserve.adapter.AdapterBase#isSupportsClob()
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
	 * @see org.conserve.adapter.AdapterBase#averageRequiresCast()
	 */
	@Override
	public boolean averageRequiresCast()
	{
		return false;
	}
	
	/**
	 * @see org.conserve.adapter.AdapterBase#getColumnModificationTypeKeyword()
	 */
	@Override
	public Object getColumnModificationTypeKeyword()
	{
		return "TYPE";
	}
	
}
