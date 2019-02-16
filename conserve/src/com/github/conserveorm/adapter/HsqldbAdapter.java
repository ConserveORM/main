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

/**
 * Specifics for HSQLDB databases.
 * 
 * @author Erik Berglund
 *
 */
public class HsqldbAdapter extends AdapterBase
{

	public HsqldbAdapter(Persist persist)
	{
		super(persist);
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#getVarCharKeyword()
	 */
	@Override
	public String getVarCharKeyword()
	{
		return "LONGVARCHAR";
	}

	@Override
	public String getVarCharKeyword(long length)
	{
		return getVarCharKeyword();
	}
	

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#getLastInsertedIdentity(java.lang.String)
	 */
	@Override
	public String getLastInsertedIdentity(String tableName)
	{
		return "call identity()";
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
	 * @see com.github.conserveorm.adapter.AdapterBase#getBlobTypeKeyword()
	 */
	@Override
	public String getBlobTypeKeyword()
	{
		return "LONGVARBINARY";
	}

	/**
	 * @see com.github.conserveorm.adapter.AdapterBase#isSupportsClob()
	 */
	@Override
	public boolean isSupportsClob()
	{
		return false;
	}
	
		
}
