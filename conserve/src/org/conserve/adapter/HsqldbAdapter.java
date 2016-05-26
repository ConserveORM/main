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
	 * @see org.conserve.adapter.AdapterBase#getVarCharKeyword()
	 */
	@Override
	public String getVarCharKeyword()
	{
		return "LONGVARCHAR";
	}

	@Override
	public String getVarCharKeyword(int length)
	{
		return getVarCharKeyword();
	}
	
	@Override
	public boolean columnTypesEqual(String type1,String type2)
	{
		//LONGVARCHAR is a synonym for VARCHAR
		if(type1.equalsIgnoreCase("LONGVARCHAR"))
		{
			type1="VARCHAR";
		}
		if(type2.equalsIgnoreCase("LONGVARCHAR"))
		{
			type2="VARCHAR";
		}
		return super.columnTypesEqual(type1, type2);
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#isValidColumnName(java.lang.String)
	 */
	@Override
	public boolean isValidColumnName(String name)
	{
		if(name.equalsIgnoreCase("COUNT"))
		{
			return false;
		}
		else
		{
			return super.isValidColumnName(name);
		}
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getLastInsertedIdentity(java.lang.String)
	 */
	@Override
	public String getLastInsertedIdentity(String tableName)
	{
		return "call identity()";
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
	 * @see org.conserve.adapter.AdapterBase#getBlobTypeKeyword()
	 */
	@Override
	public String getBlobTypeKeyword()
	{
		return "LONGVARBINARY";
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#isSupportsClob()
	 */
	@Override
	public boolean isSupportsClob()
	{
		return false;
	}
	
		
}
