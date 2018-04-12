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

import java.util.Properties;

import com.github.conserveorm.Persist;

/**
 * An adapter for the SQLDroid JDBC implementation. It's almost identical to the SQLite adapter.
 * 
 * @author Erik Berglund
 */
public class SqlDroidAdapter extends SqLiteAdapter
{
	public SqlDroidAdapter(Persist persist)
	{
		super(persist);
	}
	
	@Override
	public Properties getAdapterSpecificProperties()
	{
		Properties res = super.getAdapterSpecificProperties();
		//set android.database.sqlite.SQLiteDatabase.NO_LOCALIZED_COLLATORS flag
		res.put("AdditionalDatabaseFlags", 16);
		return res;
	}
	
	@Override
	public boolean getCatalogIsBroken()
	{
		//the getCatalog call isn't implemented in SQLDroid 1.0.3.
		return true;
	}
	
	@Override
	public boolean getObjectIsBroken()
	{
		return true;
	}
	
	@Override
	public boolean setBooleanIsBroken()
	{
		return true;
	}
}
