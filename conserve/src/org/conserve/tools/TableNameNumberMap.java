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
package org.conserve.tools;

import java.sql.SQLException;

import org.conserve.adapter.AdapterBase;
import org.conserve.connection.ConnectionWrapper;
import org.conserve.tools.generators.NameGenerator;

/**
 * NameNumberMap subclass that keeps track of table names.
 * 
 * @author Erik Berglund
 *
 */
public class TableNameNumberMap extends NameNumberMap
{

	/**
	 * @param adapter
	 */
	public TableNameNumberMap(AdapterBase adapter)
	{
		super(adapter, Defaults.TABLE_NAME_MAP_TABLE);
	}
	
	/**
	 * Get the number for the table name associated with the given class. Insert the table name if it does not exist.
	 * 
	 * @param cw
	 * @param clazz
	 * @throws SQLException 
	 */
	public Integer getNumber(ConnectionWrapper cw, Class<?>clazz) throws SQLException
	{
		return getNumber(cw,NameGenerator.getTableName(clazz, adapter));
	}

}
