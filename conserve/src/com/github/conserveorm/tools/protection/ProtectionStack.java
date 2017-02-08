/*******************************************************************************
 *  
 * Copyright (c) 2009, 2017 Erik Berglund.
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
package com.github.conserveorm.tools.protection;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.github.conserveorm.adapter.AdapterBase;
import com.github.conserveorm.connection.ConnectionWrapper;


/**
 * 
 * Maintains a stack of protection entries.
 * 
 * @author Erik Berglund
 * 
 */
public class ProtectionStack
{
	private List<ProtectionEntry> protectionEntries = new ArrayList<ProtectionEntry>();
	private AdapterBase adapter;

	public ProtectionStack(AdapterBase adapter)
	{
		this.adapter = adapter;
	}

	/**
	 * Add entries that will be added to the HAS_A table relating to this object
	 * when done.
	 * 
	 * @param protectionEntry
	 *            a protection entry that will be saved by this stack.
	 */
	public void addEntry(ProtectionEntry protectionEntry)
	{
		this.protectionEntries.add(protectionEntry);
	}

	/**
	 * Save all the protection entries in this stack as belonging to the given
	 * owner table and id.
	 * 
	 * @param ownerTableName
	 * @param ownerId
	 * @param cw
	 * @throws SQLException
	 */
	public void save(String ownerTableName, long ownerId, ConnectionWrapper cw)
			throws SQLException
	{
		Integer ownerTableId = adapter.getPersist().getTableNameNumberMap().getNumber(cw, ownerTableName);
		for (ProtectionEntry pe : protectionEntries)
		{
			pe.save(ownerTableId, ownerId, adapter.getPersist().getProtectionManager(), cw);
		}
	}

}
