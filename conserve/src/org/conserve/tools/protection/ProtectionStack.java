/*******************************************************************************
 * Copyright (c) 2009, 2012 Erik Berglund.
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
package org.conserve.tools.protection;

import java.sql.SQLException;
import java.util.ArrayList;

import org.conserve.connection.ConnectionWrapper;

/**
 * 
 * Maintains a stack of protection entries.
 * 
 * @author Erik Berglund
 * 
 */
public class ProtectionStack
{
	private ArrayList<ProtectionEntry> protectionEntries = new ArrayList<ProtectionEntry>();
	private ProtectionManager protectionManager;

	public ProtectionStack(ProtectionManager protectionManager)
	{
		this.protectionManager = protectionManager;
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
		for (ProtectionEntry pe : protectionEntries)
		{
			pe.save(ownerTableName, ownerId, protectionManager, cw);
		}
	}

}