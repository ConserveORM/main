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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.conserve.connection.ConnectionWrapper;
import org.conserve.tools.Defaults;
import org.conserve.tools.Tools;

/**
 * Keeps track of all the objects that are referenced (directly or indirectly) by a certain object, as well as all
 * objects that reference them.
 * 
 * @author Erik Berglund
 * 
 */
public class DependentSet
{

	private boolean protectedEntry;

	private List<ProtectionEntry>cacheKey= new ArrayList<ProtectionEntry>();
	private List<List<ProtectionEntry>>cacheValue= new ArrayList<List<ProtectionEntry>>();
	
	/**
	 * Create and populate a new set of entries that are dependent on the given entry.
	 * 
	 * @param tableName
	 * @param id
	 * @param cw
	 * @throws SQLException
	 */
	public DependentSet(String tableName, Long id, ConnectionWrapper cw) throws SQLException
	{
		ProtectionEntry candidate = new ProtectionEntry(tableName, id);
		List<ProtectionEntry> candidates = new ArrayList<ProtectionEntry>();
		candidates.add(candidate);
		List<ProtectionEntry> entries = new ArrayList<ProtectionEntry>(candidates);
		// find all entries that will be deleted if candidate is deleted
		populateList(entries, candidates, cw);
		// remove all objects that have references not in list
		cullEntries(entries, cw);
		// if the candidate is in the set, remove it
		if (entries.contains(candidate))
		{
			protectedEntry = false;
		}
		else
		{
			protectedEntry = true;
		}
	}

	public boolean isProtected()
	{
		return protectedEntry;
	}

	/**
	 * Remove all entries that are referenced from outside the list.
	 * 
	 * @param entries
	 * @throws SQLException
	 */
	private void cullEntries(List<ProtectionEntry> entries, ConnectionWrapper cw) throws SQLException
	{
		ProtectionEntry toDelete = null;
		do
		{
			toDelete = null;
			outerLoop: for (ProtectionEntry entry : entries)
			{
				List<ProtectionEntry> referencers = this.listAllDependingEntries(entry, cw);
				for (ProtectionEntry ref : referencers)
				{
					if (ref.getPropertyId() == null)
					{
						// external reference, object can't be deleted
						// remove it
						toDelete = entry;
						break outerLoop;
					}
					// check all non-self-referencing entries
					if (!ref.equals(entry))
					{
						if (!entries.contains(ref))
						{
							// the object has a reference that is not in the
							// list,
							// remove it
							toDelete = entry;
							// break the loop
							break outerLoop;
						}
					}
				}
			}
			if (toDelete != null)
			{
				entries.remove(toDelete);
			}
		} while (toDelete != null);
	}

	private void populateList(List<ProtectionEntry> entries, List<ProtectionEntry> candidates, ConnectionWrapper cw)
			throws SQLException
	{
		List<ProtectionEntry> nuCandidates = new ArrayList<ProtectionEntry>();
		for (ProtectionEntry pe : candidates)
		{
			List<ProtectionEntry> tmp = listAllDependentEntries(pe, cw);
			for (ProtectionEntry tmpEntr : tmp)
			{
				if (!candidates.contains(tmpEntr) && !entries.contains(tmpEntr))
				{
					nuCandidates.add(tmpEntr);
				}
			}
		}
		if (nuCandidates.size() > 0)
		{
			entries.addAll(nuCandidates);
			populateList(entries, nuCandidates, cw);
		}
	}

	/**
	 * Get a list of all objects that are references by the given entry.
	 * 
	 * @param entry
	 * @param cw
	 * @return
	 * @throws SQLException
	 */
	private List<ProtectionEntry> listAllDependentEntries(ProtectionEntry entry, ConnectionWrapper cw) throws SQLException
	{
		ArrayList<ProtectionEntry> res = new ArrayList<ProtectionEntry>();
		StringBuilder statement = new StringBuilder(100);
		statement.append("SELECT PROPERTY_TABLE, PROPERTY_ID FROM ");
		statement.append(Defaults.HAS_A_TABLENAME);
		statement.append(" WHERE OWNER_TABLE = ? AND OWNER_ID = ?");
		PreparedStatement ps = cw.prepareStatement(statement.toString());
		ps.setString(1, entry.getPropertyTableName());
		ps.setLong(2, entry.getPropertyId());
		Tools.logFine(ps);
		try
		{
			ResultSet rs = ps.executeQuery();
			while (rs.next())
			{
				ProtectionEntry nuEntry = new ProtectionEntry(rs.getString(1), rs.getLong(2));
				res.add(nuEntry);
			}
		}
		finally
		{
			ps.close();
		}
		return res;
	}

	/**
	 * Get a list of all objects that references the given entry.
	 * 
	 * @param entry
	 * @param cw
	 * @return
	 * @throws SQLException
	 */
	private List<ProtectionEntry> listAllDependingEntries(ProtectionEntry entry, ConnectionWrapper cw) throws SQLException
	{

		List<ProtectionEntry> res = null;
		int index = cacheKey.indexOf(entry);
		if (index<0)
		{
			res = new ArrayList<ProtectionEntry>();
			StringBuilder statement = new StringBuilder(100);
			statement.append("SELECT OWNER_TABLE, OWNER_ID FROM ");
			statement.append(Defaults.HAS_A_TABLENAME);
			statement.append(" WHERE PROPERTY_TABLE = ? AND PROPERTY_ID = ?");
			PreparedStatement ps = cw.prepareStatement(statement.toString());
			ps.setString(1, entry.getPropertyTableName());
			ps.setLong(2, entry.getPropertyId());
			Tools.logFine(ps);
			try
			{
				ResultSet rs = ps.executeQuery();
				while (rs.next())
				{
					ProtectionEntry nuEntry = new ProtectionEntry(rs.getString(1), rs.getLong(2));
					res.add(nuEntry);
				}
			}
			finally
			{
				ps.close();
			}
			cacheKey.add(entry);
			cacheValue.add(res);
		}
		else
		{
			res = cacheValue.get(index);
		}
		return res;
	}

}
