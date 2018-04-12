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
package com.github.conserveorm.tools.protection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.conserveorm.connection.ConnectionWrapper;
import com.github.conserveorm.tools.Defaults;
import com.github.conserveorm.tools.Tools;

/**
 * Tracks all the objects that are referenced (directly or indirectly) by a certain object, as well as all
 * objects that reference them.
 * 
 * @author Erik Berglund
 * 
 */
public class DependentSet
{

	private boolean protectedEntry;

	private Map<ProtectionEntry,List<ProtectionEntry>>dependingCache=new HashMap<>();
	
	/**
	 * Create and populate a new set of entries that are dependent on the given entry.
	 * 
	 * @param id
	 * @param cw
	 * @throws SQLException
	 */
	public DependentSet( Long id, ConnectionWrapper cw) throws SQLException
	{
		ProtectionEntry candidate = new ProtectionEntry(id);
		List<ProtectionEntry> candidates = new ArrayList<ProtectionEntry>();
		candidates.add(candidate);
		List<ProtectionEntry> entries = new ArrayList<ProtectionEntry>(candidates);
		// find all entries that will be deleted if candidate is deleted
		populateList(entries, candidates, cw);
		// remove all objects that have references not in list
		cullEntries(entries, cw);
		//entries now only contains objects that do not have external references.
		// if the candidate is in entries, it does not have any references pointing to it and
		// is not protected.
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
	 * Get a list of all objects that are referenced by the given entry.
	 * 
	 * @param entry
	 * @param cw
	 * @return
	 * @throws SQLException
	 */
	private List<ProtectionEntry> listAllDependentEntries(ProtectionEntry entry, ConnectionWrapper cw) throws SQLException
	{
		List<ProtectionEntry> res = new ArrayList<ProtectionEntry>();
		StringBuilder statement = new StringBuilder(100);
		statement.append("SELECT  PROPERTY_ID FROM ");
		statement.append(Defaults.HAS_A_TABLENAME);
		statement.append(" WHERE OWNER_ID = ?");
		PreparedStatement ps = cw.prepareStatement(statement.toString());
		ps.setLong(1, entry.getPropertyId());
		Tools.logFine(ps);
		try
		{
			ResultSet rs = ps.executeQuery();
			while (rs.next())
			{
				Long value = rs.getLong(1);
				if(rs.wasNull())
				{
					value = null;
				}
				ProtectionEntry nuEntry = new ProtectionEntry(value);
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

		List<ProtectionEntry> res = dependingCache.get(entry);
		if (res==null)
		{
			res = new ArrayList<ProtectionEntry>();
			StringBuilder statement = new StringBuilder(100);
			statement.append("SELECT OWNER_ID FROM ");
			statement.append(Defaults.HAS_A_TABLENAME);
			statement.append(" WHERE PROPERTY_ID = ?");
			PreparedStatement ps = cw.prepareStatement(statement.toString());
			ps.setLong(1, entry.getPropertyId());
			Tools.logFine(ps);
			try
			{
				ResultSet rs = ps.executeQuery();
				while (rs.next())
				{
					Long value = rs.getLong(1);
					if(rs.wasNull())
					{
						value = null;
					}
					ProtectionEntry nuEntry = new ProtectionEntry( value);
					res.add(nuEntry);
				}
			}
			finally
			{
				ps.close();
			}
			dependingCache.put(entry, res);
		}
		return res;
	}

}
