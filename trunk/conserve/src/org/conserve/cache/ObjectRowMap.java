/*******************************************************************************
 * Copyright (c) 2009, 2013 Erik Berglund.
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
package org.conserve.cache;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

/**
 * Maintains a map between objects that have been loaded and their
 * System.identityHash(...) value. This lets us know if an object has been
 * loaded (and get a reference to it), and the __id of any stored object.
 * 
 * @author Erik Berglund
 * 
 */
public class ObjectRowMap implements Runnable
{
	/**
	 * Map from the systemHashCode to the database table and id
	 */
	private HashMap<Long, List<TableEntry>> classMap = new HashMap<Long, List<TableEntry>>();
	/**
	 * Map from the database table and database id to the object.
	 */
	private HashMap<String, HashMap<Long, WeakReference<Object>>> objectMap = new HashMap<String, HashMap<Long, WeakReference<Object>>>();

	/**
	 * Map that maps from a weakreference to the systemHashCode it is mapped
	 * under. This is to ensure safe deletion of the key.
	 */
	private HashMap<WeakReference<Object>, Long> inverseReferenceMap = new HashMap<WeakReference<Object>, Long>();

	/**
	 * Reference queue where References for deleted objects are placed.
	 */
	private ReferenceQueue<Object> refQueue = new ReferenceQueue<Object>();
	private boolean running = true;

	/**
	 * Object constructor.
	 */
	public ObjectRowMap()
	{
	}

	/**
	 * Start the thread purging old entries.
	 */
	public void start()
	{
		Thread queueThread = new Thread(this);
		queueThread.setDaemon(true);
		queueThread.start();
	}

	/**
	 * Store the identity of a given object in both directional maps.
	 * 
	 * @param o
	 * @param dbId
	 */
	public void storeObject(String tableName, Object o, long dbId)
	{
		long id = System.identityHashCode(o);

		// store the id -> dbId map
		List<TableEntry> list = classMap.get(id);
		if (list == null)
		{
			list = new ArrayList<TableEntry>();
			classMap.put(id, list);
		}
		list.add(new TableEntry(tableName, dbId, o));

		// store the dbId -> object map
		WeakReference<Object> ref = new WeakReference<Object>(o, refQueue);
		HashMap<Long, WeakReference<Object>> map = objectMap.get(tableName);
		if (map == null)
		{
			map = new HashMap<Long, WeakReference<Object>>();
			objectMap.put(tableName, map);
		}
		map.put(dbId, ref);

		// store the object->id map
		inverseReferenceMap.put(ref, id);
	}

	/**
	 * Get the object from tablename with id, if it has been previously loaded.
	 * 
	 * @param tableName
	 *            the table name to look in.
	 * @param id
	 *            the identifier of the object within the table.
	 * @return null if the object is not found.
	 */
	public Object getObject(String tableName, long id)
	{
		HashMap<Long, WeakReference<Object>> map = objectMap.get(tableName);
		if (map == null)
		{
			return null;
		}
		WeakReference<Object> ref = map.get(id);
		if (ref == null || ref.isEnqueued())
		{
			// the object has been collected by the garbage collector, or is not
			// known
			return null;
		}
		return ref.get();
	}

	/**
	 * Get the unique database id of this object. Return null if the object is
	 * not known.
	 * 
	 * @param o
	 *            the object to get the database id for.
	 * @return the database id, or null if the object is not known.
	 */
	public Long getDatabaseId(Object o)
	{
		long id = System.identityHashCode(o);
		List<TableEntry> list = classMap.get(id);
		if (list != null)
		{
			for (TableEntry entry : list)
			{
				if (entry.refernetEquals(o))
				{
					return entry.getDbId();
				}
			}
		}
		return null;
	}

	/**
	 * Remove an object based on its identityHashCode.
	 * 
	 * @param id
	 *            The identityHashCode of the object to purge
	 */
	public void purge(Object o)
	{
		Long id = (long) System.identityHashCode(o);
		// remove from the id -> dbId map, if exists
		List<TableEntry> list = classMap.get(id);
		if (list != null)
		{
			TableEntry toRemove = null;
			for (TableEntry entry : list)
			{
				if (entry.refernetEquals(o))
				{
					toRemove = entry;
					break;
				}
			}
			if (toRemove != null)
			{
				list.remove(toRemove);
				if (list.isEmpty())
				{
					classMap.remove(id);
				}

				HashMap<Long, WeakReference<Object>> map = objectMap
						.get(toRemove.getTableName());
				WeakReference<Object> ref = map.get(toRemove.getDbId());
				if (ref != null)
				{
					map.remove(toRemove.getDbId());
					if (map.isEmpty())
					{
						objectMap.remove(toRemove.getTableName());
					}
					inverseReferenceMap.remove(ref);
				}
			}
		}
	}

	/**
	 * Remove an object based on its table name and database id.
	 * 
	 * @param tableName
	 * @param dbId
	 */
	public void purge(String tableName, Long dbId)
	{
		Object o = getObject(tableName, dbId);
		if (o != null)
		{
			purge((long) System.identityHashCode(o));
		}
	}

	/**
	 * Remove all objects based on their table name.
	 * 
	 * @param tableName
	 */
	public void purge(String tableName)
	{

		HashMap<Long, WeakReference<Object>> map = objectMap.get(tableName);
		List<Object> toRemove = new ArrayList<Object>();
		if (map != null)
		{
			for (Entry<Long, WeakReference<Object>> e : map.entrySet())
			{

				WeakReference<Object> ref = e.getValue();
				if (ref != null && !ref.isEnqueued())
				{
					toRemove.add(ref.get());
				}
			}
			for (Object o : toRemove)
			{
				purge(o);
			}
		}
	}

	/**
	 * Change the stored name of all objects with a given name
	 * 
	 * @param oldName
	 * @param newName
	 */
	public void changeName(String oldName, String newName)
	{
		// fix all the table-entries
		for (List<TableEntry> list : classMap.values())
		{
			for (TableEntry entry : list)
			{
				if (entry.getTableName().equals(oldName))
				{
					entry.setTableName(newName);
				}
			}
		}

		// change the dbId -> object map
		HashMap<Long, WeakReference<Object>> map = objectMap.get(oldName);
		if (map != null)
		{
			objectMap.remove(oldName);
			objectMap.put(newName, map);
		}
	}

	public void stop()
	{
		running = false;
	}

	@Override
	public void run()
	{
		while (running)
		{
			try
			{
				// get a reference from the queue
				// this call blocks until a reference becomes available or it
				// times out
				Reference<? extends Object> ref = this.refQueue.remove(200);
				if (ref != null)
				{
					// remove the reference from the map
					Long key = inverseReferenceMap.get(ref);
					if (key != null)
					{
						purge(key);
					}
				}
			}
			catch (InterruptedException e)
			{
				// do nothing
			}
		}
	}

	/**
	 * Inner class for keeping track of the relation between database IDs and
	 * table names.
	 */
	private static class TableEntry
	{
		private String tableName;
		private Long dbId;
		private WeakReference<Object> ref;

		public TableEntry(String tableName, Long dbId, Object o)
		{
			this.tableName = tableName;
			this.dbId = dbId;
			this.ref = new WeakReference<Object>(o);
		}

		public boolean refernetEquals(Object o)
		{
			return ref.get() == o;
		}

		public String getTableName()
		{
			return tableName;
		}

		public void setTableName(String tableName)
		{
			this.tableName = tableName;
		}

		public Long getDbId()
		{
			return dbId;
		}

	}

}
