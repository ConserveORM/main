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
package com.github.conserveorm.cache;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
	 * Map from an object to its weak reference
	 */
	private WeakRefMap objectToRef = new WeakRefMap();

	/**
	 * Map from table entry (table name and database id) to an object.
	 */
	private Map<TableEntry, WeakReference<Object>> tableToReference = new ConcurrentHashMap<>();

	/**
	 * Map from a weak reference to the table entry (table name and database
	 * id).
	 */
	private Map<WeakReference<Object>, TableEntry> referenceToTable = new ConcurrentHashMap<>();

	/**
	 * Reference queue where References for deleted objects are placed.
	 */
	private ReferenceQueue<Object> deletedObjectQueue = new ReferenceQueue<Object>();
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
		queueThread.setName(this.getClass().getName());
		queueThread.setDaemon(true);
		queueThread.start();
	}

	/**
	 * Store the identity of a given object in both directional maps.
	 * 
	 * @param obj
	 * @param dbId
	 */
	public void storeObject(String tableName, Object obj, long dbId)
	{

		TableEntry te = new TableEntry(tableName, dbId);
		WeakReference<Object> wref = new WeakReference<Object>(obj, deletedObjectQueue);
		tableToReference.put(te, wref);
		referenceToTable.put(wref, te);
		objectToRef.put(obj, wref);
	}

	/**
	 * Get the object from tablename with id, if it has been previously loaded.
	 * 
	 * @param tableName
	 *            the table name to look in.
	 * @param dbId
	 *            the identifier of the object within the table.
	 * @return null if the object is not found.
	 */
	public Object getObject(String tableName, long dbId)
	{
		Object res = null;
		TableEntry te = new TableEntry(tableName, dbId);
		WeakReference<Object> wref = tableToReference.get(te);
		if (wref != null)
		{
			res = wref.get();
		}
		return res;
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
		Long res = null;
		WeakReference<Object> wref = objectToRef.get(o);
		if (wref != null)
		{
			TableEntry te = referenceToTable.get(wref);
			if (te != null)
			{
				res = te.getDbId();
			}
		}
		return res;
	}

	/**
	 * Remove an object based on its table name and database id.
	 * 
	 * @param tableName
	 * @param dbId
	 */
	public void purge(String tableName, Long dbId)
	{
		TableEntry te = new TableEntry(tableName, dbId);
		WeakReference<Object> wref = tableToReference.get(te);
		if (wref != null)
		{
			purge(wref);
		}
	}

	/**
	 * Remove all objects based on their table name.
	 * 
	 * @param tableName
	 */
	public void purge(String tableName)
	{
		List<TableEntry> toRemove = new ArrayList<>();
		Set<TableEntry> keySet = tableToReference.keySet();
		for (TableEntry key : keySet)
		{
			if (key.getTableName().equals(tableName))
			{
				toRemove.add(key);
			}
		}
		for (TableEntry remove : toRemove)
		{
			purge(remove.getTableName(), remove.getDbId());
		}
	}

	/**
	 * Purge an object based on its weak reference. This method is called after
	 * garbage collection, so wref's referent does not exist.
	 * 
	 * @param wref
	 */
	private void purge(WeakReference<Object> wref)
	{
		TableEntry te = referenceToTable.get(wref);
		if (te != null)
		{
			tableToReference.remove(te);
			referenceToTable.remove(wref);
		}
		objectToRef.remove(wref);
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
				@SuppressWarnings("unchecked")
				WeakReference<Object> ref = (WeakReference<Object>) this.deletedObjectQueue.remove(200);
				if (ref != null)
				{
					// remove the reference from the map
					purge(ref);
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

		public TableEntry(String tableName, Long dbId)
		{
			this.tableName = tableName;
			this.dbId = dbId;
		}

		public String getTableName()
		{
			return tableName;
		}

		public Long getDbId()
		{
			return dbId;
		}

		/**
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj)
		{
			boolean res = false;
			if (obj instanceof TableEntry)
			{
				TableEntry other = (TableEntry) obj;
				if(other.tableName.equals(tableName))
				{
					if(other.dbId.equals(dbId))
					{
						res = true;
					}
				}
			}
			return res;
		}

		/**
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode()
		{
			return tableName.hashCode() + dbId.hashCode();
		}

	}

	/**
	 * A class that maps an object to a weak reference. This class is not
	 * reentrant, thread safety is handled by the enclosing class.
	 * 
	 */
	private class WeakRefMap
	{
		private Map<Long, List<WeakReference<Object>>> buckets = new ConcurrentHashMap<>();

		public void put(Object obj, WeakReference<Object> wr)
		{
			Long id = (long) System.identityHashCode(obj);
			List<WeakReference<Object>> bucket = buckets.get(id);
			if (bucket == null)
			{
				bucket = new ArrayList<>();
				buckets.put(id, bucket);
			}
			synchronized (bucket)
			{
				if (!bucket.contains(wr))
				{
					bucket.add(wr);
				}
			}
		}

		public WeakReference<Object> get(Object obj)
		{
			WeakReference<Object> res = null;
			Long id = (long) System.identityHashCode(obj);
			List<WeakReference<Object>> bucket = buckets.get(id);
			if (bucket != null)
			{
				synchronized (bucket)
				{
					// iterate over the references in the bucket until we find
					// one
					// that references obj
					for (WeakReference<Object> ref : bucket)
					{
						if (obj == ref.get())
						{
							res = ref;
							break;
						}
					}
				}
			}
			return res;
		}


		/**
		 * This is used to clean up after an object has been claimed by GC.
		 * Since the referent is no longer availalbe we'll have to iterate over
		 * the buckets until we find it.
		 * 
		 * @param wref
		 */
		public void remove(WeakReference<Object> wref)
		{
			Set<Entry<Long, List<WeakReference<Object>>>> entrySet = buckets.entrySet();
			// iterate over the buckets
			Iterator<Entry<Long, List<WeakReference<Object>>>> iterator = entrySet.iterator();
			while (iterator.hasNext())
			{
				Entry<Long, List<WeakReference<Object>>> next = iterator.next();
				List<WeakReference<Object>> value = next.getValue();
				if (value != null)
				{
					synchronized (value)
					{
						if (value.contains(wref))
						{
							value.remove(wref);
							if (value.isEmpty())
							{
								// we've removed the last wref with this id,
								// remove the bucket
								iterator.remove();
							}
							break;
						}
					}
				}
			}
		}
	}

}
