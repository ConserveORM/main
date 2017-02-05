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
package com.github.conserveorm.tools;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.conserveorm.Persist;
import com.github.conserveorm.connection.ConnectionWrapper;
import com.github.conserveorm.tools.generators.NameGenerator;
import com.github.conserveorm.tools.protection.ProtectionManager;


/**
 * 
 * Maintains a list of objects to be inserted at a later time, with the
 * associated reference.
 * 
 * @author Erik Berglund
 * 
 */
public class DelayedInsertionBuffer
{
	private ArrayList<InsertionObject> buffer = new ArrayList<InsertionObject>();
	private Map<Long,List<Object>> idBuffer = new HashMap<Long,List<Object>>();
	private Persist persist;


	public DelayedInsertionBuffer(Persist persist)
	{
		this.persist = persist;
	}

	public boolean isKnown(Long id, Object obj)
	{
		List<Object> list = idBuffer.get(id);
		if(list != null)
		{
			for(Object o:list)
			{
				if(o == obj)
				{
					return true;
				}
			}
		}
		return false;
	}

	public void addId(Long id, Object obj)
	{
		List<Object> list = idBuffer.get(id);
		if(list == null)
		{
			list = new ArrayList<Object>();
			idBuffer.put(id, list);
		}
		list.add(obj);
	}

	/**
	 * Insert a new object to be added later.
	 * 
	 * @param tableName
	 *            the name of the table the object is referred from.
	 * @param columnName
	 *            the name of the column that references the object.
	 * @param parentId
	 *            the C__ID value of the referring object.
	 * @param insertionObject
	 *            the referred object.
	 * @param referencetype
	 *            the type to cast the reference to.
	 * @param hash
	 *            the result of the System.identityHash code for the owning
	 *            object.
	 */
	public void addArrayEntry(String tableName, String columnName, Long parentId,
			Object insertionObject, Class<?> referencetype, long hash,String arrayTableName, Long arrayId)
	{
		buffer.add(new InsertionObject(tableName, columnName, parentId, insertionObject,
				referencetype, hash,arrayTableName, arrayId));
	}

	/**
	 * Insert a new object to be added later. The parent id of the inserting
	 * object is undefined.
	 * 
	 * @param tableName
	 *            the name of the table the object is referred from.
	 * @param columnName
	 *            the name of the column that references the object.
	 * @param insertionObject
	 *            the referred object.
	 * @param referencetype
	 *            the type to cast the reference to.
	 * @param hash
	 *            the result of the System.identityHash code for the owning
	 *            object.
	 */
	public void add(String tableName, String columnName, Object insertionObject,
			Class<?> referencetype, long hash)
	{

		buffer.add(new InsertionObject(tableName, columnName, null, insertionObject,
				referencetype, hash));
	}

	/**
	 * Set the parent-id for all entries that do not have it set.
	 * 
	 * @param id
	 */
	public void setUndefinedIds(Long id,long hash)
	{

		for (InsertionObject i : buffer)
		{
			if (i.getParentId() == null && i.getParentHash()==hash)
			{
				i.setParentId(id);
			}
		}
	}

	public void insertObjects(ConnectionWrapper cw, ProtectionManager protectionManager)
			throws SQLException
	{
		ArrayList<InsertionObject> toRemove = new ArrayList<InsertionObject>();
		for (InsertionObject i : buffer)
		{
			// insert the object
			Long id = persist.saveObjectUnprotected(cw, i.getInsertionObject(), this);
			// get the object id
			if (id != null)
			{
				// update the referring table entry
				StringBuilder sb = new StringBuilder("UPDATE ");
				sb.append(i.getTableName());
				sb.append(" SET ");
				sb.append(i.getColumnName());
				sb.append(" = ?");
				sb.append(" WHERE ");
				sb.append(Defaults.ID_COL);
				sb.append(" = ?");
				PreparedStatement ps = cw.prepareStatement(sb.toString());
				ps.setLong(1, id);
				ps.setLong(2, i.getParentId());
				Tools.logFine(ps);
				try
				{
					int updated = ps.executeUpdate();
					// make sure only one object was updated.
					if (updated != 1)
					{
						throw new SQLException("Updated " + updated + ", not 1.");
					}
					// add a protection entry
					String tableName = null;
					if (i.getReferenceType().isArray())
					{
						tableName = NameGenerator.getArrayTablename(persist.getAdapter());
					}
					else
					{
						tableName = NameGenerator.getTableName(i.getReferenceType(),
								persist.getAdapter());
					}
					protectionManager.protectObjectInternal(i.getProtectionParentTable(), i
							.getProtectionParentId(), i.getColumnName(), tableName, id,
							NameGenerator.getSystemicName(i.getInsertionObject()
									.getClass()), cw);

					// if the object was successfully inserted, remove it
					// from the buffer.
					toRemove.add(i);
				}
				finally
				{
					ps.close();
				}
			}
		}
		for (InsertionObject i : toRemove)
		{
			buffer.remove(i);
		}

	}

	/**
	 * Check if the buffer is empty.
	 * 
	 * @return true if there are no more delayed insertion objects in the
	 *         buffer.
	 */
	public boolean isEmpty()
	{
		return buffer.isEmpty();
	}

	private static class InsertionObject
	{
		private String tableName;
		private String columnName;
		private Object insertionObject;
		private Long parentId;
		private long parentHash;
		private String protectionParentTable;
		private Long protectionParentId;
		private Class<?> referenceType;

		public InsertionObject(String tableName, String columnName, Long parentId,
				Object insertionObject, Class<?> referenceType, Long parentHash)
		{
			this(tableName,columnName,parentId,insertionObject,referenceType,parentHash,tableName,parentId);
		}

		public InsertionObject(String tableName, String columnName,
				Long parentId, Object insertionObject, Class<?> referenceType,
				Long parentHash, String protectionParentTable, Long protectionParentId)
		{
			this.tableName = tableName;
			this.columnName = columnName;
			this.protectionParentId = protectionParentId;
			this.setParentId(parentId);
			this.insertionObject = insertionObject;
			this.referenceType = referenceType;
			this.protectionParentTable = protectionParentTable;
			this.setParentHash(parentHash);
		}

		public String getTableName()
		{
			return tableName;
		}

		public String getColumnName()
		{
			return columnName;
		}

		public Object getInsertionObject()
		{
			return insertionObject;
		}

		public Class<?> getReferenceType()
		{
			return referenceType;
		}

		public void setParentId(Long parentId)
		{
			this.parentId = parentId;
			if(this.protectionParentId==null)
			{
				this.protectionParentId=parentId;
			}
		}

		public Long getParentId()
		{
			return parentId;
		}
		
		public Long getProtectionParentId()
		{
			return protectionParentId;
		}
		
		public String getProtectionParentTable()
		{
			return protectionParentTable;
		}

		/**
		 * @return the parentHash
		 */
		public long getParentHash()
		{
			return parentHash;
		}

		/**
		 * @param parentHash
		 *            the parentHash to set
		 */
		public void setParentHash(long parentHash)
		{
			this.parentHash = parentHash;
		}
	}
}
