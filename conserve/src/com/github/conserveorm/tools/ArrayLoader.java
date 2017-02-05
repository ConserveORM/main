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

import java.lang.reflect.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.github.conserveorm.adapter.AdapterBase;
import com.github.conserveorm.cache.ObjectRowMap;
import com.github.conserveorm.connection.ConnectionWrapper;
import com.github.conserveorm.tools.generators.NameGenerator;

/**
 * Loads arrays, arrays of arrays and so on.
 * 
 * @author Erik Berglund
 * 
 */
public class ArrayLoader
{
	private ConnectionWrapper connectionWrapper;
	private ObjectRowMap cache;
	private AdapterBase adapter;

	private Object array;
	private int dimensions;

	private String componentClassName;
	private String relationalTableName;


	/**
	 * The database IDs of the relational table entries - corresponds to the
	 * entries.
	 */
	private ArrayList<Long> relationalIds;

	public ArrayLoader(AdapterBase adapter, ObjectRowMap cache,
			ConnectionWrapper cw)
	{
		this.adapter = adapter;
		this.cache = cache;
		this.connectionWrapper = cw;
	}

	/**
	 * Get the array represented by a given id.
	 * 
	 * @param arrayId
	 * @throws SQLException
	 */
	public void loadArray(Long arrayId) throws SQLException
	{
		// first, get the array class
		componentClassName = null;
		StringBuilder statement = new StringBuilder("SELECT ");
		statement.append(Defaults.COMPONENT_CLASS_COL);
		statement.append(" FROM ");
		statement.append(Defaults.ARRAY_TABLENAME);
		statement.append(" WHERE ");
		statement.append(Defaults.ID_COL);
		statement.append(" = ? ");
		PreparedStatement ps = connectionWrapper.prepareStatement(statement
				.toString());
		ps.setLong(1, arrayId);
		Tools.logFine(ps);
		ResultSet rs = ps.executeQuery();

		if (rs.next())
		{
			Integer componentClassNameId = rs.getInt(1);
			componentClassName = adapter.getPersist().getClassNameNumberMap().getName(connectionWrapper, componentClassNameId);
			// then, load the array
			try
			{
				loadArray(componentClassName, arrayId);
			}
			catch (Exception e)
			{
				throw new SQLException(e);
			}
		}
		ps.close();

	}

	private void loadArray(String className, Long dbId)
			throws InstantiationException, IllegalAccessException,
			SQLException, ClassNotFoundException
	{
		setDimensions(className);
		Object tmpObject = cache.getObject(NameGenerator.getArrayTablename(adapter), dbId);
		if (tmpObject == null)
		{
			relationalIds = new ArrayList<Long>();
			// object not found in cache, load it from db
			if (getDimensions() == 1)
			{
				loadArray(ObjectTools.lookUpClass(className.replaceFirst(
						"\\[\\]", ""),adapter), dbId, connectionWrapper);
			}
			else
			{
				relationalTableName = Defaults.ARRAY_MEMBER_TABLE_NAME_ARRAY;
				// get the contained class name
				String containedClass = className.replaceFirst("\\[\\]", "");
				// get the ID of all the arrays contained in this array
				List<Long> subIds = getSubIds(dbId);
				List<ArrayLoader> subLoaders = new ArrayList<ArrayLoader>();
				// load all the arrays contained in this array
				for (Long subId : subIds)
				{
					ArrayLoader subLoader = new ArrayLoader(adapter, cache,
							this.connectionWrapper);
					subLoader.loadArray(containedClass, subId);
					subLoaders.add(subLoader);
					relationalIds.add(subId);
				}
				if (subLoaders.size() > 0)
				{
					// create a dimension array, so we can instantiate the right
					// type of array
					int[] dims = new int[dimensions];
					dims[0] = subLoaders.size();
					// get the name of the component type
					String innerName = className.replaceAll("\\[\\]", "");
					array = Array.newInstance(
							ObjectTools.lookUpClass(innerName,adapter), dims);
					int index = 0;
					for (ArrayLoader subLoader : subLoaders)
					{
						Array.set(array, index, subLoader.getArray());
						index++;
					}
				}
			}
			cache.storeObject(NameGenerator.getArrayTablename(adapter), array, dbId);
		}
		else
		{
			// object found in cache, use cached instance
			array = tmpObject;
		}
	}

	/**
	 * Load array with database id dbId.
	 * 
	 * @param c
	 *            the class of the array to get.
	 * @param dbId
	 *            the database id of the array.
	 * @param cw
	 *            a connection wrapper used for the current transaction.
	 * @return a new instance of the requested array, corresponding to the row
	 *         of the array table with id dbId.
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	private void loadArray(Class<?> c, Long dbId, ConnectionWrapper cw)
			throws InstantiationException, IllegalAccessException,
			SQLException, ClassNotFoundException
	{
		relationalTableName = NameGenerator.getArrayMemberTableName(c,
				this.adapter);
		boolean isReference = !ObjectTools.isDatabasePrimitive(c);
		StringBuilder statement = new StringBuilder("SELECT ");
		statement.append(Defaults.COMPONENT_CLASS_COL);
		statement.append(",");
		statement.append(Defaults.VALUE_COL);
		statement.append(" FROM ");
		statement.append(relationalTableName);
		statement.append(" WHERE ");
		statement.append(Defaults.ARRAY_MEMBER_ID);
		statement.append(" = ? ORDER BY ");
		statement.append(Defaults.ARRAY_POSITION);
		statement.append(" ASC");
		PreparedStatement ps = cw.prepareStatement(statement.toString());
		ps.setLong(1, dbId);
		Tools.logFine(ps);
		ResultSet rs = ps.executeQuery();
		ArrayList<Object> tmpList = new ArrayList<Object>();
		ArrayList<String> classNames = new ArrayList<String>();
		while (rs.next())
		{
			Integer componentClassNameId = rs.getInt(1);
			String componentClassName = adapter.getPersist().getClassNameNumberMap().getName(cw, componentClassNameId);
			classNames.add(componentClassName);
			tmpList.add(rs.getObject(2));
			if(isReference)
			{
				relationalIds.add(rs.getLong(2));
			}
			else
			{
				relationalIds.add(null);
			}

		}
		ps.close();

		array = Array.newInstance(c, tmpList.size());
		if (ObjectTools.isDatabasePrimitive(c))
		{
			for (int x = 0; x < tmpList.size(); x++)
			{
				Object value = tmpList.get(x);
				if (value instanceof Number)
				{
					Array.set(array, x,
							ObjectTools.cast(c, (Number) tmpList.get(x)));
				}
				else
				{
					Array.set(array, x, value);
				}
			}
		}
		else
		{
			// the object is not primitive, so value stored in tmpList refers to
			// foreign key to other table.
			for (int x = 0; x < tmpList.size(); x++)
			{
				Long foreignKey = ((Number) tmpList.get(x)).longValue();
				Object comp = adapter.getPersist().getObject(cw, c, foreignKey,
						cache);
				// only one object will correspond to the foreign key
				Array.set(array, x, comp);
			}
		}
	}

	/**
	 * Get the ids of all arrays that are members of a given array.
	 * 
	 * @param parentArrayId
	 * @return
	 * @throws SQLException
	 */
	private List<Long> getSubIds(Long parentArrayId) throws SQLException
	{
		StringBuilder sb = new StringBuilder("SELECT ");
		sb.append(Defaults.VALUE_COL);
		sb.append(" FROM ");
		sb.append(Defaults.ARRAY_MEMBER_TABLE_NAME_ARRAY);
		sb.append(" WHERE ");
		sb.append(Defaults.ARRAY_MEMBER_ID);
		sb.append(" = ? ORDER BY ");
		sb.append(Defaults.ARRAY_POSITION);
		sb.append(" ASC");
		PreparedStatement ps = connectionWrapper
				.prepareStatement(sb.toString());
		ps.setLong(1, parentArrayId);
		Tools.logFine(ps);
		try
		{
			ResultSet rs = ps.executeQuery();
			ArrayList<Long> res = new ArrayList<Long>();
			while (rs.next())
			{
				res.add(rs.getLong(1));
				relationalIds.add(rs.getLong(1));
			}
			return res;
		}
		finally
		{
			ps.close();
		}
	}

	/**
	 * Set the dimensions based on the number of "[]" strings in the class name.
	 * 
	 * @param className
	 */
	private void setDimensions(String className)
	{
		dimensions = 0;// no "[]" in class name indicates no array.
		while (className.contains("["))
		{
			// delete one instance
			className = className.replaceFirst("\\[\\]", "");
			// increment the dimension count
			dimensions++;
		}

	}

	/**
	 * Get the number of dimensions in this array. Ordinary arrays (e.g. Object
	 * [] foo) returns 1, arrays of arrays (e.g. Object [][] bar) return 2 etc.
	 * 
	 * @return the number of dimensions.
	 */
	public int getDimensions()
	{
		return this.dimensions;
	}

	/**
	 * Get the length of the topmost array level.
	 * 
	 * @return the length of the array.
	 */
	public int getLength()
	{
		return Array.getLength(array);
	}

	/**
	 * Get the content (either an object or another array) at the given index.
	 * 
	 * @param index
	 * @return an object if getDimensions() == 1, an array otherwise.
	 */
	public Object getEntry(int index)
	{
		return Array.get(array, index);
	}

	public Object getArray()
	{
		return array;
	}

	/**
	 * @return the relationalIds
	 */
	public ArrayList<Long> getRelationalIds()
	{
		return relationalIds;
	}

}
