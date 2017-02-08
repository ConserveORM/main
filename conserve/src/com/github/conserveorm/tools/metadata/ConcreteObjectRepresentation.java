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
package com.github.conserveorm.tools.metadata;

import java.lang.reflect.Method;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.github.conserveorm.adapter.AdapterBase;
import com.github.conserveorm.annotations.AsBlob;
import com.github.conserveorm.annotations.AsClob;
import com.github.conserveorm.annotations.Indexed;
import com.github.conserveorm.annotations.MaxLength;
import com.github.conserveorm.annotations.MultiIndexed;
import com.github.conserveorm.connection.ConnectionWrapper;
import com.github.conserveorm.exceptions.SchemaPermissionException;
import com.github.conserveorm.tools.Defaults;
import com.github.conserveorm.tools.DelayedInsertionBuffer;
import com.github.conserveorm.tools.ObjectTools;
import com.github.conserveorm.tools.Tools;
import com.github.conserveorm.tools.generators.NameGenerator;
import com.github.conserveorm.tools.protection.ProtectionEntry;
import com.github.conserveorm.tools.protection.ProtectionStack;

/**
 * An object representation loaded by direct examination of the actual,
 * in-memory object graph.
 * 
 * @author Erik Berglund
 *
 */
public class ConcreteObjectRepresentation extends ObjectRepresentation
{

	public ConcreteObjectRepresentation(AdapterBase adapter, Class<?> c, Object o, DelayedInsertionBuffer delayBuffer)
	{
		this.protectionStack = new ProtectionStack(adapter);
		this.delayBuffer = delayBuffer;
		this.adapter = adapter;
		this.object = o;
		this.clazz = c;
		Class<?> actualClass = c;
		if (o != null)
		{
			actualClass = o.getClass();
		}
		Method[] methods = c.getDeclaredMethods();
		if (c.isArray())
		{
			tableName = NameGenerator.getArrayMemberTableName(c.getComponentType(), adapter);
		}
		else
		{
			tableName = NameGenerator.getTableName(c, adapter);
		}
		for (Method m : methods)
		{
			if (ObjectTools.isValidMethod(m))
			{
				String name = NameGenerator.getColumnName(m);
				while (!adapter.isValidColumnName(name))
				{
					// create a valid name by pre-pending a string
					name = "C_" + name;
				}
				try
				{
					props.add(name);
					// note that we need to use the actual class, to prevent an
					// exception
					Method mutator = ObjectTools.getMutator(actualClass, getMutatorName(m), m.getReturnType());
					Method getter = ObjectTools.getAccessor(actualClass, m.getName());
					setters.add(mutator);
					getters.add(getter);
					if (o != null && getter != null)
					{
						boolean oldAccessValue = getter.isAccessible();
						getter.setAccessible(true);
						Object value = getter.invoke(o);
						values.add(value);
						getter.setAccessible(oldAccessValue);
					}
					else
					{
						values.add(null);
					}

					// handle BLOB/CLOB annotations
					if (m.isAnnotationPresent(AsClob.class) && m.getReturnType().equals(char[].class) && adapter.isSupportsClob())
					{
						returnTypes.add(Clob.class);
					}
					else if (m.isAnnotationPresent(AsBlob.class) && m.getReturnType().equals(byte[].class) && adapter.isSupportsBlob())
					{
						returnTypes.add(Blob.class);
					}
					else
					{
						returnTypes.add(m.getReturnType());
						if (adapter.isSupportsClob() && m.isAnnotationPresent(AsClob.class))
						{
							LOGGER.warning("AsClob annotation is present on property " + name + " of class " + NameGenerator.getSystemicName(clazz)
									+ ", but it does not have char[] return type.");
						}
						if (adapter.isSupportsBlob() && m.isAnnotationPresent(AsBlob.class))
						{
							LOGGER.warning("AsBlob annotation is present on property " + name + " of class " + NameGenerator.getSystemicName(clazz)
									+ ", but it does not have byte[] return type.");
						}
					}

					// handle Indexed annotations
					List<String> indexNames = new ArrayList<String>();
					if (m.isAnnotationPresent(Indexed.class))
					{
						indexNames.add(m.getAnnotation(Indexed.class).value());
					}
					if (m.isAnnotationPresent(MultiIndexed.class))
					{
						String[] indices = m.getAnnotation(MultiIndexed.class).value();
						for (String i : indices)
						{
							indexNames.add(i);
						}
					}
					if (indexNames.size() > 0)
					{
						indices.put(name, indexNames);
					}
					
					//handle MaxLength annotations
					if(m.isAnnotationPresent(MaxLength.class))
					{
						Long size = m.getAnnotation(MaxLength.class).value();
						setColumnSize(name, size);
					}
				}
				catch (Exception e)
				{
					// can't recover, use generic catch
					e.printStackTrace();
				}
			}
		}
		if (ObjectTools.isDatabasePrimitive(c))
		{
			props.add(Defaults.VALUE_COL);
			returnTypes.add(c);
			// no need to add setter, primitives are cast from the raw types and
			// are final
			values.add(o);
		}

		// sort out the indices
		buildIndexMap();
	}

	/**
	 * Save this object to the database. All properties will be saved. After
	 * this operation, the getId method will return the id of the database row.
	 * 
	 * Preconditions: The table for saving this object exists.
	 * 
	 * @param cw
	 *            the connection wrapper to use for the database operations.
	 * @param subClassName
	 *            the name of the direct subclass, if any. May be null.
	 * @param realId
	 *            the id of the table row, if any. May be null.
	 * @throws SQLException
	 */
	public void save(ConnectionWrapper cw, String subClassName, Long realId) throws SQLException
	{
		// store a reference to the subclass table entry
		Integer subClassNameId = adapter.getPersist().getClassNameNumberMap().getNumber(cw, subClassName);
		addValueTrio(Defaults.REAL_CLASS_COL, subClassNameId, Integer.class);
		if (realId != null)
		{
			id = realId;
		}
		String stmt = getRowInsertionStatement();
		PreparedStatement ps = cw.prepareStatement(stmt);
		fillRowInsertionStatement(ps, cw);
		Tools.logFine(ps);
		ps.execute();
		ps.close();
		if (!isArray())
		{
			// store the id of the inserted row
			if (realId == null)
			{
				id = adapter.getPersist().getLastId(cw, getTableName());
			}
			// save the protection entries
			protectionStack.save(this.getTableName(), id, cw);
		}
		else
		{
			// add the entries of the array
			if (realId == null)
			{
				id = adapter.getPersist().getLastId(cw,NameGenerator.getArrayTablename(adapter));
			}
			protectionStack.save(NameGenerator.getArrayTablename(adapter), id, cw);
			adapter.getPersist().getArrayEntryWriter().addArrayEntries(cw, id, object, delayBuffer);
		}
		if (delayBuffer != null && object != null)
		{
			delayBuffer.setUndefinedIds(id, System.identityHashCode(object));
		}
	}

	/**
	 * Statement to create the table for this type of object.
	 * 
	 * @return an SQL string that can be used to create the table for this
	 *         object.
	 * @throws SQLException
	 */
	public String getTableCreationStatement(ConnectionWrapper cw) throws SQLException
	{
		StringBuilder statement = new StringBuilder("CREATE TABLE ");
		statement.append(getTableName());
		statement.append(" (");
		ArrayList<String> columnDescriptions = new ArrayList<String>();
		// add the identifier, set it to a primary key if this is the
		// java.lang.Object class or an array
		if (this.getRepresentedClass().equals(Object.class) || this.getRepresentedClass().isArray())
		{
			if (adapter.isSupportsIdentity())
			{
				columnDescriptions.add(Defaults.ID_COL + " " + adapter.getIdentity() + " PRIMARY KEY");
			}
			else
			{
				columnDescriptions.add(Defaults.ID_COL + " " + adapter.getLongTypeKeyword() + " PRIMARY KEY");

			}
		}
		else
		{
			// not the base class (java.lang.Object), so not
			// auto-increment
			columnDescriptions.add(Defaults.ID_COL + " " + adapter.getLongTypeKeyword() + " PRIMARY KEY");
		}

		if (this.clazz.isArray())
		{
			columnDescriptions.add(Defaults.ARRAY_MEMBER_ID + " " + adapter.getReferenceType(Defaults.ARRAY_TABLENAME));
			columnDescriptions.add(Defaults.ARRAY_POSITION + " " + adapter.getIntegerTypeKeyword());
			columnDescriptions.add(Defaults.COMPONENT_CLASS_COL + " " + adapter.getIntegerTypeKeyword());
			columnDescriptions.add(Defaults.VALUE_COL + " " + adapter.getColumnType(clazz.getComponentType(), null));
		}
		else
		{
			columnDescriptions.add(Defaults.REAL_CLASS_COL + " " + adapter.getIntegerTypeKeyword());
			for (int x = 0; x < this.getPropertyCount(); x++)
			{
				String mName = this.getPropertyName(x) + " ";
				Class<?> returnType = getReturnType(x);
				if (this.isReferenceType(x) && !returnType.isEnum() && !returnType.equals(Class.class))
				{
					mName += adapter.getReferenceType(returnType);
				}
				else
				{
					mName += getColumnType(x);
					//special case: Get size of varchars
					Method accessor = getAccessor(x);
					if(accessor !=null && accessor.isAnnotationPresent(MaxLength.class))
					{
						Long size = accessor.getAnnotation(MaxLength.class).value();
						setColumnSize(getPropertyName(x), size);
					}
				}
				columnDescriptions.add(mName);
				// add info to type table
				adapter.getPersist().getTableManager().addTypeInfo(getTableName(), getPropertyName(x), getReturnType(x),getColumnSize(getPropertyName(x)), cw);
			}
		}
		for (int x = 0; x < columnDescriptions.size(); x++)
		{
			statement.append(columnDescriptions.get(x));
			if (x < columnDescriptions.size() - 1)
			{
				statement.append(", ");
			}
		}
		statement.append(")");
		return statement.toString();
	}

	public void ensureContainedTablesExist(ConnectionWrapper cw) throws SQLException, SchemaPermissionException
	{

		// a list of contained classes that must be added
		ArrayList<Class<?>> containedClasses = new ArrayList<Class<?>>();
		if (this.clazz.isArray())
		{
			if (!ObjectTools.isDatabasePrimitive(clazz.getComponentType()))
			{
				// make sure the component type exists
				containedClasses.add(clazz.getComponentType());
			}
		}
		else
		{
			for (int x = 0; x < this.getPropertyCount(); x++)
			{
				if (this.isReferenceType(x))
				{
					Class<?> returnType = returnTypes.get(x);
					containedClasses.add(returnType);
				}
			}
		}
		for (Class<?> c : containedClasses)
		{
			adapter.getPersist().getTableManager().ensureTableExists(c, cw);
		}
	}

	/**
	 * Generate the SQL statement that creates this object.
	 * 
	 * @return an SQL insert statement that can be used to create a
	 *         PreparedStatement used to insert this object.
	 */
	public String getRowInsertionStatement()
	{
		StringBuilder statement = new StringBuilder("INSERT INTO ");
		if (this.clazz.isArray())
		{
			statement.append(Defaults.ARRAY_TABLENAME);
			statement.append(" (");
			statement.append(Defaults.COMPONENT_TABLE_COL);
			statement.append(",");
			statement.append(Defaults.COMPONENT_CLASS_COL);
			statement.append(",");
			statement.append(Defaults.ID_COL);
			statement.append(")VALUES(?,?,?)");
		}
		else
		{
			if (id != null)
			{
				addValueTrio(Defaults.ID_COL, id, Long.class);
			}
			statement.append(getTableName());
			int nonNullCount = this.getNonNullPropertyCount();
			statement.append(" (");
			if (nonNullCount == 0 && !adapter.getAllowsEmptyStatements())
			{
				if (this.getPropertyCount() > 0)
				{
					// insert a null value for the first property
					statement.append(this.getPropertyName(0));
					statement.append(")VALUES(NULL");
				}
			}
			else
			{
				int addedCount = 0;
				for (int x = 0; x < props.size(); x++)
				{
					if (values.get(x) != null)
					{
						statement.append(props.get(x));
						addedCount++;
						if (addedCount < nonNullCount)
						{
							statement.append(", ");
						}
					}
				}
				// add the placeholders for the values
				statement.append(")VALUES(");
				addedCount = 0;
				for (int x = 0; x < values.size(); x++)
				{
					if (values.get(x) != null)
					{
						statement.append("?");
						addedCount++;
						if (addedCount < nonNullCount)
						{
							statement.append(", ");
						}
					}
				}
			}
			statement.append(")");
		}
		return statement.toString();
	}

	/**
	 * Fill in the values in the PreparedStatement, making it ready for
	 * execution.
	 * 
	 * @param ps
	 * @throws SQLException
	 */
	public void fillRowInsertionStatement(PreparedStatement ps, ConnectionWrapper cw) throws SQLException
	{
		if (this.isArray())
		{
			String componentTableName =NameGenerator.getTableName(clazz.getComponentType(), adapter);
			if (clazz.getComponentType().isArray())
			{
				componentTableName = NameGenerator.getArrayTablename(adapter);
			}
			Integer componentTableNameId = adapter.getPersist().getTableNameNumberMap().getNumber(cw, componentTableName);
			ps.setInt(1, componentTableNameId);
			// the table also contains a reference to the actual name of the class
			Integer componentClassNameId = adapter.getPersist().getClassNameNumberMap().getNumber(cw, clazz);
			ps.setInt(2, componentClassNameId);
			ps.setLong(3, id);

		}
		else
		{
			// enter the values for the properties
			int index = 0;
			for (int x = 0; x < values.size(); x++)
			{
				Class<?> c = returnTypes.get(x);
				Object value = values.get(x);
				if (value != null)
				{
					index++;
					// find the type that best describes the object to store
					if(c.equals(Class.class))
					{
						//save the name of the class, let the classloader handle it
						String name = ((Class<?>)value).getName();
						Tools.setParameter(ps, String.class, index, name,adapter);
					}
					else if(c.isEnum())
					{
						String name = ((Enum<?>)value).name();
						Tools.setParameter(ps, String.class, index, name,adapter);
					}
					else if (ObjectTools.isDatabasePrimitive(c))
					{
						Tools.setParameter(ps, c, index, value,adapter);
					} 
					else
					{
						Long id = adapter.getPersist().saveObjectUnprotected(cw, value, delayBuffer);
						if (id == null)
						{
							ps.setNull(index, java.sql.Types.BIGINT);
							// this is a circularly referenced object
							// mark it for later insertion
							delayBuffer.add(getTableName(), getPropertyName(x), value, getReturnType(x), System.identityHashCode(this.object));
						}
						else
						{
							// get the correct id for the representative class
							ps.setLong(index, id);

							// arrays are given as references to the
							// ARRAY_TABLE_NAME row that represents them
							if (value.getClass().isArray())
							{
								Integer tableNameId = adapter.getPersist().getTableNameNumberMap().getNumber(cw, NameGenerator.getArrayTablename(adapter));
								protectionStack.addEntry(new ProtectionEntry(tableNameId, null, id, props.get(x)));
							}
							else
							{
								protectionStack.addEntry(new ProtectionEntry(cw,value.getClass(), id, props.get(x), adapter));
							}
						}
					}
				}
			}
		}
	}

	@Override
	public String getColumnType(String prop)
	{
		int index = props.indexOf(prop);
		return getColumnType(index);
	}
	
	public String getColumnType(int index)
	{
		if (index >= 0)
		{
			return adapter.getColumnType(getReturnType(index), getAccessor(index));
		}
		return null;
	}

}
