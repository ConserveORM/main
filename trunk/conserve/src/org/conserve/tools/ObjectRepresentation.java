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
package org.conserve.tools;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Logger;

import org.conserve.adapter.AdapterBase;
import org.conserve.annotations.AsBlob;
import org.conserve.annotations.AsClob;
import org.conserve.connection.ConnectionWrapper;
import org.conserve.exceptions.SchemaPermissionException;
import org.conserve.tools.protection.ProtectionEntry;
import org.conserve.tools.protection.ProtectionStack;

/**
 * Maintains a map of name-values that represents the object. Think of it as a
 * wrapper for the reference layer.
 * 
 * @author Erik Berglund
 * 
 */
public class ObjectRepresentation implements Iterable<Integer>
{
	private ArrayList<Object> values = new ArrayList<Object>();
	private ArrayList<String> props = new ArrayList<String>();
	private ArrayList<Class<?>> returnTypes = new ArrayList<Class<?>>();
	private ProtectionStack protectionStack;
	private ArrayList<Method> setters = new ArrayList<Method>();
	private ArrayList<Method> getters = new ArrayList<Method>();
	private Class<?> clazz;
	private String tableName;
	private AdapterBase adapter;
	private Object object;
	private Long id;
	private static final Logger LOGGER = Logger.getLogger("org.conserve");
	private String asName;

	private DelayedInsertionBuffer delayBuffer;

	public ObjectRepresentation(AdapterBase adapter, Class<?> c,
			DelayedInsertionBuffer delayBuffer)
	{
		this(adapter, c, null, delayBuffer);
	}

	public ObjectRepresentation(AdapterBase adapter, Class<?> c, Object o,
			DelayedInsertionBuffer delayBuffer)
	{
		this.protectionStack = new ProtectionStack(adapter.getPersist()
				.getProtectionManager());
		this.delayBuffer = delayBuffer;
		this.adapter = adapter;
		this.object = o;
		this.clazz = c;
		Method[] methods = c.getDeclaredMethods();
		if (c.isArray())
		{
			tableName = NameGenerator.getArrayMemberTableName(
					c.getComponentType(), adapter);
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
					Method mutator = ObjectTools.getMutator(c,
							getMutatorName(m), m.getReturnType());
					setters.add(mutator);
					getters.add(m);
					Object value = null;
					if (o != null)
					{
						boolean oldAccessValue = m.isAccessible();
						m.setAccessible(true);
						value = m.invoke(o);
						values.add(value);
						m.setAccessible(oldAccessValue);
					}
					else
					{
						values.add(null);
					}

					if (m.isAnnotationPresent(AsClob.class)
							&& m.getReturnType().equals(char[].class)
							&& adapter.isSupportsClob())
					{
						returnTypes.add(Clob.class);
					}
					else if (m.isAnnotationPresent(AsBlob.class)
							&& m.getReturnType().equals(byte[].class)
							&& adapter.isSupportsBlob())
					{
						returnTypes.add(Blob.class);
					}
					else
					{
						returnTypes.add(m.getReturnType());
						if (adapter.isSupportsClob()
								&& m.isAnnotationPresent(AsClob.class))
						{
							LOGGER.warning("AsClob annotation is present on property "
									+ name
									+ " of class "
									+ ObjectTools.getSystemicName(clazz)
									+ ", but it does not have char[] return type.");
						}
						if (adapter.isSupportsBlob()
								&& m.isAnnotationPresent(AsBlob.class))
						{
							LOGGER.warning("AsBlob annotation is present on property "
									+ name
									+ " of class "
									+ ObjectTools.getSystemicName(clazz)
									+ ", but it does not have byte[] return type.");
						}
					}
				}
				catch (Exception e)
				{
					//can't recover, use generic catch
					e.printStackTrace();
				}
			}
		}
		if (ObjectTools.isPrimitive(c))
		{
			props.add(Defaults.VALUE_COL);
			returnTypes.add(c);
			// no need to add setter, primitives are cast from the raw types and
			// are final
			values.add(o);
		}
	}

	/**
	 * Extract the entry set and save it as a property of this object.
	 */
	void implementMap()
	{
		props.add(Defaults.MAP_PROPERTY_COL);
		returnTypes.add(Object[].class);
		// add a dummy mutator
		setters.add(null);
		getters.add(null);
		if (this.object != null)
		{
			// this should work:
			// Set<?> entrySet =((Map<?,?>)object).entrySet();
			// values.add(entrySet.toArray());
			// unfortunately, it does not, as it causes problems later when we
			// try to access the contents of the Set.
			// This is caused by bug 4071957:
			// http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4071957
			// therefore we have to map the inner class object onto an ordinary
			// class object:
			Set<?> entrySet = ((Map<?, ?>) object).entrySet();
			ArrayList<MapEntry> ordinarySet = new ArrayList<MapEntry>();
			for (Object o : entrySet)
			{
				Map.Entry<?, ?> entry = (Entry<?, ?>) o;
				MapEntry nuEntry = new MapEntry();
				nuEntry.setKey(entry.getKey());
				nuEntry.setValue(entry.getValue());
				ordinarySet.add(nuEntry);
			}
			values.add(ordinarySet.toArray());
		}
	}

	/**
	 * Extract the contents of this Collection and save it as a property of this
	 * object.
	 * 
	 */
	void implementCollection()
	{
		props.add(Defaults.COLLECTION_PROPERTY_COL);
		returnTypes.add(Object[].class);
		// add a dummy mutator
		setters.add(null);
		getters.add(null);
		if (this.object != null)
		{
			Object[] contents = ((Collection<?>) object).toArray();
			values.add(contents);
		}
	}

	/**
	 * Check if this class implements interface interf or any of its
	 * sub-interfaces.
	 * 
	 * @return true if this object directly implements interf or a
	 *         sub-interface.
	 */
	boolean isImplementation(Class<?> interf)
	{
		return ObjectTools.implementsInterface(clazz, interf);
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
	 * @param subClassId
	 *            the id of the subclass table row, if any. May be null.
	 * @throws SQLException
	 * @throws IOException
	 */
	public void save(ConnectionWrapper cw, String subClassName, Long subClassId)
			throws SQLException
	{
		if (subClassName != null && subClassId != null)
		{
			// store a reference to the subclass table entry
			addValuePair(Defaults.REAL_CLASS_COL, subClassName);
			addValuePair(Defaults.REAL_ID_COL, subClassId);
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
			id = adapter.getPersist().getLastId(cw, getTableName());
			// save the protection entries
			protectionStack.save(this.getTableName(), id, cw);
		}
		else
		{
			// add the entries of the array
			id = adapter.getPersist().getLastId(cw, Defaults.ARRAY_TABLE_NAME);
			protectionStack.save(Defaults.ARRAY_TABLE_NAME, id, cw);
			adapter.getPersist().getArrayEntryWriter()
					.addArrayEntries(cw, id, object, delayBuffer);
		}
		if(delayBuffer!=null && object!=null)
		{
			delayBuffer.setUndefinedIds(id,System.identityHashCode(object));
		}
	}

	public Object getObject()
	{
		return object;
	}

	public void setId(Long nuId)
	{
		this.id = nuId;
	}

	public Long getId()
	{
		return id;
	}

	public String getPropertyName(int index)
	{
		return props.get(index);
	}

	public Object getPropertyValue(int index)
	{
		return values.get(index);
	}

	/**
	 * Set the new property of the object. The new value is copied both into he
	 * object itself and the property map.
	 * 
	 * @param index
	 * @param nuProperty
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 */
	public void setPropertyValue(int index, Object nuProperty)
			throws IllegalArgumentException, IllegalAccessException,
			InvocationTargetException
	{
		values.set(index, nuProperty);
		// Get the mutator
		Method mutator = getMutator(index);
		if (mutator != null)
		{
			mutator.invoke(this.object, nuProperty);
		}
	}

	public Class<?> getReturnType(int index)
	{
		return returnTypes.get(index);
	}

	public Method getMutator(int index)
	{
		return this.setters.get(index);
	}

	/**
	 * @param x
	 * @return
	 */
	private Method getAccessor(int x)
	{
		if (x >= this.getters.size())
		{
			return null;
		}
		return this.getters.get(x);
	}

	public Object getValue(String name)
	{
		int index = props.indexOf(name);
		if (index >= 0)
		{
			return values.get(index);
		}
		return null;
	}

	public Class<?> getReturnType(String name)
	{
		int index = props.indexOf(name);
		if (index >= 0)
		{
			return returnTypes.get(index);
		}
		return null;
	}

	public int getPropertyCount()
	{
		return this.props.size();
	}

	public boolean isReferenceType(int index)
	{
		Class<?> c = getReturnType(index);
		return !ObjectTools.isPrimitive(c);

	}

	public String getTableName()
	{
		return tableName;
	}

	/**
	 * Get the mutator that is associated with a given accessor.
	 * 
	 * @param m
	 *            an accessor.
	 * @return
	 */
	private String getMutatorName(Method m)
	{
		String name = m.getName();
		if (name.startsWith("get"))
		{
			return "set" + name.substring(3);
		}
		else if (name.startsWith("is"))
		{
			return "set" + name.substring(2);
		}
		else
		{
			return null;// not valid, should not happen
		}
	}

	/**
	 * Statement to create the table for this type of object.
	 * 
	 * @return an SQL string that can be used to create the table for this
	 *         object.
	 * @throws SQLException
	 */
	public String getTableCreationStatement(ConnectionWrapper cw)
			throws SQLException
	{
		StringBuilder statement = new StringBuilder("CREATE TABLE ");
		statement.append(getTableName());
		statement.append(" (");
		ArrayList<String> columnDescriptions = new ArrayList<String>();
		// add the identifier, set it to a primary key
		if (adapter.isSupportsIdentity())
		{
			columnDescriptions.add(Defaults.ID_COL + " "
					+ adapter.getIdentity() + " PRIMARY KEY");
		}
		else
		{
			columnDescriptions.add(Defaults.ID_COL + " "
					+ adapter.getLongTypeKeyword() + " PRIMARY KEY");

		}

		if (this.clazz.isArray())
		{
			columnDescriptions.add(Defaults.ARRAY_MEMBER_ID + " "
					+ adapter.getReferenceType(Defaults.ARRAY_TABLE_NAME));
			columnDescriptions.add(Defaults.ARRAY_POSITION + " int ");
			columnDescriptions.add(Defaults.COMPONENT_CLASS_COL +" "
					+ adapter.getVarCharIndexed());
			columnDescriptions.add(Defaults.VALUE_COL + " "
					+ adapter.getColumnType(clazz.getComponentType(), null));
		}
		else
		{
			columnDescriptions.add(Defaults.REAL_CLASS_COL + " "
					+ adapter.getVarCharIndexed());
			columnDescriptions.add(Defaults.REAL_ID_COL + " bigint");
			for (int x = 0; x < this.getPropertyCount(); x++)
			{
				String mName = this.getPropertyName(x) + " ";
				if (this.isReferenceType(x))
				{
					Class<?> returnType = returnTypes.get(x);
					if (returnType.isInterface())
					{
						returnType = Object.class;
					}
					mName += adapter.getReferenceType(returnType);
				}
				else
				{
					mName += adapter.getColumnType(getReturnType(x),
							getAccessor(x));
				}
				columnDescriptions.add(mName);
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

	public void ensureContainedTablesExist(ConnectionWrapper cw)
			throws SQLException, SchemaPermissionException
	{

		// a list of contained classes that must be added
		ArrayList<Class<?>> containedClasses = new ArrayList<Class<?>>();
		if (this.clazz.isArray())
		{
			if (!ObjectTools.isPrimitive(clazz.getComponentType()))
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
					if (returnType.isInterface())
					{
						returnType = Object.class;
					}
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
			statement.append(Defaults.ARRAY_TABLE_NAME);
			statement.append(" (");
			statement.append(Defaults.COMPONENT_TABLE_COL);
			statement.append(",");
			statement.append(Defaults.COMPONENT_CLASS_COL);
			statement.append(")VALUES(?,?)");
		}
		else
		{
			statement.append(getTableName());
			int nonNullCount = this.getNonNullPropertyCount();
			statement.append(" (");
			if (nonNullCount == 0 && !adapter.allowsEmptyStatements())
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
	 * @throws IOException
	 */
	public void fillRowInsertionStatement(PreparedStatement ps,
			ConnectionWrapper cw) throws SQLException
	{
		if (this.isArray())
		{
			ps.setString(1, NameGenerator.getTableName(
					clazz.getComponentType(), adapter));
			if (clazz.getComponentType().isArray())
			{
				ps.setString(1, Defaults.ARRAY_TABLE_NAME);
			}
			// the table also contains the actual name of the class
			ps.setString(2, ObjectTools.getSystemicName(clazz));

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
					if (ObjectTools.isPrimitive(c))
					{
						Tools.setParameter(ps, c, index, value);
					}
					else
					{
						Long id = adapter.getPersist().saveObjectUnprotected(
								cw, value, delayBuffer);
						if (id == null)
						{
							ps.setNull(index, java.sql.Types.BIGINT);
							// this is a circularly referenced object
							// mark it for later insertion
							delayBuffer.add(getTableName(), getPropertyName(x),
									value, getReturnType(x),System.identityHashCode(this.object));
						}
						else
						{
							// get the correct id for the representative class
							if (!c.equals(value.getClass()))
							{
								Class<?> tempClass = c;
								if (c.isInterface())
								{
									// since we can't cast to an interface, just
									// cast to java.lang.Object.
									tempClass = Object.class;
								}
								long nuId = adapter.getPersist().getCastId(
										tempClass, value.getClass(), id, cw);
								ps.setLong(index, nuId);
							}
							else
							{
								ps.setLong(index, id);
							}
							// arrays are given as references to the
							// ARRAY_TABLE_NAME row that represents them
							if (value.getClass().isArray())
							{
								protectionStack.addEntry(new ProtectionEntry(
										Defaults.ARRAY_TABLE_NAME,null, id, props.get(x)));
							}
							else
							{
								protectionStack.addEntry(new ProtectionEntry(
										value.getClass(), id, props.get(x),
										adapter));
							}
						}
					}
				}
			}
		}
	}

	/**
	 * Add a value as if it was a bona-fide property of the represented object.
	 * 
	 * @param column
	 * @param value
	 */
	public void addValuePair(String column, Object value)
	{
		addValuePair(column, value, value.getClass());
	}

	/**
	 * Add a value as if it was a bona-fide property of the represented object.
	 * 
	 * @param column
	 * @param value
	 * @param clazz
	 *            the class of value
	 */
	public void addValuePair(String column, Object value, Class<?> clazz)
	{
		this.props.add(column);
		this.values.add(value);
		this.returnTypes.add(clazz);
	}

	public Class<?> getRepresentedClass()
	{
		return clazz;
	}

	public Class<?> getSuperclass()
	{
		return clazz.getSuperclass();
	}

	/**
	 * Check if the represented object is primitive. Primitive objects are
	 * strings, primitives (int, Integer, etc) and Enums.
	 * 
	 * @return true if the represented class/object is an array, false
	 *         otherwise.
	 */
	public boolean isPrimitive()
	{
		return ObjectTools.isPrimitive(clazz);
	}

	/**
	 * Check if the property at index is primitive or not.
	 * 
	 * @param index
	 * @return true if the property at index is primitive.
	 */
	public boolean isPrimitive(int index)
	{
		return ObjectTools.isPrimitive(getReturnType(index));
	}

	/**
	 * Check if the named property is primitive or not.
	 * 
	 * @param name
	 *            the name of the property
	 * @return true if the property at index is primitive.
	 */
	public boolean isPrimitive(String name)
	{
		return ObjectTools.isPrimitive(getReturnType(name));
	}

	/**
	 * Check if the represented object is an array.
	 * 
	 * @return true if the represented class/object is an array.
	 */
	public boolean isArray()
	{
		return clazz.isArray();
	}

	public int getNonNullPropertyCount()
	{
		int res = 0;
		for (Object o : values)
		{
			if (o != null)
			{
				res++;
			}
		}
		return res;
	}

	/**
	 * Return an iterator to all the non-null property indices.
	 * 
	 * @return an iterator of Integers.
	 */
	@Override
	public Iterator<Integer> iterator()
	{
		ArrayList<Integer> nonNullProperties = new ArrayList<Integer>();
		for (int x = 0; x < this.values.size(); x++)
		{
			if (values.get(x) != null)
			{
				nonNullProperties.add(x);
			}
		}
		return nonNullProperties.iterator();
	}

	/**
	 * Check if this representation has a property with the given name.
	 * 
	 * @param name
	 * @return true if the property exists in this representation, false
	 *         otherwise.
	 */
	public boolean hasProperty(String name)
	{
		return props.indexOf(name) >= 0;
	}

	/**
	 * Remove the property with the given name.
	 * 
	 * @param p
	 */
	public void removeProperty(int p)
	{
		props.remove(p);
		values.remove(p);
		returnTypes.remove(p);
	}

	/**
	 * Set the name used by this representation in TABLENAME AS NAME statements.
	 * 
	 * @param asName
	 *            AS name.
	 */
	public void setAsName(String asName)
	{
		this.asName = asName;
	}

	public String getAsName()
	{
		return this.asName;
	}

	public DelayedInsertionBuffer getDelayedInsertionBuffer()
	{
		return this.delayBuffer;
	}

	/**
	 * Objects only have the C__DUMMY column if they have no other columns.
	 * 
	 * @return true if there are columns other than 'C__DUMMY'.
	 */
	public boolean hasNonDummyProperty()
	{
		if (this.props.size() == 1)
		{
			if (this.props.get(0).equals(Defaults.DUMMY_COL_NAME))
			{
				return false;
			}
		}
		return true;
	}

	/**
	 * @return
	 */
	public String getSystemicName()
	{
		return ObjectTools.getSystemicName(getRepresentedClass());
	}

}
