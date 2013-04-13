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
package org.conserve.tools.metadata;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Logger;

import org.conserve.adapter.AdapterBase;
import org.conserve.tools.Defaults;
import org.conserve.tools.DelayedInsertionBuffer;
import org.conserve.tools.ObjectTools;
import org.conserve.tools.protection.ProtectionStack;

/**
 * Maintains a map of name-values that represents the object. Think of it as a
 * wrapper for the reference layer.
 * 
 * @author Erik Berglund
 * 
 */
public abstract class ObjectRepresentation implements Iterable<Integer>
{
	protected ArrayList<Object> values = new ArrayList<Object>();
	protected ArrayList<String> props = new ArrayList<String>();
	protected ArrayList<Class<?>> returnTypes = new ArrayList<Class<?>>();
	protected ProtectionStack protectionStack;
	protected ArrayList<Method> setters = new ArrayList<Method>();
	protected ArrayList<Method> getters = new ArrayList<Method>();
	protected Class<?> clazz;
	protected String tableName;
	protected AdapterBase adapter;
	protected Object object;
	protected Long id;
	protected static final Logger LOGGER = Logger.getLogger("org.conserve");
	protected String asName;

	protected DelayedInsertionBuffer delayBuffer;

	/**
	 * Default constructor for subclassing.
	 */
	protected ObjectRepresentation()
	{
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
	public void setPropertyValue(int index, Object nuProperty) throws IllegalArgumentException, IllegalAccessException,
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
	 * Get the change in the database model from this object to the
	 * toRepresentation object. If no change is detected, null is returned.
	 * 
	 * @throws MetadataException
	 *             if there is more than one change.
	 * 
	 * @param toRepresentation
	 * 
	 * @return a description of the change.
	 */
	public ChangeDescription getDifference(ObjectRepresentation toRepresentation) throws MetadataException
	{
		ChangeDescription res = null;
		if (!hasSameProperties(toRepresentation))
		{
			// check for deletions
			if (getPropertyCount() > toRepresentation.getPropertyCount())
			{
				// check error condition
				if (getPropertyCount() > toRepresentation.getPropertyCount() + 1)
				{
					throw new MetadataException("Removing more than one column.");
				}
				res = new ChangeDescription();
				// find the column not in toRepresentation
				for (int x = 0; x < props.size(); x++)
				{
					if (!toRepresentation.props.contains(props.get(x)))
					{
						res.setFromName(props.get(x));
						res.setFromClass(returnTypes.get(x));
					}
				}
			}
			// check for insertions
			else if (getPropertyCount() < toRepresentation.getPropertyCount())
			{
				if (getPropertyCount() < toRepresentation.getPropertyCount() - 1)
				{
					throw new MetadataException("Adding more than one column.");
				}
				res = new ChangeDescription();
				// find the column not in this representation
				for (int x = 0; x < toRepresentation.props.size(); x++)
				{
					if (!props.contains(toRepresentation.props.get(x)))
					{
						res.setToName(toRepresentation.props.get(x));
						res.setToClass(toRepresentation.returnTypes.get(x));
					}
				}
			}
			else
			{
				// the number of columns are the same
				// check if any column has been renamed
				List<String> fromNames = new ArrayList<String>(props);
				List<String> toNames = new ArrayList<String>(toRepresentation.props);
				for (int x = 0; x < fromNames.size(); x++)
				{
					String t = fromNames.get(x);
					int toIndex = toNames.indexOf(t);
					if (toIndex >= 0)
					{
						// make sure the two properties are still the same type
						Class<?> fromClass = getReturnType(fromNames.get(x));
						Class<?> toClass = toRepresentation.getReturnType(toNames.get(toIndex));
						if (!fromClass.equals(toClass))
						{
							// found same name, different return types
							res = new ChangeDescription();
							// same to/from name
							res.setFromName(fromNames.get(x));
							res.setToName(res.getFromName());
							res.setFromClass(fromClass);
							res.setToClass(toClass);
							break;
						}
						fromNames.remove(x);
						toNames.remove(toIndex);
						x--;
					}
				}
				// there are only two unmatched names, one in each list, and
				// there is no type change
				if (res == null && fromNames.size() == 1 && toNames.size() == 1)
				{
					// no type change found, must be a name change
					// check that the return type is unchanged
					Class<?> fromClass = this.getReturnType(fromNames.get(0));
					Class<?> toClass = toRepresentation.getReturnType(toNames.get(0));
					if (fromClass.equals(toClass))
					{

						res = new ChangeDescription();
						res.setFromName(fromNames.get(0));
						res.setToName(toNames.get(0));
						res.setFromClass(fromClass);
						res.setToClass(toClass);
					}
					else
					{
						throw new MetadataException("Chaning both name and type.");
					}
				}
			}

			if (res != null)
			{
				// check that the remaining columns are unchanged
				for (int x = 0; x < props.size(); x++)
				{
					String p = props.get(x);
					if (!p.equals(res.getFromName()))
					{
						int i = toRepresentation.props.indexOf(p);
						if (i < 0)
						{
							// could not find matching column
							throw new MetadataException("Changing more than one column.");
						}
						else
						{
							if (!returnTypes.get(x).equals(toRepresentation.returnTypes.get(i)))
							{
								// the return type is not the same
								throw new MetadataException("Changing more than one column.");
							}
						}
					}
				}
			}
			else
			{
				// we did not find the changes, but we know they are there
				throw new MetadataException("Changes detected but not identified.");
			}
		}

		return res;
	}

	private boolean hasSameProperties(ObjectRepresentation other)
	{
		// different size
		if (props.size() != other.props.size())
		{
			return false;
		}
		for (int x = 0; x < props.size(); x++)
		{
			String fromProp = props.get(x);
			int toIndex = other.props.indexOf(fromProp);
			if (toIndex < 0)
			{
				// other object does not have property
				return false;
			}
			// check that the return type is the same
			if (!returnTypes.get(x).equals(other.returnTypes.get(toIndex)))
			{
				// return types differ
				return false;
			}
		}
		// all properties and return types match
		return true;
	}

	/**
	 * @param x
	 * @return
	 */
	protected Method getAccessor(int x)
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
		return !ObjectTools.isDatabasePrimitive(c);

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
	protected String getMutatorName(Method m)
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
	 * Add a value as if it was a bona-fide property of the represented object.
	 * 
	 * @param column
	 * @param value
	 */
	public void addValuePair(String column, Object value)
	{
		addValueTrio(column, value, value.getClass());
	}

	/**
	 * @param column
	 * @param clazz
	 */
	public void addNamedType(String column, Class<?> clazz)
	{
		addValueTrio(column, null, clazz);
	}

	/**
	 * Add a value as if it was a bona-fide property of the represented object.
	 * 
	 * @param column
	 * @param value
	 * @param clazz
	 *            the class of value
	 */
	protected void addValueTrio(String column, Object value, Class<?> clazz)
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
		return ObjectTools.isDatabasePrimitive(clazz);
	}

	/**
	 * Check if the property at index is primitive or not.
	 * 
	 * @param index
	 * @return true if the property at index is primitive.
	 */
	public boolean isPrimitive(int index)
	{
		return ObjectTools.isDatabasePrimitive(getReturnType(index));
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
		return ObjectTools.isDatabasePrimitive(getReturnType(name));
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
	 * @param name
	 *            the name of the property to remove.
	 */
	public void removeProperty(String name)
	{
		int p = props.indexOf(name);
		if (p >= 0)
		{
			removeProperty(p);
		}
	}

	/**
	 * Remove the property at the given index.
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
