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
package com.github.conserveorm.tools;

import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.CharBuffer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.github.conserveorm.adapter.AdapterBase;
import com.github.conserveorm.cache.ObjectRowMap;
import com.github.conserveorm.connection.ConnectionWrapper;
import com.github.conserveorm.tools.metadata.MapEntry;
import com.github.conserveorm.tools.metadata.ObjectRepresentation;
import com.github.conserveorm.tools.metadata.ObjectStack;

/**
 * Generates and instantiates objects based on property-value pairs.
 * 
 * @author Erik Berglund
 * 
 */
public class ObjectFactory
{

	/**
	 * 
	 * Create an object from a resultset row.
	 * 
	 * @param <T>
	 * 
	 * @param map
	 *            a hashmap of the values, indexed by name
	 * @return a new object of the appropriate type.
	 * @throws SQLException
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T> T createObject(AdapterBase adapter,ObjectRowMap cache, HashMap<String, Object> map, Class<T> resultClass,ConnectionWrapper cw,String tableName, Long dbId)
			throws SQLException
	{
		try
		{
			if (ObjectTools.isDatabasePrimitive(resultClass))
			{
				if(Number.class.isAssignableFrom(resultClass))
				{
					//safe cast of numbers
					return (T) ObjectTools.cast(resultClass, (Number)map.get(Defaults.VALUE_COL));
				}
				else if(resultClass.isEnum())
				{
					return (T) Enum.valueOf((Class<Enum>)resultClass, (String)map.get(Defaults.VALUE_COL));
				}
				else
				{					
					return resultClass.cast(map.get(Defaults.VALUE_COL));
				}
			}
			else
			{
				// create a new object using the default constructor.
				Constructor<T> constructor = resultClass.getDeclaredConstructor();
				boolean wasAccessible = constructor.isAccessible();
				if(!wasAccessible)
				{
					//if the constructor is private, make it accessible
					constructor.setAccessible(true);
				}
				T res = constructor.newInstance();
				if(!wasAccessible)
				{
					constructor.setAccessible(wasAccessible);
				}
				// add object to cache
				cache.storeObject(tableName, res, dbId);
				fillObjectValues(adapter,cache, res, resultClass, map,cw);
				return res;
			}
		}
		catch (Exception e)
		{
			throw new SQLException(e);
		}
	}


	@SuppressWarnings("unchecked")
	private static <T> void fillObjectValues(AdapterBase adapter,ObjectRowMap cache, T res, Class<T> clazz, HashMap<String, Object> map,ConnectionWrapper cw)
			throws IllegalArgumentException, IllegalAccessException, InvocationTargetException, ClassNotFoundException,
			SQLException, InstantiationException, IOException
	{
		// get an object representation stack
		ObjectStack objStack = new ObjectStack(adapter, clazz);
		// iterate over all the representations in the stack
		for(ObjectRepresentation rep:objStack.getAllRepresentations())
		{
			for (int x = 0; x < rep.getPropertyCount(); x++)
			{
				String name = rep.getPropertyName(x);
				Method m = rep.getMutator(x);
				Object o = map.get(name);
				if (o != null)
				{
					if (m == null)
					{
						if (name.equals(Defaults.MAP_PROPERTY_COL))
						{
							// this is a map, so load the property into a
							// separate variable and process it
							Object object = adapter.getPersist().getObject(cw, rep.getReturnType(x),
									((Number) o).longValue(), cache);
							// cast the object to an object array
							Object[] array = (Object[]) object;
							Map<Object, Object> resultMap = (Map<Object, Object>) res;
							for (int y = 0; y < array.length; y++)
							{
								MapEntry entry = (MapEntry) array[y];
								resultMap.put(entry.getKey(), entry.getValue());
							}
						}
						else if (name.equals(Defaults.COLLECTION_PROPERTY_COL))
						{
							// this is a collection, so load the property into a
							// separate variable
							Object object = adapter.getPersist().getObject(cw, rep.getReturnType(x),
									((Number) o).longValue(), cache);
							// process the contents
							// cast the object to an object array
							Object[] array = (Object[]) object;
							Collection<Object> collection = (Collection<Object>) res;
							for (int y = 0; y < array.length; y++)
							{
								collection.add(array[y]);
							}
						}
						else
						{
							// there is no mutator for this property, and that's
							// ok - it's a derived property.
						}
					}
					else
					{
						boolean wasAccessible = m.isAccessible();
						if(!wasAccessible)
						{
							m.setAccessible(true);
						}
						// this is neither a map or collection content variable,
						// so process it as usual.
						if(rep.getReturnType(x).isEnum())
						{
							String enumName = (String)o;
							Class<? extends Enum<?>> enClass = (Class<? extends Enum<?>>) rep.getReturnType(x);
							Enum<?>[] enConsts =enClass.getEnumConstants();
							for(int t = 0;t<enConsts.length;t++)
							{
								if(enConsts[t].name().equals(enumName))
								{
									m.invoke(res, enConsts[t]);
									break;
								}
							}
						}
						else if(rep.getReturnType(x).equals(Class.class))
						{
							//classes are stored as strings and loaded by the classloader
							String className = (String)o;
							Class<?> value = ObjectFactory.class.getClassLoader().loadClass(className);
							m.invoke(res, value);
						}
						else if (rep.isPrimitive(x))
						{
							if (o instanceof Number)
							{
								m.invoke(res, ObjectTools.cast((Class<? extends Number>) m.getParameterTypes()[0],
										(Number) o));
							}
							else if (o instanceof Clob)
							{
								Clob clob = (Clob) o;
								Reader r = clob.getCharacterStream();
								CharBuffer cb = CharBuffer.allocate((int) clob.length());
								r.read(cb);
								r.close();
								m.invoke(res, cb.array());
							}
							else if (o instanceof Blob)
							{
								Blob b = (Blob) o;
								m.invoke(res, b.getBytes(1, (int) b.length()));
							}
							else
							{
								m.invoke(res, o);
							}
						}
						else
						{
							// get the referenced object
							// get the id
							Long dbId = ((Number) o).longValue();
							Object object = adapter.getPersist().getObject(cw, rep.getReturnType(x), dbId, cache);
							if (object != null)
							{
								// save the retrieved
								m.invoke(res, object);
							}
						}
						// set the old accessibility
						m.setAccessible(wasAccessible);
					}
				}
			}
		}
	}
}
