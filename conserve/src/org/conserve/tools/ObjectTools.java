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
package org.conserve.tools;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Clob;
import java.sql.Blob;
import java.util.ArrayList;
import java.util.List;

import org.conserve.adapter.AdapterBase;
import org.conserve.annotations.Transient;

/**
 * Tools for extracting information about objects.
 * 
 * @author Erik Berglund
 * 
 */
public class ObjectTools
{
	private static Class<?>[] classes = new Class<?>[] { long.class, int.class, short.class, char.class, byte.class,
			boolean.class, double.class, float.class, String.class };

	/**
	 * Get all interfaces of c. If c is an interface, get all super-interfaces.
	 * Ignores superclasses of c.
	 * 
	 * @param c
	 * @return all interfaces that are implemented by c.
	 */
	public static List<Class<?>> getAllInterfaces(Class<?> c)
	{
		ArrayList<Class<?>> res = new ArrayList<Class<?>>();
		if (c != null)
		{
			if (c.isInterface())
			{
				res.add(c);
			}
			Class<?>[] interfaces = c.getInterfaces();
			for (Class<?> intf : interfaces)
			{
				res.addAll(getAllInterfaces(intf));
			}
		}
		return res;
	}
	
	/**
	 * Get a list of all direct interfaces of c. 
	 * If c is an interface, it is not included. Ignores superclasses of c. 
	 * Ignores super-interfaces.
	 * 
	 * @param c
	 */
	public static List<Class<?>>getAllDirectInterfaces(Class<?>c)
	{
		ArrayList<Class<?>> res = new ArrayList<Class<?>>();
		if (c != null)
		{
			Class<?>[] interfaces = c.getInterfaces();
			for (Class<?> intf : interfaces)
			{
				res.add(intf);
			}
		}
		return res;
	}

	/**
	 * Get all interfaces of c. If c is an interface, get all super-interfaces.
	 * Includes superclasses of c.
	 * 
	 * @param c
	 * @return all interfaces that are implemented by c or one of its
	 *         superclasses.
	 */
	public static List<Class<?>> getAllInterfacesIncludingSuper(Class<?> c)
	{
		ArrayList<Class<?>> res = new ArrayList<Class<?>>();
		while (c != null)
		{
			res.addAll(getAllInterfaces(c));
			c = c.getSuperclass();
		}
		return res;
	}
	
	/**
	 * Get all types that can legally be used to reference an object of c.
	 * This includes all superclasses of c, and all interfaces implemented by c or any of its superclasses.
	 * The class c itself is not included in the result.
	 * Each class is only present once.
	 * 
	 * @param c the class to get legal refernece types for.S
	 */
	public static List<Class<?>> getAllLegalReferenceTypes(Class<?>c)
	{
		ArrayList<Class<?>> res = new ArrayList<Class<?>>();
		while (c != null)
		{
			List<Class<?>>tmp = getAllInterfaces(c);
			for(Class<?>t:tmp)
			{
				//only add interfaces we haven't added already.
				if(!res.contains(t))
				{
					res.add(t);
				}
			}
			c = c.getSuperclass();
			if(c != null && !res.contains(c))
			{
				res.add(c);
			}
		}
		return res;
	}

	/**
	 * Returns true if implementor is equal to interf or implementor implements
	 * interf.
	 * 
	 * @param implementor the class that may or may not be interf or one of its implementations.
	 * @param interf the interface.
	 * @return true if interf is one of the interfaces of implementor.
	 */
	public static boolean implementsInterface(Class<?> implementor, Class<?> interf)
	{
		List<Class<?>> interfaces = getAllInterfaces(implementor);
		for (Class<?> intf : interfaces)
		{
			if (intf.equals(interf))
			{
				return true;
			}
		}
		return false;
	}

	/**
	 * Returns true if implementor is equal to interf, implementor implements
	 * interf, or one of implementor's superclasses implements interf.
	 * 
	 * @param implementor
	 * @param interf
	 * @return true if interf is one of the interfaces of implementor or its
	 *         superclasses.
	 */
	public static boolean implementsInterfaceIncludingSuper(Class<?> implementor, Class<?> interf)
	{
		List<Class<?>> interfaces = getAllInterfacesIncludingSuper(implementor);
		for (Class<?> intf : interfaces)
		{
			if (intf.equals(interf))
			{
				return true;
			}
		}
		return false;
	}

	/**
	 * Checks if subClass is a subClass (directly or indirectly) of
	 * possibleSuperClass. This also returns true if subClass is equal to
	 * possibleSuperClass.
	 * 
	 * @param subClass
	 * @param possibleSuperClass
	 * @return true if the two classes is in a class-superclass relationship.
	 */
	public static boolean isSubClassOf(Class<?> subClass, Class<?> possibleSuperClass)
	{
		Class<?> c = subClass;
		while (c != null)
		{
			if (c.equals(possibleSuperClass))
			{
				return true;
			}
			c = c.getSuperclass();
		}
		return false;
	}

	/**
	 * Returns true if sub is equal to sup, if sub is a subclass of sup -
	 * including indirectly, or if sub or any of its superclasses implement sup.
	 * 
	 * @param sub
	 *            the implementor, subclass or interface.
	 * @param sup
	 *            the superclass or interface,
	 * @return true if sub is_a sup (possibly indirectly), false otherwise.
	 */
	public static boolean isA(Class<?> sub, Class<?> sup)
	{
		boolean res = false;
		if (isSubClassOf(sub, sup))
		{
			res = true;
		}
		else if (sup.isInterface() && implementsInterface(sub, sup))
		{
			res = true;
		}
		return res;
	}

	/**
	 * Determine if a class is a 'database primitive'. A class is a database
	 * primitive if its Class.isPrimitive method returns true, if it's an object
	 * representation of a primitive class (e.g. Double, Integer), or if it is
	 * String, Enum, Class, or any of the java.sql.Time,Timestapm, or Date classes.
	 * 
	 * In short, a database primitive is any class that can be fully represented
	 * in one column of an SQL database.
	 * 
	 * @param c
	 * @return true if the parameter represents a 'database primitive' class.
	 */
	public static boolean isDatabasePrimitive(Class<?> c)
	{
		if (c.isPrimitive())
		{
			return true;
		}
		else if (c.equals(String.class))
		{
			return true;
		}
		else if (c.isEnum())
		{
			return true;
		}
		else if (c.equals(Class.class))
		{
			return true;
		}
		else if (c.equals(Boolean.class))
		{
			return true;
		}
		else if (c.getSuperclass() != null && c.getSuperclass().equals(Number.class))
		{
			return true;
		}
		else if (c.equals(Character.class))
		{
			return true;
		}
		else if (c.equals(java.sql.Date.class))
		{
			return true;
		}
		else if (c.equals(java.sql.Time.class))
		{
			return true;
		}
		else if (c.equals(java.sql.Timestamp.class))
		{
			return true;
		}
		else if (c.equals(Clob.class))
		{
			return true;
		}
		else if (c.equals(Blob.class))
		{
			return true;
		}
		return false;
	}

	/**
	 * Get the class for a given name. Unlike ClassLoader.loadClass(...), this
	 * also gives primitive classes.
	 * 
	 * @param name
	 *            the canonical name of a class.
	 * @return the Class corresponding to the name.
	 * @throws ClassNotFoundException
	 */
	public static Class<?> lookUpClass(String name, AdapterBase adapter) throws ClassNotFoundException
	{
		Class<?> res = null;
		if (name.contains(".") || Character.isUpperCase(name.charAt(0)))
		{
			try
			{
				res = ClassLoader.getSystemClassLoader().loadClass(name);
			}
			catch (ClassNotFoundException e)
			{
				// this means it's not a valid class name
				// try the other ways of finding the class
			}
		}

		if (res == null)
		{
			if (name.endsWith("[]"))
			{
				String subName = name.substring(0, name.length() - 2);
				Class<?> type = lookUpClass(subName, adapter);
				// create a new object, get the class
				res = Array.newInstance(type, 0).getClass();
			}
			else if ("boolean".equals(name))
			{
				res = boolean.class;
			}
			else if ("byte".equals(name))
			{
				res = byte.class;
			}
			else if ("short".equals(name))
			{
				res = short.class;
			}
			else if ("char".equals(name))
			{
				res = char.class;
			}
			else if ("int".equals(name))
			{
				res = int.class;
			}
			else if ("long".equals(name))
			{
				res = long.class;
			}
			else if ("float".equals(name))
			{
				res = float.class;
			}
			else if ("double".equals(name))
			{
				res = double.class;
			}
			// database-returned name, see if the adapter can help us.
			else 
			{
				for (Class<?> c : classes)
				{
					if (adapter.getColumnType(c, null).equalsIgnoreCase(name))
					{
						res = c;
						break;
					}
				}
			}
		}
		if (res == null)
		{
			throw new ClassNotFoundException("Don't know how to handle class " + name);
		}
		return res;
	}

	/**
	 * Cast an object that extends Number to the desired class which also
	 * extends number. This is to get around the tendency some database engines
	 * have to cast everything to ints.
	 * 
	 * @param clazz
	 * @param o
	 * @return a primitive wrapped in an
	 */
	public static Object cast(Class<?> clazz, Number o)
	{
		if (clazz.equals(o.getClass()))
		{
			// no need for a cast, just return the value.
			return o;
		}
		if (clazz.equals(Double.class) || clazz.equals(double.class))
		{
			return o.doubleValue();
		}
		else if (clazz.equals(Long.class) || clazz.equals(long.class))
		{
			return o.longValue();
		}
		else if (clazz.equals(Integer.class) || clazz.equals(int.class))
		{
			return o.intValue();
		}
		else if (clazz.equals(Float.class) || clazz.equals(float.class))
		{
			return o.floatValue();
		}
		else if (clazz.equals(Short.class) || clazz.equals(short.class))
		{
			return o.shortValue();
		}
		else if (clazz.equals(Byte.class) || clazz.equals(byte.class))
		{
			return o.byteValue();
		}
		else if (clazz.equals(Character.class) || clazz.equals(char.class))
		{
			return (char) o.intValue();
		}
		return null;
	}

	/**
	 * Check if the provided method is a valid table-mapped method. This returns
	 * true if:
	 * 
	 * 1. m.getName starts with "is" or "get"
	 * 
	 * 2. m returns non-void
	 * 
	 * 3. m accepts no parameters.
	 * 
	 * 4. m is not labelled Transient.
	 * 
	 * 5. m is not getClass
	 * 
	 * @param m
	 * @return true if m is a valid method.
	 */
	public static boolean isValidMethod(Method m)
	{
		int mod = m.getModifiers();
		if ((!Modifier.isStatic(mod) && !m.isSynthetic() &&  !m.getName().equals("getClass")
				&& !m.isAnnotationPresent(Transient.class) && !m.getReturnType().equals(void.class)
				&& (m.getName().startsWith("get") || m.getName().startsWith("is")) && m.getParameterTypes().length == 0))
		{
			if (!isIgnored(m))
			{
				return true;
			}
		}
		return false;
	}

	/**
	 * Return true if the method is one of the methods to be ignored.
	 * 
	 * This is necessary since some methods create havoc with the internal
	 * checks.
	 * 
	 * This only applies to classes in the system class library. All
	 * user-supplied classes can be supplied with the @transient annotation.
	 * 
	 * @param m
	 * @return
	 */
	private static boolean isIgnored(Method m)
	{
		if (isSubClassOf(m.getDeclaringClass(), java.util.AbstractCollection.class))
		{
			if (m.getName().equalsIgnoreCase("isEmpty"))
			{
				return true;
			}
		}
		if (isSubClassOf(m.getDeclaringClass(), java.util.AbstractMap.class))
		{
			if (m.getName().equalsIgnoreCase("isEmpty"))
			{
				return true;
			}
		}
		if (isSubClassOf(m.getDeclaringClass(), java.lang.String.class))
		{
			if (m.getName().equalsIgnoreCase("isEmpty"))
			{
				return true;
			}
			if (m.getName().equalsIgnoreCase("getBytes"))
			{
				return true;
			}
		}

		// these entries have to be removed from Date or else they will
		// overwrite the more accurate TIME value.
		if (isSubClassOf(m.getDeclaringClass(), java.util.Date.class))
		{
			if (m.getName().equalsIgnoreCase("getDate"))
			{
				return true;
			}
			if (m.getName().equalsIgnoreCase("getYear"))
			{
				return true;
			}
			if (m.getName().equalsIgnoreCase("getMonth"))
			{
				return true;
			}
			if (m.getName().equalsIgnoreCase("getDay"))
			{
				return true;
			}
			if (m.getName().equalsIgnoreCase("getHours"))
			{
				return true;
			}
			if (m.getName().equalsIgnoreCase("getMinutes"))
			{
				return true;
			}
			if (m.getName().equalsIgnoreCase("getSeconds"))
			{
				return true;
			}
			if (m.getName().equalsIgnoreCase("getCalendarDate"))
			{
				return true;
			}
			if (m.getName().equalsIgnoreCase("getJulianCalendar"))
			{
				return true;
			}
			if (m.getName().equalsIgnoreCase("getTimeImpl"))
			{
				return true;
			}
		}
		return false;
	}

	/**
	 * Recursively look up the inheritance tree for a mutator that matches the
	 * given name and the given argument type.
	 * 
	 * @param declaringClass
	 * @param mutatorName
	 *            the method name.
	 * @param argument
	 *            the argument type.
	 * @return the Method with the given name and single argument type.
	 */
	public static Method getMutator(Class<?> declaringClass, String mutatorName, Class<?> argument)
	{
		Method res = null;
		if (declaringClass != null)
		{
			// check this class
			try
			{
				res = declaringClass.getDeclaredMethod(mutatorName, argument);
			}
			catch (NoSuchMethodException e)
			{
				// do nothing, keep iterating up
			}
			if (res == null)
			{
				res = getMutator(declaringClass.getSuperclass(), mutatorName, argument);
			}
		}

		return res;
	}
	/**
	 * Recursively look up the inheritance tree for an accessor that matches the
	 * given name and the given argument type.
	 * 
	 * @param declaringClass
	 * @param accessorname
	 *            the method name.
	 * @return the Method with the given name and single argument type.
	 */
	public static Method getAccessor(Class<?> declaringClass, String accessorname)
	{
		Method res = null;
		if (declaringClass != null)
		{
			// check this class
			try
			{
				res = declaringClass.getDeclaredMethod(accessorname);
			}
			catch (NoSuchMethodException e)
			{
				// do nothing, keep iterating up
			}
			if (res == null)
			{
				res = getAccessor(declaringClass.getSuperclass(), accessorname);
			}
		}

		return res;
	}

}
