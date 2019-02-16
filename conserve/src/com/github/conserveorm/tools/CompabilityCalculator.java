/*******************************************************************************
 *  
 * Copyright (c) 2009, 2019 Erik Berglund.
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

import java.sql.Clob;
import java.sql.Blob;
import java.util.ArrayList;
import java.util.List;


/**
 * 
 * Tool that calculates whether two types are compatible for conversion.
 * 
 * 
 * @author Erik Berglund
 * 
 */
public class CompabilityCalculator
{
	/**
	 * A list of the primitive types' object representations and types that are
	 * represented directly as SQL types.
	 */
	private static List<Class<?>> primitives = new ArrayList<Class<?>>();
	private static List<Class<?>> ints = new ArrayList<Class<?>>();
	private static List<Class<?>> floats = new ArrayList<Class<?>>();

	private static List<Class<?>> directs = new ArrayList<Class<?>>();
	static
	{
		// the 8 basic primitives
		primitives.add(Boolean.class);
		primitives.add(Byte.class);
		primitives.add(Short.class);
		primitives.add(Character.class);
		primitives.add(Integer.class);
		primitives.add(Long.class);
		primitives.add(Float.class);
		primitives.add(Double.class);

		ints.add(boolean.class);
		ints.add(Boolean.class);
		ints.add(byte.class);
		ints.add(Byte.class);
		ints.add(char.class);
		ints.add(Character.class);
		ints.add(int.class);
		ints.add(Integer.class);
		ints.add(short.class);
		ints.add(Short.class);
		ints.add(long.class);
		ints.add(Long.class);
		floats.add(float.class);
		floats.add(Float.class);
		floats.add(double.class);
		floats.add(Double.class);

		// primitives for classes that have direct SQL representations
		directs.add(String.class);
		directs.add(java.sql.Blob.class);
		directs.add(java.sql.Clob.class);
		directs.add(java.sql.Date.class);
		directs.add(java.sql.Time.class);
		directs.add(java.sql.Timestamp.class);
	}

	/**
	 * Return true if the class represents a floating point type (double, float, or their object counterparts).
	 * @param c
	 * @return
	 */
	private static boolean isFloat(Class<?> c)
	{
		return floats.contains(c);
	}

	/**
	 * Returns true if the class represents an integer type (long, int, short, byte, char, boolean, or their object counterparts).
	 * @param c
	 * @return
	 */
	private static boolean isInteger(Class<?> c)
	{
		return ints.contains(c);
	}

	/**
	 * Return true if c is a primitive type or a primitive type object
	 * representation.
	 * 
	 * @param c
	 * @return
	 */
	private static boolean isPrimitive(Class<?> c)
	{
		if (c.isPrimitive())
		{
			return true;
		}
		else
		{
			return primitives.contains(c);
		}
	}
	
	/**
	 * Get the bit size of an integer type.
	 * 
	 * @param c
	 * @return
	 */
	private static int sizeof(Class<?> c)
	{
		if (c.equals(Boolean.class) || c.equals(boolean.class))
		{
			return 1;
		}
		else if (c.equals(Byte.class) || c.equals(byte.class))
		{
			return 8;
		}
		else if (c.equals(Integer.class) || c.equals(int.class))
		{
			return 32;
		}
		else if (c.equals(Long.class) || c.equals(long.class))
		{
			return 64;
		}
		else if (c.equals(Short.class) || c.equals(short.class) || c.equals(Character.class) || c.equals(char.class))
		{
			return 16;
		}
		else if (c.equals(Double.class)||c.equals(double.class))
		{
			return 64;
		}
		else
		{
			//float 
			return 32;
		}
	}

	/**
	 * @param fromClass
	 * @param toClass
	 * @return true if converting from fromClass to toClass will not result in
	 *         loss of data or generality.
	 */
	public static boolean calculate(Class<?> fromClass, Class<?> toClass)
	{
		if(fromClass.equals(toClass))
		{
			//no conversion taking place
			return true;
		}
		//Check primitives
		if(isPrimitive(fromClass)&&isPrimitive(toClass))
		{
			if(isInteger(fromClass)&&isInteger(toClass))
			{
				return sizeof(fromClass)<=sizeof(toClass);
			}
			else if(isFloat(fromClass)&&isFloat(toClass))
			{
				return sizeof(fromClass)<=sizeof(toClass);
			}
			else
			{
				//changing int to float or vice versa
				return false;
			}
		}
		//both are not primitives, check if either is primitive
		else if(isPrimitive(fromClass)||isPrimitive(toClass))
		{
			//one primitive and one non-primitive, no conversion
			return false;
		}
		// date and time can be converted to timestamp, all others are off
		if (toClass.equals(java.sql.Timestamp.class) && (fromClass.equals(java.sql.Time.class) || fromClass.equals(java.sql.Date.class)))
		{
			return true;
		}
		if (fromClass.isEnum() && toClass.equals(String.class))
		{
			//enums can be converted to strings, but not the other way around
			return true;
		}
		if(fromClass.isArray() 
				|| toClass.isArray())
		{
			//arrays can not be converted
			return false;
		}		
		//blobs and clobs can not be converted to or from
		if(fromClass.equals(Blob.class)  || toClass.equals(Blob.class))
		{
			return (toClass.equals(fromClass));
		}
		if(fromClass.equals(Clob.class) || toClass.equals(Clob.class))
		{
			return (toClass.equals(fromClass));
		}
		//enum can be converted to another enum
		if(fromClass.isEnum() || toClass.isEnum())
		{
			return (fromClass.isEnum() == toClass.isEnum());
		}
		
		//if we got this far it's a reference type
		//check if fromClass is a subclass of toClass or vice versa
		if(fromClass.isAssignableFrom(toClass)||toClass.isAssignableFrom(fromClass))
		{
			return true;
		}
		//if either reference is to an interface, conversion may be possible
		if(fromClass.isInterface() || toClass.isInterface())
		{
			return true;
		}
		
		//don't know how  to treat this, probably not possible to convert
		return false;
	}
}
