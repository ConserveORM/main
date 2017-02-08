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

package com.github.conserveorm.tools.uniqueid;

import java.util.ArrayList;
import java.util.List;

/**
 * Generates a series of unique strings. The strings fulfil these properties:
 * 
 * They are unique for the lifetime of the object - no sequence will be
 * generated twice.
 * 
 * They are as short as possible - the first string will be one character long
 * and two-character strings won't be generated until all possible one-character
 * strings have been generated and so on.
 * 
 * They are not equal to words that are likely to be reserved by the underlying
 * SQL database - such as IS, IF, NOT etc.
 * 
 * @author Erik Berglund
 * 
 */
public class UniqueIdGenerator
{
	private static final String[] PARTS = new String[] { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L",
			"M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z" };
	private static final String[] FORBIDDEN_STRINGS = new String[] { "IS", "IF", "AS", "ON", "IN", "OF", "OR", "AND",
			"NOT", "NULL", "FROM", "AVG", "SET", "UPDATE", "COUNT", "VALUE" };
	
	private List<String>forbiddenStrings=new ArrayList<String>();

	private int lastIndex;

	public synchronized String next()
	{
		String res = generateString();
		while (isForbidden(res))
		{
			res = generateString();
		}
		return res;
	}

	/**
	 * Generate the next sequence.
	 * 
	 * @return
	 */
	private String generateString()
	{
		StringBuilder res = new StringBuilder();
		int radix = PARTS.length;
		int tmp = lastIndex;
		do
		{
			int remainder = tmp % radix;
			tmp /= radix;
			res.append(PARTS[remainder]);

		} while (tmp > 0);
		lastIndex++;
		return res.toString();
	}
	
	/**
	 * Add a string that may not be used as an alias.
	 * @param forbiddenString
	 */
	public void addForbiddenString(String forbiddenString)
	{
		if(!isForbidden(forbiddenString))
		{
			this.forbiddenStrings.add(forbiddenString);
		}
	}

	/**
	 * Check if s is equal to one of the disallowed strings.
	 * 
	 * @param s
	 *            the string to check.
	 * @return true if s is disallowed, false otherwise.
	 */
	private boolean isForbidden(String s)
	{
		for (int x = 0; x < FORBIDDEN_STRINGS.length; x++)
		{
			if (s.equals(FORBIDDEN_STRINGS[x]))
			{
				return true;
			}
		}
		if(forbiddenStrings.contains(s))
		{
			return true;
		}
		return false;
	}
}
