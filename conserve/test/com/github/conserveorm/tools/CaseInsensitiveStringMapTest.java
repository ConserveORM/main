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

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.Test;

import com.github.conserveorm.tools.CaseInsensitiveStringMap;

/**
 * @author Erik Berglund
 *
 */
public class CaseInsensitiveStringMapTest
{

	/**
	 * Test method for {@link com.github.conserveorm.tools.CaseInsensitiveStringMap#clear()}.
	 */
	@Test
	public void testClear()
	{
		CaseInsensitiveStringMap map = new CaseInsensitiveStringMap();
		//add some data
		map.put("foo", "bar");
		map.put("bar", "42");
		assertEquals("bar",map.get("FOO"));
		assertEquals(2,map.size());
		map.clear();
		assertEquals(0,map.size());
		assertNull(map.get("foo"));
		assertNull(map.get("bar"));
		
	}

	/**
	 * Test method for {@link com.github.conserveorm.tools.CaseInsensitiveStringMap#containsKey(java.lang.Object)}.
	 */
	@Test
	public void testContainsKey()
	{
		CaseInsensitiveStringMap map = new CaseInsensitiveStringMap();
		//add some data
		map.put("foo", "bar");
		map.put("bar", "42");
		assertTrue(map.containsKey("foo"));
		assertTrue(map.containsKey("FOO"));
		assertTrue(map.containsKey("bar"));
		assertTrue(map.containsKey("BAR"));
		assertFalse(map.containsKey("fo"));
		assertFalse(map.containsKey("FOOO"));
		assertFalse(map.containsKey("barr"));
		assertFalse(map.containsKey("BA"));
	}

	/**
	 * Test method for {@link com.github.conserveorm.tools.CaseInsensitiveStringMap#containsValue(java.lang.Object)}.
	 */
	@Test
	public void testContainsValue()
	{
		CaseInsensitiveStringMap map = new CaseInsensitiveStringMap();
		//add some data
		map.put("foo", "bar");
		map.put("bar", "42");
		assertTrue(map.containsValue("bar"));
		assertTrue(map.containsValue("42"));
		assertFalse(map.containsValue("foo"));
		assertFalse(map.containsValue("FOO"));
	}

	/**
	 * Test method for {@link com.github.conserveorm.tools.CaseInsensitiveStringMap#entrySet()}.
	 */
	@Test
	public void testEntrySet()
	{
		CaseInsensitiveStringMap map = new CaseInsensitiveStringMap();
		//add some data
		map.put("foo", "bar");
		map.put("bar", "42");
		Set<Entry<String, String>> entrySet = map.entrySet();
		for(Entry<String,String>e:entrySet)
		{
			assertTrue(e.getKey().equalsIgnoreCase("foo")||e.getKey().equalsIgnoreCase("bar"));
			assertTrue(e.getValue().equalsIgnoreCase("42")||e.getValue().equalsIgnoreCase("bar"));
		}
		
	}

	/**
	 * Test method for {@link com.github.conserveorm.tools.CaseInsensitiveStringMap#get(java.lang.Object)}.
	 */
	@Test
	public void testGet()
	{
		CaseInsensitiveStringMap map = new CaseInsensitiveStringMap();
		//add some data
		map.put("foo", "bar");
		map.put("bar", "42");
		assertEquals("bar",map.get("FOO"));
		assertEquals("bar",map.get("foo"));
		assertEquals("42",map.get("BAR"));
		assertEquals("42",map.get("Bar"));
		assertNull(map.get("42"));
	}
	
	@Test
	public void testNulls()throws Exception
	{
		CaseInsensitiveStringMap map = new CaseInsensitiveStringMap();
		//add some data
		map.put("foo", "bar");
		map.put("bar", "42");
		assertFalse(map.containsKey(null));
		assertFalse(map.containsValue(null));
		
		//try null key
		map.put(null, "none");
		assertEquals(3,map.size());
		assertEquals("none",map.get(null));
		assertTrue(map.containsKey(null));
		
		//try null value
		map.put("nada", null);
		assertEquals(4,map.size());
		assertNull(map.get("nada"));
		assertTrue(map.containsValue(null));
		
		//try removing null key
		assertEquals("none", map.remove(null));
		assertEquals(3,map.size());
		assertNull(map.get(null));
		assertFalse(map.containsKey(null));
	}

	/**
	 * Test method for {@link com.github.conserveorm.tools.CaseInsensitiveStringMap#isEmpty()}.
	 */
	@Test
	public void testIsEmpty()
	{
		CaseInsensitiveStringMap map = new CaseInsensitiveStringMap();
		assertTrue(map.isEmpty());
		//add some data
		map.put("foo", "bar");
		map.put("bar", "42");
		assertFalse(map.isEmpty());
		map.clear();
		assertTrue(map.isEmpty());
	}



	/**
	 * Test method for {@link com.github.conserveorm.tools.CaseInsensitiveStringMap#putAll(java.util.Map)}.
	 */
	@Test
	public void testPutAll()
	{
		Map<String,String>input = new HashMap<>();
		input.put("foo", "bar");
		input.put("BAZ", "42");
		CaseInsensitiveStringMap map = new CaseInsensitiveStringMap();
		map.putAll(input);
		assertEquals("bar",map.get("FOO"));
		assertEquals("42",map.get("baz"));
		assertEquals(2,map.size());
		map.clear();
		assertEquals(0,map.size());
		assertNull(map.get("foo"));
		assertNull(map.get("baz"));
		
		
	}

	/**
	 * Test method for {@link com.github.conserveorm.tools.CaseInsensitiveStringMap#remove(java.lang.Object)}.
	 */
	@Test
	public void testRemove()
	{
		CaseInsensitiveStringMap map = new CaseInsensitiveStringMap();
		//add some data
		map.put("foo", "bar");
		map.put("bar", "42");
		assertTrue(map.containsKey("foo"));
		assertTrue(map.containsKey("foO"));
		assertTrue(map.containsValue("bar"));
		assertEquals("bar",map.remove("fOO"));
		assertFalse(map.containsKey("foo"));
		assertFalse(map.containsKey("foO"));
		assertFalse(map.containsValue("bar"));
		
	}



}
