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

package com.github.conserveorm.tools.generators;

import static org.junit.Assert.*;

import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;

import org.junit.Test;

import com.github.conserveorm.objects.DummyAdapter;
import com.github.conserveorm.tools.generators.StatementPrototypeGenerator;

/**
 * @author Erik Berglund
 *
 */
public class StatementPrototypeGeneratorTest
{

	
	@Test
	public void testIsSorted() throws Exception
	{
		StatementPrototypeGenerator gen = new StatementPrototypeGenerator(new DummyAdapter());
		//arrays are sorted
		assertTrue(gen.isSorted(double[].class));
		assertTrue(gen.isSorted(String[].class));
		assertTrue(gen.isSorted(Integer[].class));
		assertTrue(gen.isSorted(Object[].class));
		
		//test if general maps are unsorted
		//sorted maps are sorted
		assertTrue(gen.isSorted(SortedMap.class));
		assertTrue(gen.isSorted(NavigableMap.class));
		assertTrue(gen.isSorted(LinkedHashMap.class));
		assertTrue(gen.isSorted(EnumMap.class));
		assertFalse(gen.isSorted(HashMap.class));
		
		//some collections are not sorted
		assertFalse(gen.isSorted(Collection.class));
		assertFalse(gen.isSorted(HashSet.class));
		assertFalse(gen.isSorted(Set.class));
		//...others are
		assertTrue(gen.isSorted(Queue.class));
		assertTrue(gen.isSorted(List.class));
		assertTrue(gen.isSorted(SortedSet.class));
		assertTrue(gen.isSorted(LinkedHashSet.class));
		
		//some random object is not sorted
		assertNull(gen.isSorted(Object.class));	
	}

	@Test
	public void testIsCollectionsObject() throws Exception
	{
		StatementPrototypeGenerator gen = new StatementPrototypeGenerator(new DummyAdapter());
		assertTrue(gen.isCollectionsObject(Collection.class));
		assertTrue(gen.isCollectionsObject(double[].class));
		assertTrue(gen.isCollectionsObject(Map.class));
		assertTrue(gen.isCollectionsObject(Object[].class));
		assertTrue(gen.isCollectionsObject(Map[].class));
		assertTrue(gen.isCollectionsObject(HashMap.class));
		assertTrue(gen.isCollectionsObject(HashMap[].class));
		assertFalse(gen.isCollectionsObject(Double.class));
		assertFalse(gen.isCollectionsObject(double.class));
		assertFalse(gen.isCollectionsObject(String.class));
		assertFalse(gen.isCollectionsObject(Object.class));
		
	}
}
