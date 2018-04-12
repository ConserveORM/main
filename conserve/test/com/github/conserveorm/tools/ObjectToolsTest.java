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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.conserveorm.objects.BaseInterface;
import com.github.conserveorm.objects.LessSimpleObject;
import com.github.conserveorm.objects.SerializableInheritingObject;
import com.github.conserveorm.objects.SimplestObject;
import com.github.conserveorm.objects.SubInterface;

/**
 * @author Erik Berglund
 *
 */
public class ObjectToolsTest
{

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception
	{
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception
	{
	}

	/**
	 * Test method for {@link com.github.conserveorm.tools.ObjectTools#getAllInterfaces(java.lang.Class)}.
	 */
	@Test
	public void testGetAllInterfaces()
	{
		List<Class<?>> interfaces = ObjectTools.getAllInterfaces(SubInterface.class);
		assertTrue(interfaces.contains(SubInterface.class));
		assertTrue(interfaces.contains(BaseInterface.class));
		assertTrue(interfaces.contains(Serializable.class));
		interfaces = ObjectTools.getAllInterfaces(SimplestObject.class);
		assertTrue(interfaces.isEmpty());
		interfaces = ObjectTools.getAllInterfaces(LessSimpleObject.class);
		assertTrue(interfaces.contains(Serializable.class));
		assertTrue(interfaces.contains(Runnable.class));
		assertTrue(interfaces.contains(SubInterface.class));
		assertTrue(interfaces.contains(BaseInterface.class));
		interfaces = ObjectTools.getAllInterfaces(SerializableInheritingObject.class);
		assertFalse(interfaces.contains(Serializable.class));
	}

}
