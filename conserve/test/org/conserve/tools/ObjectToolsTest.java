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

import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.conserve.objects.BaseInterface;
import org.conserve.objects.LessSimpleObject;
import org.conserve.objects.SerializableInheritingObject;
import org.conserve.objects.SimplestObject;
import org.conserve.objects.SubInterface;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
	 * Test method for {@link org.conserve.tools.ObjectTools#getAllInterfaces(java.lang.Class)}.
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
	/**
	 * Test method for {@link org.conserve.tools.ObjectTools#getAllInterfacesIncludingSuper(java.lang.Class)}.
	 */
	@Test
	public void testGetAllInterfacesIncludingSuper()
	{
		List<Class<?>> interfaces = ObjectTools.getAllInterfacesIncludingSuper(SubInterface.class);
		assertTrue(interfaces.contains(SubInterface.class));
		assertTrue(interfaces.contains(BaseInterface.class));
		assertTrue(interfaces.contains(Serializable.class));
		interfaces = ObjectTools.getAllInterfacesIncludingSuper(SimplestObject.class);
		assertTrue(interfaces.isEmpty());
		interfaces = ObjectTools.getAllInterfacesIncludingSuper(LessSimpleObject.class);
		assertTrue(interfaces.contains(Serializable.class));
		assertTrue(interfaces.contains(Runnable.class));
		assertTrue(interfaces.contains(SubInterface.class));
		assertTrue(interfaces.contains(BaseInterface.class));
		interfaces = ObjectTools.getAllInterfacesIncludingSuper(SerializableInheritingObject.class);
		assertTrue(interfaces.contains(Serializable.class));
	}

	/**
	 * Test method for {@link org.conserve.tools.ObjectTools#implementsInterface(java.lang.Class, java.lang.Class)}.
	 */
	@Test
	public void testImplementsInterface()
	{
		ArrayList<String> foo = new ArrayList<String>();
		assertTrue(ObjectTools.implementsInterfaceIncludingSuper(foo.getClass(), List.class));
		assertTrue(ObjectTools.implementsInterfaceIncludingSuper(foo.getClass(), Collection.class));
	}

}
