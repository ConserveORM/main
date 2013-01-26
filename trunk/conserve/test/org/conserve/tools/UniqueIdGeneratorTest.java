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

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.conserve.tools.uniqueid.UniqueIdGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class UniqueIdGeneratorTest
{

	@Before
	public void setUp() throws Exception
	{
	}

	@After
	public void tearDown() throws Exception
	{
	}

	/**
	 * Generate a large set of keys, check that none are generated more than once.
	 */
	@Test
	public void testNext()
	{
		ArrayList<String> seen = new ArrayList<String>();
		UniqueIdGenerator generator = new UniqueIdGenerator();
		for(int x = 0;x<10000;x++)
		{
			String value = generator.next();
			assertTrue(value.length()>0);
			assertFalse(seen.contains(value));
			seen.add(value);
		}
	}

}
