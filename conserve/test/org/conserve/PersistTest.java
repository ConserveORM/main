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
package org.conserve;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.conserve.PersistenceManager;
import org.conserve.connection.ConnectionWrapper;
import org.conserve.objects.Author;
import org.conserve.objects.BaseInterface;
import org.conserve.objects.BlobClobObject;
import org.conserve.objects.Book;
import org.conserve.objects.ComplexArrayObject;
import org.conserve.objects.ComplexObject;
import org.conserve.objects.DateObject;
import org.conserve.objects.LessSimpleObject;
import org.conserve.objects.ListContainingObject;
import org.conserve.objects.SelfContainingObject;
import org.conserve.objects.SimpleObject;
import org.conserve.objects.SimplestObject;
import org.conserve.objects.polymorphism.AbstractBar;
import org.conserve.objects.polymorphism.ConcreteBar1;
import org.conserve.objects.polymorphism.ConcreteBar2;
import org.conserve.objects.polymorphism.ExtendedFooContainer;
import org.conserve.objects.polymorphism.MyExtendedFooContainer;
import org.conserve.objects.polymorphism.Foo;
import org.conserve.objects.polymorphism.FooContainer;
import org.conserve.objects.polymorphism.FooContainerOwner;
import org.conserve.objects.polymorphism.MyFooContainer;
import org.conserve.objects.recursive.Layer1;
import org.conserve.objects.recursive.Layer2;
import org.conserve.objects.recursive.Layer3;
import org.conserve.objects.schemaupdate.ChangedInheritance;
import org.conserve.objects.schemaupdate.NewName;
import org.conserve.objects.schemaupdate.NotSubClass;
import org.conserve.objects.schemaupdate.OriginalObject;
import org.conserve.objects.schemaupdate.RemovedColumn;
import org.conserve.objects.schemaupdate.RenamedColumn;
import org.conserve.objects.schemaupdate.SubClass;
import org.conserve.objects.schemaupdate.changedcolumns.ArrayToLong;
import org.conserve.objects.schemaupdate.changedcolumns.IntToLong;
import org.conserve.objects.schemaupdate.changedcolumns.ObjectToLong;
import org.conserve.objects.schemaupdate.changedcolumns.ObjectToSubclass;
import org.conserve.objects.schemaupdate.changedcolumns.StringToLong;
import org.conserve.objects.schemaupdate.copydown.ModifiedBottom;
import org.conserve.objects.schemaupdate.copydown.ModifiedMiddle;
import org.conserve.objects.schemaupdate.copydown.OriginalBottom;
import org.conserve.objects.schemaupdate.copydown.ModifiedTop;
import org.conserve.objects.schemaupdate.copydown.OriginalMiddle;
import org.conserve.objects.schemaupdate.copydown.OriginalTop;
import org.conserve.select.All;
import org.conserve.select.And;
import org.conserve.select.Or;
import org.conserve.select.discriminators.Different;
import org.conserve.select.discriminators.Equal;
import org.conserve.select.discriminators.Greater;
import org.conserve.select.discriminators.GreaterOrEqual;
import org.conserve.select.discriminators.LessOrEqual;
import org.conserve.sort.Ascending;
import org.conserve.sort.Descending;
import org.conserve.sort.Order;
import org.conserve.tools.ObjectTools;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration test of various functionality.
 * 
 * @author Erik Berglund
 * 
 */
public class PersistTest
{
	// Settings for test
	protected String driver;
	protected String database;
	protected String login;
	protected String password;
	protected String secondDatabase;

	private static final Logger LOGGER = Logger.getLogger("org.conserve");
	// create a log handler that outputs all warning events to the console
	static
	{
		ConsoleHandler consoleHandler = new ConsoleHandler();
		LOGGER.addHandler(consoleHandler);
		Level level = Level.FINE;
		LOGGER.setLevel(level);
		consoleHandler.setLevel(level);
	}

	/**
	 * Classes overriding this method should set database connection strings
	 * before calling deleteAll().
	 * 
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception
	{
		driver = "org.h2.Driver";
		database = "jdbc:h2:tcp://localhost/~/test";
		secondDatabase = "jdbc:h2:tcp://localhost/~/test2";
		login = "sa";
		password = "";
		deleteAll();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception
	{
		// deleteAll();
	}

	protected void deleteAll() throws SQLException
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		persist.deleteObjects(new Object());// clear everything
		persist.close();
	}

	/**
	 * Test method for
	 * {@link org.conserve.PersistenceManager#saveObject(java.lang.Object)}.
	 */
	@Test
	public void testAddSimplestObject() throws Exception
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		// create a test object
		SimplestObject so = new SimplestObject();
		so.setFoo(0.67);
		persist.saveObject(so);
		persist.close();
	}

	/**
	 * Test method for
	 * {@link org.conserve.PersistenceManager#saveObject(java.lang.Object)}.
	 */
	@Test
	public void testAddObject() throws Exception
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		// create a test object
		SimpleObject so = new SimpleObject();
		so.setAge(1000L);
		so.setCount(3);
		so.setName("foo bar");
		so.setScale(0.67);
		persist.saveObject(so);
		LessSimpleObject lso = new LessSimpleObject();
		lso.setAge(1000000L);
		lso.setCount(123456);
		lso.setKey((short) 99);
		lso.setName("j random");
		lso.setScale(-9.0);
		persist.saveObject(lso);
		// insert the same object again
		persist.saveObject(lso);
		// new less-simple object
		persist.close();
	}

	/**
	 * Try returning the objects inserted, make sure the right count is
	 * returned.
	 * 
	 */
	@Test
	public void testGetObjectFromInterface() throws Exception
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		// create a test object
		SimpleObject so = new SimpleObject();
		so.setAge(1000L);
		so.setCount(3);
		so.setName("foo bar");
		so.setScale(0.67);
		persist.saveObject(so);
		LessSimpleObject lso = new LessSimpleObject();
		lso.setAge(1000000L);
		lso.setCount(123456);
		lso.setKey((short) 99);
		lso.setName("j random");
		lso.setScale(-9.0);
		persist.saveObject(lso);
		// insert the same object again
		persist.saveObject(lso);
		persist.close();

		persist = new PersistenceManager(driver, database, login, password);
		// try getting all the objects
		List<BaseInterface> obs = persist.getObjects(BaseInterface.class, new All());
		assertEquals(1, obs.size());
		persist.close();
	}

	/**
	 * Try returning the objects
	 * 
	 */
	@Test
	public void testGetObjectFromClass() throws Exception
	{

		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		// create a test object
		SimpleObject so = new SimpleObject();
		so.setAge(1000L);
		so.setCount(3);
		so.setName("foo bar");
		so.setScale(0.67);
		persist.saveObject(so);
		LessSimpleObject lso = new LessSimpleObject();
		lso.setAge(1000000L);
		lso.setCount(123456);
		lso.setKey((short) 99);
		lso.setName("j random");
		lso.setScale(-9.0);
		persist.saveObject(lso);
		// insert the same object again
		persist.saveObject(lso);
		persist.close();

		persist = new PersistenceManager(driver, database, login, password);
		// try getting all the objects
		SimpleObject pattern = new SimpleObject();
		pattern.setCount(3);
		List<SimpleObject> obs = persist.getObjects(SimpleObject.class, new GreaterOrEqual(pattern));
		assertEquals(2, obs.size());
		pattern.setCount(4);
		obs = persist.getObjects(SimpleObject.class, new GreaterOrEqual(pattern));
		assertEquals(1, obs.size());
		pattern.setCount(3);
		List<LessSimpleObject> obs2 = persist.getObjects(LessSimpleObject.class, new GreaterOrEqual(pattern));
		assertEquals(1, obs2.size());

		// assert that two objects are equal if they are the result of the same
		// search
		List<LessSimpleObject> obs3 = persist.getObjects(LessSimpleObject.class, new GreaterOrEqual(pattern));
		assertEquals(obs2.get(0), obs3.get(0));

		persist.close();
	}

	/**
	 * Test deleting objects.
	 */
	@Test
	public void testDeleteObject() throws Exception
	{

		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		// create a test object
		SimpleObject so = new SimpleObject();
		so.setAge(1000L);
		so.setCount(3);
		so.setName("foo bar");
		so.setScale(0.67);
		persist.saveObject(so);
		LessSimpleObject lso = new LessSimpleObject();
		lso.setAge(1000000L);
		lso.setCount(123456);
		lso.setKey((short) 99);
		lso.setName("j random");
		lso.setScale(-9.0);
		persist.saveObject(lso);
		// insert the same object again
		persist.saveObject(lso);
		persist.close();

		persist = new PersistenceManager(driver, database, login, password);
		// try deleting one object
		List<Object> allObjects = persist.getObjectsMatching(new Object());
		assertEquals(2, allObjects.size());
		// delete one LessSimpleObject
		SimpleObject pattern = new LessSimpleObject();
		persist.deleteObjects(LessSimpleObject.class, new GreaterOrEqual(pattern));
		allObjects = persist.getObjectsMatching(new Object());
		assertEquals(1, allObjects.size());
		// delete all objects
		persist.deleteObjects(new Object());
		allObjects = persist.getObjectsMatching(new Object());
		assertTrue(allObjects.isEmpty());
		persist.close();
	}

	/**
	 * Test inserting complex objects.
	 * 
	 */
	@Test
	public void testAddComplexObject() throws Exception
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);

		ComplexObject co = new ComplexObject(new double[] { 1.1, 2.2, 3.3 });
		LessSimpleObject so = new LessSimpleObject();
		so.setScale(0.67);
		co.setObject(so);
		SimplestObject sto = new SimplestObject();
		sto.setFoo(9999.6);
		co.setSimplestObject(sto);
		persist.saveObject(co);
		ComplexObject co2 = new ComplexObject(co.getData());
		co2.setObject(so);
		co2.setSimplestObject(sto);
		persist.saveObject(co2);
		persist.close();
		persist = new PersistenceManager(driver, database, login, password);
		// List<Object> allList = persist.getObjects(new Object());
		// assertEquals(4, allList.size());
		List<ComplexObject> complexList = persist.getObjectsMatching(new ComplexObject());
		assertEquals(2, complexList.size());
		ComplexObject a = complexList.get(0);
		ComplexObject b = complexList.get(1);
		assertEquals(a.getData(), b.getData());
		assertTrue(a.getData() != null);
		assertEquals(3, a.getData().length);
		assertTrue(a.getData()[2] == 3.3);
		persist.close();
	}

	/**
	 * Add/remove/retrieve objects with arrays of complex objects.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testComplexArray() throws Exception
	{
		// create the database connection
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);

		ComplexObject co1 = new ComplexObject(new double[] { 1.1, 2.2, 3.3, 4.4 });
		persist.saveObject(co1);

		// create an array of complex objects
		ComplexArrayObject cao = new ComplexArrayObject();
		ComplexObject[] coArray = new ComplexObject[3];
		for (int x = 0; x < coArray.length; x++)
		{
			ComplexObject co = new ComplexObject();
			co.setData(new double[] { x, x * 2, x * 3 });
			SimpleObject so = new SimpleObject();
			so.setAge((long) x);
			co.setObject(so);
			coArray[x] = co;
		}
		cao.setData(coArray);
		persist.saveObject(cao);
		persist.close();
		persist = new PersistenceManager(driver, database, login, password);
		List<ComplexArrayObject> res = persist.getObjectsMatching(new ComplexArrayObject());
		assertEquals(1, res.size());
		ComplexArrayObject ob1 = res.get(0);
		assertEquals(3, ob1.getData().length);
		persist.close();

	}

	/**
	 * Make sure no unrelated objects are deleted when deleting a complex
	 * object.
	 */
	@Test
	public void testDeleteComplexObject() throws Exception
	{
		// create the database connection
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);

		ComplexObject co1 = new ComplexObject(new double[] { 1.1, 2.2, 3.3, 4.4 });
		// saving the object gives it an outside reference
		persist.saveObject(co1);

		// create an array of complex objects
		ComplexObject[] coArray = new ComplexObject[3];
		for (int x = 0; x < coArray.length - 1; x++)
		{
			ComplexObject co = new ComplexObject();
			co.setData(new double[] { x, x * 2, x * 3 });
			SimpleObject so = new SimpleObject();
			so.setAge((long) x);
			co.setObject(so);
			coArray[x] = co;
		}
		// one of the complex objects in the array has an outside reference
		coArray[2] = co1;

		ComplexArrayObject cao = new ComplexArrayObject();
		cao.setData(coArray);
		persist.saveObject(cao);
		persist.close();

		// check that there is one ComplexArrayObject
		persist = new PersistenceManager(driver, database, login, password);
		List<ComplexArrayObject> res = persist.getObjectsMatching(new ComplexArrayObject());
		assertEquals(1, res.size());
		ComplexArrayObject ob1 = res.get(0);
		// check that the ComplexArrayObject contains 3 ComplexObjects
		assertEquals(3, ob1.getData().length);
		persist.close();

		persist = new PersistenceManager(driver, database, login, password);
		persist.deleteObjects(new ComplexArrayObject());
		List<ComplexObject> tmp = persist.getObjectsMatching(new ComplexObject());
		assertEquals(1, tmp.size());
		persist.close();
	}

	/**
	 * Test adding an array that consists of other arrays.
	 * 
	 */
	@Test
	public void testArrayOfArrays() throws Exception
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		ComplexObject co = new ComplexObject();
		SimplestObject[][][] array = new SimplestObject[1][1][1];
		for (int x = 0; x < array.length; x++)
		{
			for (int y = 0; y < array[x].length; y++)
			{
				array[x][y][0] = new SimplestObject();
			}
		}
		co.setObject(array);
		persist.saveObject(co);
		ComplexObject co2 = new ComplexObject();
		SimplestObject[][][] array2 = new SimplestObject[1][][];
		array2[0] = array[0];
		co2.setObject(array2);
		persist.saveObject(co2);
		persist.close();
		persist = new PersistenceManager(driver, database, login, password);
		List<ComplexObject> cos = persist.getObjectsMatching(new ComplexObject());
		assertEquals(2, cos.size());
		co = cos.get(0);
		SimplestObject[][][] resArray = (SimplestObject[][][]) co.getObject();
		assertTrue(resArray != null);
		assertEquals(1, resArray.length);
		assertEquals(1, resArray[0].length);
		assertEquals(1, resArray[0][0].length);
		assertTrue(resArray[0][0][0] != null);
		co2 = cos.get(1);
		SimplestObject[][][] resArray2 = (SimplestObject[][][]) co2.getObject();
		assertTrue(resArray != resArray2);
		assertArrayEquals(resArray[0], resArray2[0]);

		persist.close();
	}

	/**
	 * Makes sure that it is possible to search based on contents of complex
	 * objects.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testInnerMatching() throws Exception
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		SimplestObject so = new SimplestObject();
		so.setFoo(1.0);
		ComplexObject co = new ComplexObject();
		co.setObject(so);
		persist.saveObject(co);
		// close the persistence object
		persist.close();

		// re-open the persistence object
		persist = new PersistenceManager(driver, database, login, password);
		// create a SimplestObject so that no results will match
		so = new SimplestObject();
		so.setFoo(2.0);
		// make sure this matches no results
		ComplexObject search = new ComplexObject();
		search.setObject(so);
		List<ComplexObject> none = persist.getObjectsMatching(search);
		assertEquals(0, none.size());
		// change the search so that one object is returned
		so.setFoo(1.0);
		List<ComplexObject> one = persist.getObjectsMatching(search);
		assertEquals(1, one.size());

		persist.close();

	}

	/**
	 * Makes sure that it is possible to search based on contents of primitive
	 * arrays.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testArrayMatching() throws Exception
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		// create a complex object with an Integer array as object
		Integer[] intArray = new Integer[] { 4, 5, 6 };
		ComplexObject co = new ComplexObject();
		co.setObject(intArray);
		persist.saveObject(co);
		// close the persistence object
		persist.close();

		// re-open the persistence object
		persist = new PersistenceManager(driver, database, login, password);
		// create an integer array where all entries but one is null
		Integer[] searchArray = new Integer[3];
		searchArray[2] = 7;
		// make sure this matches no results
		ComplexObject search = new ComplexObject();
		search.setObject(searchArray);
		List<ComplexObject> none = persist.getObjectsMatching(search);
		assertEquals(0, none.size());
		// change the search so that one object is returned
		searchArray[1] = 5;
		searchArray[2] = 6;
		List<ComplexObject> one = persist.getObjectsMatching(search);
		assertEquals(1, one.size());

		persist.close();
	}

	/**
	 * Makes sure that it is possible to search based on contents of
	 * non-primitive arrays.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testComplexArrayMatching() throws Exception
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		// create a complex object with an Integer array as object
		SimplestObject[] array = new SimplestObject[] { new SimplestObject(), new SimplestObject(),
				new SimplestObject() };
		for (int x = 0; x < array.length; x++)
		{
			array[x].setFoo((double) (x + 1));
		}
		ComplexObject co = new ComplexObject();
		co.setObject(array);
		persist.saveObject(co);
		// close the persistence object
		persist.close();

		// re-open the persistence object
		persist = new PersistenceManager(driver, database, login, password);
		// create an integer array where all entries but one is null
		SimplestObject[] searchArray = new SimplestObject[3];
		searchArray[2] = new SimplestObject();
		searchArray[2].setFoo(2.0);
		// make sure this matches no results
		ComplexObject search = new ComplexObject();
		search.setObject(searchArray);
		List<ComplexObject> none = persist.getObjectsMatching(search);
		assertEquals(0, none.size());
		// change the search so that one object is returned
		searchArray[1] = new SimplestObject();
		searchArray[1].setFoo(2.0);
		searchArray[2].setFoo(3.0);
		List<ComplexObject> one = persist.getObjectsMatching(search);
		assertEquals(1, one.size());

		persist.close();
	}

	/**
	 * Make sure that it is possible to search based on contents of arrays of
	 * arrays.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testArrayOfArraysMatching() throws Exception
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		// create a complex object with an Integer array as object
		SimplestObject[][] array = new SimplestObject[][] { {},
				{ new SimplestObject(), new SimplestObject(), new SimplestObject() }, {} };
		for (int x = 0; x < array[1].length; x++)
		{
			array[1][x].setFoo((double) (x + 1));
		}
		ComplexObject co = new ComplexObject();
		co.setObject(array);
		persist.saveObject(co);
		// close the persistence object
		persist.close();

		// re-open the persistence object
		persist = new PersistenceManager(driver, database, login, password);
		// create an integer array where all entries but one is null
		SimplestObject[][] searchArray = new SimplestObject[3][3];
		searchArray[1][2] = new SimplestObject();
		searchArray[1][2].setFoo(2.0);
		// make sure this matches no results
		ComplexObject search = new ComplexObject();
		search.setObject(searchArray);
		List<ComplexObject> none = persist.getObjectsMatching(search);
		assertEquals(0, none.size());
		// change the search so that one object is returned
		searchArray[1][1] = new SimplestObject();
		searchArray[1][1].setFoo(2.0);
		searchArray[1][2].setFoo(3.0);
		List<ComplexObject> one = persist.getObjectsMatching(search);
		assertEquals(1, one.size());

		persist.close();

	}

	/**
	 * Tries to save a variety of objects implementing java.util.Collection.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCollectionSave() throws Exception
	{
		ArrayList<Double> foo = new ArrayList<Double>();
		foo.add(6.9);
		foo.add(1.1);
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		persist.saveObject(foo);
		List<Object> all = persist.getObjectsMatching(new Object());
		assertEquals(1, all.size());
		persist.close();
	}

	/**
	 * Saves an instance of Collection and loads it, verifying that the contents
	 * are still the same.
	 */
	@Test
	public void testCollectionLoad() throws Exception
	{
		ArrayList<Double> foo = new ArrayList<Double>();
		foo.add(6.9);
		foo.add(1.1);
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		persist.saveObject(foo);
		persist.close();

		persist = new PersistenceManager(driver, database, login, password);

		List<ArrayList<Object>> result = persist.getObjectsMatching(new ArrayList<Object>());
		assertEquals(1, result.size());
		ArrayList<?> first = result.get(0);
		assertEquals(2, first.size());
		assertEquals(6.9, first.get(0));
		assertEquals(1.1, first.get(1));
		List<Object> all = persist.getObjectsMatching(new Object());
		assertEquals(1, all.size());
		persist.close();

	}

	/**
	 * Tries to save a variety of objects implementing java.util.Map.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testMapSave() throws Exception
	{
		Map<String, Double> map = new HashMap<String, Double>();
		map.put("KEY", 3.0);
		map.put("ANOTHERKEY", 4.0);
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		persist.saveObject(map);
		List<Object> all = persist.getObjectsMatching(new Object());
		assertEquals(1, all.size());
		persist.close();
	}

	/**
	 * Saves an instance of Map and loads it, verifying that the contents are
	 * still the same.
	 */
	@Test
	public void testMapLoad() throws Exception
	{
		Map<String, Double> map = new HashMap<String, Double>();
		map.put("KEY", 3.0);
		map.put("ANOTHERKEY", 4.0);
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		persist.saveObject(map);
		persist.close();

		persist = new PersistenceManager(driver, database, login, password);
		List<HashMap<Object, Object>> results = persist.getObjectsMatching(new HashMap<Object, Object>());
		assertEquals(1, results.size());
		HashMap<?, ?> first = results.get(0);
		assertEquals(2, first.size());
		assertEquals(3.0, first.get("KEY"));
		assertEquals(4.0, first.get("ANOTHERKEY"));
		List<Object> all = persist.getObjectsMatching(new Object());
		assertEquals(1, all.size());
		persist.close();
	}

	/**
	 * Search in collections based on contents.
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	@Test
	public void testCollectionSearch() throws Exception
	{
		// store some ArrayLists in the database
		List<String> list = new ArrayList<String>();
		list.add("VALUE");
		list.add("ANOTHER VALUE");
		list.add("THIRD VALUE");
		list.add("FOURTH VALUE");
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		persist.saveObject(list);
		List<String> anotherList = new ArrayList<String>();
		anotherList.add("ANOTHER VALUE");
		anotherList.add("SECOND VALUE");
		anotherList.add("THIRD VALUE");
		persist.saveObject(anotherList);
		List<String> yetAnotherList = new ArrayList<String>();
		yetAnotherList.add("VALUE");
		yetAnotherList.add("SOME VALUE");
		persist.saveObject(yetAnotherList);
		persist.close();

		persist = new PersistenceManager(driver, database, login, password);
		// retrieve objects by a sorted object
		List<Collection> allObjects = persist.getObjects(Collection.class, new All());
		assertEquals(3, allObjects.size());

		// find all objects that has the first entry equal to 'VALUE'.
		ArrayList<String> searchList = new ArrayList<String>();
		searchList.add("VALUE");
		List<Collection> searchResults = persist.getObjects(Collection.class, new Equal(searchList));
		// ensure that there are two
		assertEquals(2, searchResults.size());

		// find all objects that has the first entry equal to 'ANOTHER VALUE'
		searchList.clear();
		searchList.add("ANOTHER VALUE");
		searchResults = persist.getObjects(Collection.class, new Equal(searchList));
		// ensure that there is one
		assertEquals(1, searchResults.size());

		// find all objects that has an entry equal to 'ANOTHER VALUE'
		HashSet<String> unorderedSearchSet = new HashSet<String>();
		unorderedSearchSet.add("ANOTHER VALUE");
		searchResults = persist.getObjects(Collection.class, new Equal(unorderedSearchSet));
		// ensure that there are two
		assertEquals(2, searchResults.size());

		persist.close();
	}

	/**
	 * Search in maps based on contents.
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	@Test
	public void testMapSearch() throws Exception
	{
		// store some hashmaps in the database
		Map<String, Double> map = new HashMap<String, Double>();
		map.put("KEY", 3.0);
		map.put("ANOTHERKEY", 4.0);
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		persist.saveObject(map);
		Map<String, Double> anotherMap = new HashMap<String, Double>();
		anotherMap.put("ANOTHERKEY", 4.0);
		persist.saveObject(anotherMap);
		Map<String, Double> yetAnotherMap = new HashMap<String, Double>();
		yetAnotherMap.put("KEY", 1.0);
		persist.saveObject(yetAnotherMap);
		persist.close();

		persist = new PersistenceManager(driver, database, login, password);
		// get all objects
		List<Map> allObjects = persist.getObjects(Map.class, new All());
		assertEquals(3, allObjects.size());

		// search by map contents
		Map<String, Double> searchMap = new HashMap<String, Double>();
		searchMap.put("KEY", 3.0);
		List<Map> result = persist.getObjects(Map.class, new Equal(searchMap));
		assertEquals(1, result.size());
		searchMap.clear();

		// search for all hashmaps with a given key
		searchMap.put("KEY", null);
		result = persist.getObjects(Map.class, new Equal(searchMap));
		assertEquals(2, result.size());
		searchMap.clear();

		// search for all hashmaps with a given key-value pair
		searchMap.put("ANOTHERKEY", 4.0);
		result = persist.getObjects(Map.class, new Equal(searchMap));
		assertEquals(2, result.size());

		persist.close();

	}

	/**
	 * Test updating a simple object.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testUpdateSimple() throws Exception
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		// create a simple object
		SimpleObject object = new SimpleObject();
		object.setAge(500L);
		// save it
		persist.saveObject(object);
		// make sure only one object exists
		List<SimpleObject> allObjects = persist.getObjectsMatching(new SimpleObject());
		assertEquals(1, allObjects.size());
		// change the object
		object.setAge(1000L);
		// save the object again
		persist.saveObject(object);
		// make sure there's still only one object
		allObjects = persist.getObjectsMatching(new SimpleObject());
		assertEquals(1, allObjects.size());
		// re-open the database connection
		persist.close();
		persist = new PersistenceManager(driver, database, login, password);
		// make sure the object has been changed
		allObjects = persist.getObjectsMatching(new SimpleObject());
		assertEquals(1, allObjects.size());
		SimpleObject first = allObjects.get(0);
		assertTrue(first.getAge().equals(1000L));

		// add another object
		SimpleObject nuObject = new SimpleObject();
		nuObject.setAge(1L);
		persist.saveObject(nuObject);

		// update the new object
		nuObject.setAge(2L);

		// make sure only one object has been modified
		allObjects = persist.getObjectsMatching(new SimpleObject());
		assertEquals(2, allObjects.size());
		first = allObjects.get(0);
		SimpleObject second = allObjects.get(1);
		assertTrue((first.getAge() == 2L && second.getAge() == 1000L)
				|| (first.getAge() == 1000L && second.getAge() == 2L));

		persist.close();

	}

	/**
	 * Test updating a complex object.
	 * 
	 */
	@Test
	public void testUpdateComplex() throws Exception
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		// create a complex object
		ComplexObject object = new ComplexObject();
		Integer[] ages = new Integer[] { 1, 2, 3 };
		object.setObject(ages);

		// save it
		persist.saveObject(object);
		// make sure only one object exists
		List<ComplexObject> allObjects = persist.getObjectsMatching(new ComplexObject());
		assertEquals(1, allObjects.size());
		// change the object
		ages[1] = 20;
		// save the object again
		persist.saveObject(object);
		// make sure there's still only one object
		allObjects = persist.getObjectsMatching(new ComplexObject());
		assertEquals(1, allObjects.size());
		// re-open the database connection
		persist.close();
		persist = new PersistenceManager(driver, database, login, password);
		// make sure the object has been changed
		allObjects = persist.getObjectsMatching(new ComplexObject());
		assertEquals(1, allObjects.size());
		ComplexObject first = allObjects.get(0);
		Integer[] databaseAges = (Integer[]) first.getObject();
		assertEquals(1, (int) databaseAges[0]);
		assertEquals(20, (int) databaseAges[1]);
		assertEquals(3, (int) databaseAges[2]);
		assertTrue(databaseAges.length == 3);

		// add another object
		ComplexObject nuObject = new ComplexObject();
		Integer[] nuAges = new Integer[] { 5, 6, 7 };
		nuObject.setObject(nuAges);
		persist.saveObject(nuObject);

		// update the new object
		nuAges[1] = 60;

		// make sure only one object has been modified
		allObjects = persist.getObjectsMatching(new ComplexObject());
		assertEquals(2, allObjects.size());
		first = allObjects.get(0);
		ComplexObject second = allObjects.get(1);
		Integer[] a = (Integer[]) first.getObject();
		Integer[] b = (Integer[]) second.getObject();

		assertTrue((a[0] == 1 && b[0] == 5) || (b[0] == 1 && a[0] == 5));
		assertTrue((a[1] == 20 && b[1] == 60) || (b[1] == 20 && a[1] == 60));
		assertTrue((a[2] == 3 && b[2] == 7) || (b[2] == 3 && a[2] == 7));

		persist.close();
	}

	/**
	 * Tests getting a count of objects.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCount() throws Exception
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		ConnectionWrapper cw = persist.getConnectionWrapper();
		// add a large number of entries
		for (int x = 0; x < 200; x++)
		{
			SimplestObject so = new SimplestObject();
			so.setFoo((double) x);
			persist.saveObject(cw, so);
		}
		cw.commitAndDiscard();
		// make sure the right number of objects are returned
		long count = persist.getCount(new SimplestObject());
		assertEquals(200L, count);
		// select half of the objects
		SimplestObject selectObject = new SimplestObject();
		selectObject.setFoo(99.0);
		Greater g = new Greater(selectObject);
		count = persist.getCount(SimplestObject.class, g);
		assertEquals(100L, count);
		persist.close();
	}

	/**
	 * Try getting a list of persisted classes.
	 * 
	 */
	@Test
	public void testGetClasses() throws Exception
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		persist.saveObject(new SimpleObject());
		List<Class<?>> classList = persist.getClasses();
		for (Class<?> c : classList)
		{
			System.out.println(ObjectTools.getSystemicName(c));
		}
		// there should be at least 3 classes: SimpleObject, Object and
		// Serializable.
		assertTrue(classList.size() > 2);

		persist.close();
	}

	/**
	 * Make sure the java.util.Date type is correctly handled.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testJavaDate() throws Exception
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		Date date = new Date();
		persist.saveObject(date);

		persist.close();

		persist = new PersistenceManager(driver, database, login, password);
		List<Date> dates = persist.getObjects(Date.class, new LessOrEqual(new Date()));
		assertEquals(1, dates.size());
		assertEquals(date.getTime(), dates.get(0).getTime());
		persist.close();
	}

	/**
	 * Make sure the java.sql.Date, Time and Timestamp types are correctly
	 * handled.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testSqlDate() throws Exception
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		DateObject dateO = new DateObject();
		dateO.setDate(new java.sql.Date(System.currentTimeMillis()));
		dateO.setTime(new java.sql.Time(System.currentTimeMillis()));
		dateO.setTimeStamp(new java.sql.Timestamp(System.currentTimeMillis()));
		persist.saveObject(dateO);

		// make sure only one object exists
		List<Object> all = persist.getObjectsMatching(new Object());
		assertEquals(1, all.size());

		persist.close();
	}

	/**
	 * Try storing and loading CLOBs and BLOBs.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testBlobClob() throws Exception
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		BlobClobObject bco = new BlobClobObject();
		bco.setBytes(new byte[] { 3, 54, 1, 9 });
		bco.setChars(new char[] { 'a', 'b', 'c' });
		persist.saveObject(bco);

		// re-open the persistence object
		persist.close();
		persist = new PersistenceManager(driver, database, login, password);
		// make sure countworks for CLOB/BLOB objects
		long clobCount = persist.getCount(new BlobClobObject());
		assertEquals(1, clobCount);
		// get the one and only bco object
		List<BlobClobObject> all = persist.getObjectsMatching(new BlobClobObject());
		assertEquals(1, all.size());
		BlobClobObject first = all.get(0);
		assertEquals(bco.getBytes().length, first.getBytes().length);
		assertEquals(bco.getChars().length, first.getChars().length);
		for (int x = 0; x < bco.getChars().length; x++)
		{
			assertEquals(bco.getChars()[x], first.getChars()[x]);
		}
		for (int x = 0; x < bco.getBytes().length; x++)
		{
			assertEquals(bco.getBytes()[x], first.getBytes()[x]);
		}
		persist.close();
	}

	/**
	 * Test sorting simple objects.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testSortingSimple() throws Exception
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		ConnectionWrapper cw = persist.getConnectionWrapper();
		// add a large number of entries
		for (int x = 0; x < 200; x++)
		{
			SimplestObject so = new SimplestObject();
			so.setFoo((double) x);
			persist.saveObject(cw, so);
		}
		cw.commitAndDiscard();
		// make sure all entries have been added
		List<SimplestObject> list = persist.getObjectsMatching(new SimplestObject());
		assertEquals(200, list.size());
		// select using sorting
		SimplestObject orderObject = new SimplestObject();
		orderObject.setFoo(0.0);
		list = persist.getObjects(SimplestObject.class, new Descending(orderObject));
		assertEquals(200, list.size());
		assertEquals((Double) 199.0, list.get(0).getFoo());
		list = persist.getObjects(SimplestObject.class, new Ascending(orderObject));
		assertEquals(200, list.size());
		assertEquals((Double) 0.0, list.get(0).getFoo());

		persist.close();
	}

	/**
	 * Test sorting complex objects
	 * 
	 * @throws Exception
	 */
	@Test
	public void testSortingComplex() throws Exception
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		ConnectionWrapper cw = persist.getConnectionWrapper();
		// add a large number of entries
		for (int x = 0; x < 200; x++)
		{
			SimplestObject so = new SimplestObject();
			so.setFoo((double) x);
			ComplexObject co = new ComplexObject();
			co.setSimplestObject(so);
			persist.saveObject(cw, co);
		}
		cw.commitAndDiscard();
		// make sure all entries have been added
		List<ComplexObject> list = persist.getObjectsMatching(new ComplexObject());
		assertEquals(200, list.size());
		// select using sorting
		SimplestObject simpleObject = new SimplestObject();
		simpleObject.setFoo(0.0);
		ComplexObject orderObject = new ComplexObject();
		orderObject.setSimplestObject(simpleObject);

		list = persist.getObjects(ComplexObject.class, new Descending(orderObject));
		assertEquals(200, list.size());
		assertEquals((Double) 199.0, list.get(0).getSimplestObject().getFoo());
		list = persist.getObjects(ComplexObject.class, new Ascending(orderObject));
		assertEquals(200, list.size());
		assertEquals((Double) 0.0, list.get(0).getSimplestObject().getFoo());

		persist.close();

	}

	/**
	 * Test limiting simple objects.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testLimitSimple() throws Exception
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		ConnectionWrapper cw = persist.getConnectionWrapper();
		// add a large number of entries
		for (int x = 0; x < 200; x++)
		{
			SimplestObject so = new SimplestObject();
			so.setFoo((double) x);
			persist.saveObject(cw, so);
		}
		cw.commitAndDiscard();
		// make sure all entries have been added
		List<SimplestObject> list = persist.getObjectsMatching(new SimplestObject());
		assertEquals(200, list.size());
		// select using sorting
		SimplestObject orderObject = new SimplestObject();
		orderObject.setFoo(0.0);
		list = persist.getObjects(SimplestObject.class, new Order(20, 10, new Ascending(orderObject)));
		assertEquals(20, list.size());
		assertEquals((Double) 10.0, list.get(0).getFoo());
		list = persist.getObjects(SimplestObject.class, new Order(45, new Ascending(orderObject)));
		assertEquals(45, list.size());
		assertEquals((Double) 0.0, list.get(0).getFoo());

		persist.close();
	}

	/**
	 * Test AND/OR nesting.
	 * 
	 */
	@Test
	public void testComplexQuery() throws Exception
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		ConnectionWrapper cw = persist.getConnectionWrapper();
		// add a large number of entries
		for (int x = 1; x <= 100; x++)
		{
			SimplestObject so = new SimplestObject();
			so.setFoo((double) x);
			persist.saveObject(cw, so);
		}
		cw.commitAndDiscard();
		persist.close();
		persist = new PersistenceManager(driver, database, login, password);
		// construct a query that returns the objects with where foo is 3,66 and
		// 99
		SimplestObject different = new SimplestObject(100.0);
		SimplestObject larger = new SimplestObject(98.0);
		SimplestObject equal = new SimplestObject(66.0);
		SimplestObject smallerOrEquals = new SimplestObject(3.0);
		SimplestObject largerOrEquals = new SimplestObject(3.0);

		And one = new And(new GreaterOrEqual(largerOrEquals), new LessOrEqual(smallerOrEquals));
		And two = new And(new Different(different), new Greater(larger));
		List<SimplestObject> list = persist.getObjects(SimplestObject.class, new Or(one, two, new Equal(equal)));
		assertEquals(3, list.size());
		list = persist.getObjects(SimplestObject.class, new Or(one, two, new Equal(equal)), new Ascending(different));
		assertEquals(3, list.size());
		assertEquals(3.0, (double) list.get(0).getFoo(), 0.0001);
		assertEquals(66.0, (double) list.get(1).getFoo(), 0.0001);
		assertEquals(99.0, (double) list.get(2).getFoo(), 0.0001);

		persist.close();
	}

	/**
	 * Simple test on book/author example.
	 * 
	 */
	@Test
	public void testAuthorBookSimple() throws Exception
	{
		// create one author
		Author asimov = new Author();
		asimov.setBirthYear(1920);
		asimov.setFirstName("Isaac");
		asimov.setLastName("Asimov");

		// create some books
		Book foundation = new Book("Foundation");
		foundation.setPublishedYear(1951);
		foundation.addKeyWord("science fiction");
		foundation.addKeyWord("psychohistory");
		asimov.addBook(foundation);
		Book cavesofsteel = new Book("The Caves of Steel");
		cavesofsteel.setPublishedYear(1954);
		cavesofsteel.addKeyWord("science fiction");
		cavesofsteel.addKeyWord("robot");
		cavesofsteel.addKeyWord("crime");
		asimov.addBook(cavesofsteel);

		// save everything
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		persist.saveObject(asimov);
		persist.close();

		// open a new connection
		persist = new PersistenceManager(driver, database, login, password);

		// find all books with the authors last name 'Asimov'.
		asimov = new Author();
		asimov.setLastName("Asimov");
		Book seekBook = new Book();
		seekBook.addAuthor(asimov);
		List<Book> asimovBooks = persist.getObjects(Book.class, new Equal(seekBook));
		assertEquals(2, asimovBooks.size());
		// print the title of the scifi books
		for (Book book : asimovBooks)
		{
			System.out.println(book.getTitle());
		}

		persist.close();
	}

	private void createAuthors() throws SQLException
	{

		// create some authors
		Author dickens = new Author();
		dickens.setBirthYear(1812);
		dickens.setFirstName("Charles");
		dickens.setLastName("Dickens");
		Author asimov = new Author();
		asimov.setBirthYear(1920);
		asimov.setFirstName("Isaac");
		asimov.setLastName("Asimov");
		Author verne = new Author();
		verne.setBirthYear(1828);
		verne.setFirstName("Jules");
		verne.setLastName("Verne");

		// create some books
		Book twocities = new Book("A Tale of Two Cities");
		twocities.setPublishedYear(1859);
		twocities.addKeyWord("London");
		twocities.addKeyWord("Paris");
		twocities.addKeyWord("revolution");
		dickens.addBook(twocities);
		Book olivertwist = new Book("Oliver Twist");
		olivertwist.setPublishedYear(1838);
		olivertwist.addKeyWord("crime");
		olivertwist.addKeyWord("London");
		dickens.addBook(olivertwist);
		Book foundation = new Book("Foundation");
		foundation.setPublishedYear(1951);
		foundation.addKeyWord("science fiction");
		foundation.addKeyWord("psychohistory");
		asimov.addBook(foundation);
		Book cavesofsteel = new Book("The Caves of Steel");
		cavesofsteel.setPublishedYear(1954);
		cavesofsteel.addKeyWord("science fiction");
		cavesofsteel.addKeyWord("robot");
		cavesofsteel.addKeyWord("crime");
		asimov.addBook(cavesofsteel);
		Book centerofearth = new Book("A Journey to the Center of the Earth");
		centerofearth.setPublishedYear(1864);
		centerofearth.addKeyWord("science fiction");
		centerofearth.addKeyWord("cave");
		verne.addBook(centerofearth);
		Book leagues = new Book("Twenty Thousand Leagues under the Sea");
		leagues.setPublishedYear(1869);
		leagues.addKeyWord("science fiction");
		leagues.addKeyWord("submarine");
		leagues.addKeyWord("ocean");
		verne.addBook(leagues);

		// save everything
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		persist.saveObject(dickens);
		persist.saveObject(asimov);
		persist.saveObject(verne);
		persist.close();
	}

	/**
	 * Test the simplified book/author example.
	 */
	@Test
	public void testAuthorBookExample1() throws Exception
	{
		this.createAuthors();
		// open a new connection
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);

		// find all science fiction books
		Book seekBook = new Book();
		seekBook.addKeyWord("science fiction");
		List<Book> scifiBooks = persist.getObjects(Book.class, new Equal(seekBook));
		assertEquals(4, scifiBooks.size());
		// print the title of the scifi books
		for (Book book : scifiBooks)
		{
			System.out.println(book.getTitle());
		}

		// find all authors with the first name 'Isaac'.
		Author asimov = new Author();
		asimov.setFirstName("Isaac");
		List<Author> asimovAuthors = persist.getObjects(Author.class, new Equal(asimov));
		assertEquals(1, asimovAuthors.size());

		persist.close();
	}

	/**
	 * Test saving/searching/loading an object that contains itself.
	 */
	@Test
	public void testSelfContaining() throws Exception
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		SelfContainingObject sco = new SelfContainingObject();
		sco.setSelf(sco);
		persist.saveObject(sco);

		persist = new PersistenceManager(driver, database, login, password);
		List<SelfContainingObject> scos = persist.getObjects(SelfContainingObject.class, new All());
		assertEquals(1, scos.size());
		assertNotNull(scos.get(0).getSelf());

		Layer1 l1 = new Layer1();
		l1.setLayer2(new Layer2());
		l1.getLayer2().setLayer3(new Layer3());
		l1.getLayer2().getLayer3().setLayer1(l1);
		persist.saveObject(l1);
		persist.close();

		persist = new PersistenceManager(driver, database, login, password);

		// it should not be possible to delete l2, since it is part of l1.
		List<Layer2> l2s = persist.getObjects(Layer2.class, new All());
		assertEquals(1, l2s.size());
		persist.deleteObjects(Layer2.class, new All());
		l2s = persist.getObjects(Layer2.class, new All());
		assertEquals(1, l2s.size());
		// it should not be possible to delete l3, since it is part of l2.
		List<Layer3> l3s = persist.getObjects(Layer3.class, new All());
		assertEquals(1, l3s.size());
		persist.deleteObjects(Layer3.class, new All());
		l3s = persist.getObjects(Layer3.class, new All());
		assertEquals(1, l3s.size());
		// it should be possible to delete l1.
		List<Layer1> l1s = persist.getObjects(Layer1.class, new All());
		assertEquals(1, l1s.size());
		persist.deleteObjects(Layer1.class, new All());
		l1s = persist.getObjects(Layer1.class, new All());
		assertEquals(0, l1s.size());
		// deleting l1 should remove l2 and l3.
		l3s = persist.getObjects(Layer3.class, new All());
		assertEquals(0, l3s.size());
		l2s = persist.getObjects(Layer2.class, new All());
		assertEquals(0, l2s.size());

		persist.close();

	}

	/**
	 * Test deletion of self-containing objects.
	 */
	@Test
	public void testDeleteSelfContaining() throws Exception
	{

		PersistenceManager persist = new PersistenceManager(driver, database, login, password);

		Layer1 l1 = new Layer1();
		l1.setName("foo bar");
		l1.setLayer2(new Layer2());
		l1.getLayer2().setLayer3(new Layer3());
		l1.getLayer2().getLayer3().setLayer1(l1);
		persist.saveObject(l1);
		persist.saveObject(l1.getLayer2());
		persist.close();

		persist = new PersistenceManager(driver, database, login, password);
		// delete all self-containing objects
		persist.deleteObjects(Layer1.class, new All());
		persist.deleteObjects(Layer2.class, new All());
		List<Layer2> objects = persist.getObjects(Layer2.class, new All());

		// make sure no Layer2 objects remain
		assertEquals(0, objects.size());
	}

	/**
	 * Test if an object containing a reference to an abstract class returns
	 * instances of all objects of containing implementing classes.
	 * 
	 */
	@Test
	public void testAbstractMemberSearch() throws Exception
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		Foo foo = new Foo();
		ConcreteBar1 bar = new ConcreteBar1();
		bar.setAbstractName("abstract");
		bar.setConcreteName("concrete");
		foo.setBar(bar);
		persist.saveObject(foo);
		ConcreteBar2 bar2 = new ConcreteBar2();
		bar2.setAbstractName("f");
		bar2.setConcreteName("C");
		foo.setBar(bar2);
		persist.saveObject(foo);
		persist.close();

		persist = new PersistenceManager(driver, database, login, password);

		// assert that there are no ConcreteBar1 objects
		List<ConcreteBar1> cbar1s = persist.getObjects(ConcreteBar1.class, new All());
		assertEquals(0, cbar1s.size());

		// assert that there is one ConcreteBar2 object
		List<ConcreteBar2> cbar2s = persist.getObjects(ConcreteBar2.class, new All());
		assertEquals(1, cbar2s.size());

		// assert that there is one AbstractBar object
		List<AbstractBar> bars = persist.getObjects(AbstractBar.class, new All());
		assertEquals(1, bars.size());

		// create a Foo search object
		Foo searchFoo = new Foo();

		// assert that the Foo object can be retrieved with a ConcreteBar2
		// object.
		ConcreteBar2 cbar2 = new ConcreteBar2();
		cbar2.setAbstractName("f");
		cbar2.setConcreteName("C");
		searchFoo.setBar(cbar2);
		List<Foo> foos = persist.getObjects(Foo.class, new Equal(searchFoo));
		assertEquals(1, foos.size());

		// assert that the Foo object can be retrieved with a ConcreteBar1
		// object.
		ConcreteBar1 cbar1 = new ConcreteBar1();
		cbar1.setAbstractName("f");
		cbar1.setConcreteName("C");
		cbar1.setFooProperty("foo");
		searchFoo.setBar(cbar1);
		foos = persist.getObjects(Foo.class, new Equal(searchFoo, false));
		assertEquals(1, foos.size());

		// assert that the Foo object can not be retrieved with incorrect values
		cbar1.setAbstractName("fail");
		searchFoo.setBar(cbar1);
		foos = persist.getObjects(Foo.class, new Equal(searchFoo, false));
		assertEquals(0, foos.size());

		persist.close();
	}

	/**
	 * Test that checks if we can get implementors of interfaces and
	 * implementors of sub-interfaces based on a non-strict query.
	 * 
	 */
	@Test
	public void testInterfaceNonStrictQuery() throws Exception
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);

		// create two FooContainerOwner objects
		FooContainerOwner one = new FooContainerOwner();
		MyFooContainer fooer = new MyFooContainer();
		fooer.setFoo("oneFooer");
		one.setFooContainer(fooer);

		FooContainerOwner two = new FooContainerOwner();
		MyExtendedFooContainer extfooer = new MyExtendedFooContainer();
		extfooer.setFoo("twoFooer");
		two.setFooContainer(extfooer);

		// save FooContainerOwner objects.
		persist.saveObject(one);
		persist.saveObject(two);
		persist.close();

		// re-open connection
		persist = new PersistenceManager(driver, database, login, password);

		// check that there are two FooContainerOwners
		List<FooContainerOwner> fooableowners = persist.getObjects(FooContainerOwner.class, new All());
		assertEquals(2, fooableowners.size());

		// check that there are two FooContainers
		List<FooContainer> fooables = persist.getObjects(FooContainer.class, new All());
		assertEquals(2, fooables.size());
		// check that there is one ExtendedFooContainer
		List<ExtendedFooContainer> extendedfooables = persist.getObjects(ExtendedFooContainer.class, new All());
		assertEquals(1, extendedfooables.size());

		FooContainerOwner searchObject = new FooContainerOwner();

		// get the first foo based on strict search
		fooer = new MyFooContainer();
		fooer.setFoo("oneFooer");
		searchObject.setFooContainer(fooer);
		fooableowners = persist.getObjects(FooContainerOwner.class, new Equal(searchObject));
		assertEquals(1, fooableowners.size());

		// get second foo based on strict search
		extfooer = new MyExtendedFooContainer();
		extfooer.setFoo("twoFooer");
		searchObject.setFooContainer(extfooer);
		fooableowners = persist.getObjects(FooContainerOwner.class, new Equal(searchObject));
		assertEquals(1, fooableowners.size());

		// check that second foo can't be gotten by strict search
		fooer.setFoo("twoFooer");
		searchObject.setFooContainer(fooer);
		fooableowners = persist.getObjects(FooContainerOwner.class, new Equal(searchObject));
		assertEquals(0, fooableowners.size());

		// get second foo based on non-strict search
		fooableowners = persist.getObjects(FooContainerOwner.class, new Equal(searchObject, false));
		assertEquals(1, fooableowners.size());

		// check that the first foo can't be gotten by strict search
		extfooer.setFoo("oneFooer");
		searchObject.setFooContainer(extfooer);
		fooableowners = persist.getObjects(FooContainerOwner.class, new Equal(searchObject));
		assertEquals(0, fooableowners.size());

		// get first foo based on non-strict search
		fooableowners = persist.getObjects(FooContainerOwner.class, new Equal(searchObject, false));
		assertEquals(1, fooableowners.size());

		persist.close();
	}

	/**
	 * Test if we can remove tables.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testDropTable() throws Exception
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		// drop all tables
		persist.dropTable(Object.class);
		persist.close();
	}

	/**
	 * Check if altering an object in one instance of Conserve and refreshing it
	 * from another instance works.
	 * 
	 */
	@Test
	public void testRefresh() throws Exception
	{
		PersistenceManager persistOne = new PersistenceManager(driver, database, login, password);
		PersistenceManager persistTwo = new PersistenceManager(driver, database, login, password);
		// create a simple object
		SimpleObject so = new SimpleObject();
		so.setName("foo bar");

		// save the object
		persistOne.saveObject(so);

		// load the object from another instance
		List<SimpleObject> simpleObjects = persistTwo.getObjects(SimpleObject.class, new All());
		SimpleObject copy = simpleObjects.get(0);

		// alter the original object
		so.setName("altered");
		// save the altered object
		persistOne.saveObject(so);
		// update the copy
		SimpleObject copy2 = persistTwo.refresh(copy);
		// make sure the change has propagated
		assertEquals(so.getName(), copy.getName());
		assertEquals(copy.getName(), copy2.getName());
		assertEquals(copy, copy2);
	}

	/**
	 * Check if self-referencing objects are correctly updated.
	 * 
	 */
	@Test
	public void testRefreshSelfReferencing() throws Exception
	{

		PersistenceManager persistOne = new PersistenceManager(driver, database, login, password);
		PersistenceManager persistTwo = new PersistenceManager(driver, database, login, password);
		// create recursive objects
		Layer1 layer1 = new Layer1();
		layer1.setName("foo bar");
		Layer2 layer2 = new Layer2();
		Layer3 layer3 = new Layer3();
		// connect objects
		layer1.setLayer2(layer2);
		layer2.setLayer3(layer3);
		layer3.setLayer1(layer1);

		// save the objects
		persistOne.saveObject(layer1);

		// load the object from another instance
		List<Layer3> layer3s = persistTwo.getObjects(Layer3.class, new All());
		Layer3 copy = layer3s.get(0);

		// alter the original object
		layer3.getLayer1().setName("altered");
		// save the altered object
		persistOne.saveObject(layer3);
		// update the copy
		persistTwo.refresh(copy);
		// make sure the change has propagated
		assertEquals(layer3.getLayer1().getName(), copy.getLayer1().getName());
	}

	/**
	 * Test duplication from one database to the next.
	 * 
	 */
	@Test
	public void testDuplicate() throws Exception
	{
		// create two persistence managers
		PersistenceManager persistOne = new PersistenceManager(driver, database, login, password);
		if (secondDatabase == null)
		{
			secondDatabase = database + "2";
		}
		PersistenceManager persistTwo = new PersistenceManager(driver, secondDatabase, login, password);

		// drop all tables in both
		persistOne.dropTable(Object.class);
		persistTwo.dropTable(Object.class);
		// create some objects in the first database
		ComplexObject co = new ComplexObject();
		co.setData(new double[] { 1, 2, 3 });
		co.setSimplestObject(new SimplestObject(5.0));
		persistOne.saveObject(co);
		LessSimpleObject lso = new LessSimpleObject();
		lso.setAge(50L);
		lso.setName("name");
		lso.setValue(6);
		persistOne.saveObject(lso);

		// peform a copy
		persistOne.duplicate(persistTwo);
		// close both persistence managers
		persistOne.close();
		persistTwo.close();

		// re-open the target persistence manager
		persistTwo = new PersistenceManager(driver, secondDatabase, login, password);
		// assert that there is 3 objects in all
		List<Object> allObjects = persistTwo.getObjects(Object.class, new All());
		assertEquals(3, allObjects.size());
		// assert that there is one LessSimpleObject
		List<LessSimpleObject> lessSimpleObjects = persistTwo.getObjects(LessSimpleObject.class, new All());
		assertEquals(1, lessSimpleObjects.size());
		// assert that the LessSimpleObject has "name" as name.
		LessSimpleObject first = lessSimpleObjects.get(0);
		assertEquals("name", first.getName());
	}

	/**
	 * Test saving/loading Calendar objects.
	 * 
	 */
	@Test
	public void testCalendar() throws Exception
	{

		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		GregorianCalendar cal = new GregorianCalendar(1999, 9, 9);

		// GregorianCalendar will throw exception if some fields are accessed
		// before others
		Date d = cal.getTime();
		System.out.println(d);

		// save the calendar
		persist.saveObject(cal);

		List<Calendar> cals = persist.getObjects(Calendar.class, new All());

		// verify that we have at least one calendar stored
		assertTrue(cals.size() > 0);

		persist.close();
	}

	/**
	 * Test renaming tables.
	 */
	@Test
	public void testRenameTable() throws Exception
	{
		String name = "foo";
		int value = 42;
		Object otherObject = new SimpleObject();
		Object redundantObject = new SimplestObject();
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);

		// create and store a new value
		OriginalObject oo = new OriginalObject();
		oo.setName(name);
		oo.setValue(value);
		oo.setOtherObject(otherObject);
		oo.setRedundantObject(redundantObject);
		pm.saveObject(oo);
		// rename the table
		pm.changeName(OriginalObject.class, NewName.class);
		// get all NewName objects
		List<NewName> res = pm.getObjects(NewName.class, new All());
		assertEquals(res.size(), 1);
		// make sure all properties match
		NewName nn = res.get(0);
		assertEquals(nn.getValue(), value);
		assertEquals(nn.getName(), name);
		assertEquals(nn.getOtherObject(), otherObject);
		assertEquals(nn.getRedundantObject(), redundantObject);
		pm.close();

	}

	/**
	 * Test renaming columns.
	 */
	@Test
	public void testRenameColumn() throws Exception
	{
		String name = "foo";
		int value = 42;
		Object otherObject = new SimpleObject();
		Object redundantObject = new SimplestObject();
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);

		// create and store a new value
		OriginalObject oo = new OriginalObject();
		oo.setName(name);
		oo.setValue(value);
		oo.setOtherObject(otherObject);
		oo.setRedundantObject(redundantObject);
		pm.saveObject(oo);
		// rename the table
		pm.changeName(OriginalObject.class, RenamedColumn.class);
		// update schema
		pm.renameProperties(RenamedColumn.class);

		// get all RenamedColumn objects
		List<RenamedColumn> res1 = pm.getObjects(RenamedColumn.class, new All());
		assertEquals(res1.size(), 1);
		// make sure all properties match
		RenamedColumn nn = res1.get(0);
		assertEquals(value, nn.getValue());
		assertEquals(name, nn.getName());
		assertEquals(otherObject, nn.getOtherObject());
		assertEquals(redundantObject, nn.getRenamedObject());

		// change everything back
		pm.changeName(RenamedColumn.class, OriginalObject.class);
		pm.renameProperties(OriginalObject.class);

		// get all OriginalObject objects
		List<OriginalObject> res2 = pm.getObjects(OriginalObject.class, new All());
		assertEquals(res2.size(), 1);
		// make sure all properties match
		oo = res2.get(0);
		assertEquals(value, oo.getValue());
		assertEquals(name, oo.getName());
		assertEquals(otherObject, oo.getOtherObject());
		// make sure the column that has been renamed is preserved
		assertEquals(redundantObject, oo.getRedundantObject());

		pm.close();
	}

	/**
	 * Test adding and removing columns.
	 */
	@Test
	public void testAddRemoveColumns() throws Exception
	{
		String name = "foo";
		int value = 42;
		Object otherObject = new SimpleObject();
		Object redundantObject = new SimplestObject();
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);
		
		// create and store a new value
		OriginalObject oo = new OriginalObject();
		oo.setName(name);
		oo.setValue(value);
		oo.setOtherObject(otherObject);
		oo.setRedundantObject(redundantObject);
		pm.saveObject(oo);
		// rename the table
		pm.changeName(OriginalObject.class, RemovedColumn.class);
		// update schema
		pm.updateSchema(RemovedColumn.class);

		// get all RemovedColumn objects
		List<RemovedColumn> res1 = pm.getObjects(RemovedColumn.class, new All());
		assertEquals(res1.size(), 1);
		// make sure all properties match
		RemovedColumn nn = res1.get(0);
		assertEquals(value, nn.getValue());
		assertEquals(otherObject, nn.getOtherObject());
		assertEquals(name, nn.getName());

		// make sure no SimplestObject entries are left
		List<SimplestObject> simpleRes = pm.getObjects(SimplestObject.class, new All());
		assertEquals(0, simpleRes.size());

		// change everything back
		pm.changeName(RemovedColumn.class, OriginalObject.class);
		pm.updateSchema(OriginalObject.class);

		// get all OriginalObject objects
		List<OriginalObject> res2 = pm.getObjects(OriginalObject.class, new All());
		assertEquals(res2.size(), 1);
		// make sure all properties match
		oo = res2.get(0);
		assertEquals(value, oo.getValue());
		assertEquals(name, oo.getName());
		assertEquals(otherObject, oo.getOtherObject());
		// make sure the column that has been added/removed is now null
		assertNull(oo.getRedundantObject());

		pm.close();
	}

	/**
	 * Test changing the type of a column. Long <-> Object.
	 */
	@Test
	public void testChangeColumnTypeLongAndObject() throws Exception
	{
		String name = "foo";
		int value = 1;
		Object otherObject = new SimpleObject();
		Object redundantObject = new SimplestObject();
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);

		// create original objects
		OriginalObject oo = new OriginalObject();
		oo.setName(name);
		oo.setValue(value);
		oo.setOtherObject(otherObject);
		oo.setRedundantObject(redundantObject);
		pm.saveObject(oo);
		pm.close();

		// change object to long
		pm = new PersistenceManager(driver, database, login, password);
		pm.changeName(OriginalObject.class, ObjectToLong.class);
		pm.close();
		pm = new PersistenceManager(driver, database, login, password);
		pm.updateSchema(ObjectToLong.class);
		pm.close();
		//check that objects exist/not exist
		pm = new PersistenceManager(driver, database, login, password);
		List<ObjectToLong>res = pm.getObjects(ObjectToLong.class, new All());
		assertEquals(1,res.size());
		ObjectToLong tmp = res.get(0);
		assertTrue(tmp.getName().equals(name));
		assertNull(tmp.getOtherObject());
		tmp.setOtherObject(42L);
		pm.saveObject(tmp);
		//check that the dependent object has been dropped
		List<SimpleObject>dependents = pm.getObjects(SimpleObject.class, new All());
		assertEquals(0,dependents.size());
		pm.close();
		// change long to object
		pm = new PersistenceManager(driver, database, login, password);
		pm.changeName(ObjectToLong.class, OriginalObject.class);
		pm.close();
		pm = new PersistenceManager(driver, database, login, password);
		pm.updateSchema(OriginalObject.class);
		pm.close();
		pm = new PersistenceManager(driver, database, login, password);
		List<OriginalObject>res1 = pm.getObjects(OriginalObject.class, new All());
		assertEquals(1,res1.size());
		OriginalObject tmp1 = res1.get(0);
		assertNull(tmp1.getOtherObject());
		tmp1.setOtherObject(new SimpleObject());
		pm.saveObject(tmp1);
		pm.close();
	}
	
	/**
	 * Test changing the type of a column. Object <-> subclass.
	 */
	@Test
	public void testChangeColumnTypeObjectAndSubclass() throws Exception
	{
		String name = "foo";
		Object object = new Object();
		Object subObject = new SimplestObject();
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);

		// create original objects
		OriginalObject oo1 = new OriginalObject();
		oo1.setName(name);
		oo1.setValue(1);
		oo1.setOtherObject(object);
		pm.saveObject(oo1);
		OriginalObject oo2 = new OriginalObject();
		oo2.setName(name);
		oo2.setValue(2);
		oo2.setOtherObject(subObject);
		pm.saveObject(oo2);
		pm.close();

		// change object to subclass
		pm = new PersistenceManager(driver, database, login, password);
		pm.changeName(OriginalObject.class, ObjectToSubclass.class);
		pm.close();
		pm = new PersistenceManager(driver, database, login, password);
		pm.updateSchema(ObjectToSubclass.class);
		pm.close();
		//check that objects exist/not exist
		pm = new PersistenceManager(driver, database, login, password);
		ObjectToSubclass src = new ObjectToSubclass();
		src.setValue(1);
		List<ObjectToSubclass>res = pm.getObjects(ObjectToSubclass.class, new Equal(src));
		assertEquals(1,res.size());
		ObjectToSubclass obj = res.get(0);
		assertTrue(obj.getName().equals(name));
		assertNull(obj.getOtherObject());
		src.setValue(2);
		res = pm.getObjects(ObjectToSubclass.class, new Equal(src));
		assertEquals(1,res.size());
		obj = res.get(0);
		assertTrue(obj.getName().equals(name));
		assertNotNull(obj.getOtherObject());
		
		//change subclass to superclass
		pm = new PersistenceManager(driver, database, login, password);
		pm.changeName(ObjectToSubclass.class, OriginalObject.class);
		pm.close();
		pm = new PersistenceManager(driver, database, login, password);
		pm.updateSchema(OriginalObject.class);
		List<OriginalObject>res2 = pm.getObjects(OriginalObject.class, new All());
		assertEquals(2,res2.size());
		pm.close();
	}
	
	/**
	 * Test changing the type of a column. int <-> long.
	 */
	@Test
	public void testChangeColumnTypeLongAndInt() throws Exception
	{
		String name = "foo";
		int value = 1;
		Object otherObject = new SimpleObject();
		Object redundantObject = new SimplestObject();
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);

		// create original objects
		OriginalObject oo = new OriginalObject();
		oo.setName(name);
		oo.setValue(value);
		oo.setOtherObject(otherObject);
		oo.setRedundantObject(redundantObject);
		pm.saveObject(oo);
		pm.close();

		// change int to long
		pm = new PersistenceManager(driver, database, login, password);
		pm.changeName(OriginalObject.class, IntToLong.class);
		pm.close();
		pm = new PersistenceManager(driver, database, login, password);
		pm.updateSchema(IntToLong.class);
		pm.close();
		//check that the integer has been changed into a long
		pm = new PersistenceManager(driver, database, login, password);
		List<IntToLong>res1 = pm.getObjects(IntToLong.class,new All());
		assertEquals(1,res1.size());
		IntToLong obj1 = res1.get(0);
		assertTrue(obj1.getName().equals(name));
		assertEquals(1,obj1.getValue());
		pm.close();
		
		//change long to int
		pm = new PersistenceManager(driver, database, login, password);
		pm.changeName(IntToLong.class, OriginalObject.class);
		pm.close();
		pm = new PersistenceManager(driver, database, login, password);
		pm.updateSchema(OriginalObject.class);
		pm.close();
		//check that the long is now null, since long can not fit in int
		pm = new PersistenceManager(driver, database, login, password);
		List<OriginalObject>res2 = pm.getObjects(OriginalObject.class,new All());
		assertEquals(1,res2.size());
		OriginalObject obj2 = res2.get(0);
		assertEquals(0,obj2.getValue());
		pm.close();
		
	}

	/**
	 * Test changing the type of a column. string <-> long.
	 */
	@Test
	public void testChangeColumnTypeLongAndString() throws Exception
	{
		String name = "foo";
		int value = 1;
		Object otherObject = new SimpleObject();
		Object redundantObject = new SimplestObject();
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);

		// create original objects
		OriginalObject oo = new OriginalObject();
		oo.setName(name);
		oo.setValue(value);
		oo.setOtherObject(otherObject);
		oo.setRedundantObject(redundantObject);
		pm.saveObject(oo);
		pm.close();

		// change string to long
		pm = new PersistenceManager(driver, database, login, password);
		pm.changeName(OriginalObject.class, StringToLong.class);
		pm.close();
		pm = new PersistenceManager(driver, database, login, password);
		pm.updateSchema(StringToLong.class);
		pm.close();
		//check that the long no longer exist
		pm = new PersistenceManager(driver, database, login, password);
		List<StringToLong>res1 = pm.getObjects(StringToLong.class, new All());
		assertEquals(1,res1.size());
		StringToLong obj1 = res1.get(0);
		assertNull(obj1.getName());
		//save a long
		obj1.setName(1L);
		pm.saveObject(obj1);
		pm.close();

		// change long to string
		pm = new PersistenceManager(driver, database, login, password);
		pm.changeName(StringToLong.class, OriginalObject.class);
		pm.close();
		pm = new PersistenceManager(driver, database, login, password);
		pm.updateSchema(OriginalObject.class);
		pm.close();
		// check that the string no longer exist
		pm = new PersistenceManager(driver, database, login, password);
		List<OriginalObject>res2 = pm.getObjects(OriginalObject.class, new All());
		assertEquals(1,res2.size());
		OriginalObject obj2 = res2.get(0);
		assertNull(obj2.getName());
		pm.close();
	}
	/**
	 * Test changing the type of a column. array <-> long.
	 */
	@Test
	public void testChangeColumnTypeLongAndArray() throws Exception
	{
		String name = "foo";
		int value = 1;
		Object otherObject = new SimpleObject();
		Object redundantObject = new SimplestObject();
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);

		// create original objects
		OriginalObject oo = new OriginalObject();
		oo.setName(name);
		oo.setValue(value);
		oo.setOtherObject(otherObject);
		oo.setRedundantObject(redundantObject);
		oo.setArray(new long[]{8,9,10,11});
		pm.saveObject(oo);
		pm.close();

		// change array to long
		pm = new PersistenceManager(driver, database, login, password);
		pm.changeName(OriginalObject.class, ArrayToLong.class);
		pm.close();
		pm = new PersistenceManager(driver, database, login, password);
		pm.updateSchema(ArrayToLong.class);
		pm.close();
		//check that array no longer exist
		pm = new PersistenceManager(driver, database, login, password);
		List<ArrayToLong>res1 = pm.getObjects(ArrayToLong.class, new All());
		assertEquals(1,res1.size());
		ArrayToLong obj1 = res1.get(0);
		assertEquals(0,obj1.getArray());
		//save a long
		obj1.setArray(1);
		pm.saveObject(obj1);
		pm.close();

		//change long to array
		pm = new PersistenceManager(driver, database, login, password);
		pm.changeName(ArrayToLong.class, OriginalObject.class);
		pm.close();
		pm = new PersistenceManager(driver, database, login, password);
		pm.updateSchema(OriginalObject.class);
		pm.close();
		//make sure the array is null
		pm = new PersistenceManager(driver, database, login, password);
		List<OriginalObject>res2 = pm.getObjects(OriginalObject.class, new All());
		assertEquals(1,res2.size());
		OriginalObject obj2  = res2.get(0);
		assertNull(obj2.getArray());
		pm.close();
	}

	/**
	 * Test changing OriginalObject into SubClass, then back again.
	 */
	@Test
	public void testChangeSubclassing() throws Exception
	{

		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);
		// create two OriginalObject, store them.
		pm.saveObject(new OriginalObject());
		pm.saveObject(new OriginalObject());
		// create two NotSubClass, store them
		NotSubClass ns = new NotSubClass();
		ns.setName("foo");
		pm.saveObject(ns);
		ns = new NotSubClass();
		ns.setName("bar");
		pm.saveObject(ns);
		pm.close();
		
		pm = new PersistenceManager(driver, database, login, password);
		// rename NotSubClass to SubClass
		pm.changeName(NotSubClass.class, SubClass.class);
		pm.close();
		
		pm = new PersistenceManager(driver, database, login, password);
		// change the database schema
		pm.updateSchema(SubClass.class);

		// search all SubClass, make sure both objects are returned
		List<SubClass> res2 = pm.getObjects(SubClass.class, new All());
		assertEquals(2, res2.size());

		// search all OriginalObject, make sure all objects are returned.
		List<OriginalObject> res3 = pm.getObjects(OriginalObject.class, new All());
		assertEquals(4, res3.size());

		// search all OriginalObject with name "foo", make sure SubClass
		// objects are returned.
		OriginalObject searchObject = new OriginalObject();
		searchObject.setName("foo");
		res3 = pm.getObjects(OriginalObject.class, new Equal(searchObject));
		assertEquals(1, res3.size());
		for (OriginalObject oo : res3)
		{
			assertTrue(oo instanceof SubClass);
		}

		// rename SubClass to to NotSubClass
		pm.changeName(SubClass.class, NotSubClass.class);
		pm.close();
		// change the database schema
		pm = new PersistenceManager(driver, database, login, password);
		pm.updateSchema(NotSubClass.class);

		// search all OriginalObject, make sure both objects are returned.
		res3 = pm.getObjects(OriginalObject.class, new All());
		assertEquals(2, res3.size());
		// search all NotSubClass, make sure both objects are returned
		List<NotSubClass> res1 = pm.getObjects(NotSubClass.class, new All());
		assertEquals(2, res1.size());
		// search NotSubClass, make sure only matchign object is returned
		NotSubClass searchObject2 = new NotSubClass();
		searchObject2.setName("foo");
		res1 = pm.getObjects(NotSubClass.class, new Equal(searchObject2));
		assertEquals(1, res1.size());

		pm.close();

		// TODO: Test changing inheritance of an object that is a property of
		// another object

	}

	/**
	 * Test changing inheritance model.
	 */
	@Test
	public void testChangeInheritance() throws Exception
	{
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);
		// create two OriginalObject, store them.
		pm.saveObject(new OriginalObject());
		pm.saveObject(new OriginalObject());
		// rename OriginalObject to ChangedInheritance
		pm.changeName(OriginalObject.class, ChangedInheritance.class);

		// change the database schema, adding interface Serialized.
		pm.updateSchema(ChangedInheritance.class);

		// search all Serializable, make sure both objects are returned
		List<Serializable> res1 = pm.getObjects(Serializable.class, new All());
		assertEquals(2, res1.size());
		// search all ChangedInheritance, make sure both objects are returned
		List<ChangedInheritance> res2 = pm.getObjects(ChangedInheritance.class, new All());
		assertEquals(2, res2.size());

		// search all OriginalObject, make sure no objects are returned.
		List<OriginalObject> res3 = pm.getObjects(OriginalObject.class, new All());
		assertEquals(0, res3.size());

		// rename ChangedInheritance to OriginalObject, thus removing the
		// interface
		pm.changeName(ChangedInheritance.class, OriginalObject.class);
		// change the database schema
		pm.updateSchema(OriginalObject.class);

		// search all Serializable, make sure no objects are returned
		res1 = pm.getObjects(Serializable.class, new All());
		assertEquals(0, res1.size());
		// search all ChangedInheritance, make sure no objects are returned.
		res2 = pm.getObjects(ChangedInheritance.class, new All());
		assertEquals(0, res2.size());
		// search all OriginalObject, make sure both objects are returned.
		res3 = pm.getObjects(OriginalObject.class, new All());
		assertEquals(2, res3.size());

		pm.close();
	}

	/**
	 * Test moving a property from a class to one of its subclasses.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCopyDown() throws Exception
	{
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);
		// add two Bottom objects.
		OriginalBottom b1 = new OriginalBottom();
		b1.setFoo(1);
		OriginalBottom b2 = new OriginalBottom();
		b2.setFoo(2);
		pm.saveObject(b1);
		pm.saveObject(b2);

		pm.changeName(OriginalTop.class, ModifiedTop.class);
		pm.changeName(OriginalMiddle.class, ModifiedMiddle.class);
		pm.changeName(OriginalBottom.class, ModifiedBottom.class);

		pm.updateSchema(ModifiedBottom.class);
		pm.close();

		// get all ModifiedBottom with foo==1
		pm = new PersistenceManager(driver, database, login, password);
		ModifiedBottom ot = new ModifiedBottom();
		ot.setFoo(1);
		List<ModifiedBottom> res = pm.getObjectsMatching(ot);
		assertEquals(1, res.size());
		pm.close();
	}

	/**
	 * Test moving a property from a class to one of its superclasses.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCopyUp() throws Exception
	{
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);
		// add two Bottom objects.
		ModifiedBottom b1 = new ModifiedBottom();
		b1.setFoo(1);
		ModifiedBottom b2 = new ModifiedBottom();
		b2.setFoo(2);
		pm.saveObject(b1);
		pm.saveObject(b2);

		pm.changeName(ModifiedTop.class, OriginalTop.class);
		pm.changeName(ModifiedMiddle.class, OriginalMiddle.class);
		pm.changeName(ModifiedBottom.class, OriginalBottom.class);

		pm.updateSchema(OriginalBottom.class);
		pm.close();

		// get all OriginalTop with foo==1
		pm = new PersistenceManager(driver, database, login, password);
		OriginalTop ot = new OriginalTop();
		ot.setFoo(1);
		List<OriginalTop> res = pm.getObjectsMatching(ot);
		assertEquals(1, res.size());
		pm.close();
	}

	@Test
	public void testListObjects() throws Exception
	{
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);

		ListContainingObject a = new ListContainingObject();
		a.addStr("1");
		a.addStr("2");
		a.addStr("3");
		a.addStr("4");
		pm.saveObject(a);
		ListContainingObject b = new ListContainingObject();
		b.addStr("5");
		b.addStr("6");
		b.addStr("7");
		b.addStr("8");
		pm.saveObject(b);
		pm.close();

		pm = new PersistenceManager(driver, database, login, password);

		// search
		List<ListContainingObject> res = pm.getObjects(ListContainingObject.class, new All());
		assertEquals(2, res.size());
		for (ListContainingObject r : res)
		{
			assertNotNull(r.getList());
			assertTrue(3 < r.getList().size());
			assertNotNull(r.getList().get(0));
		}
	}
}
