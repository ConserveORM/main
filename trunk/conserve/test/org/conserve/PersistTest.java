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
import org.conserve.aggregate.AggregateFunction;
import org.conserve.aggregate.Average;
import org.conserve.aggregate.Maximum;
import org.conserve.aggregate.Minimum;
import org.conserve.aggregate.Sum;
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
import org.conserve.objects.SubInterface;
import org.conserve.objects.polymorphism.AbstractBar;
import org.conserve.objects.polymorphism.ConcreteBar1;
import org.conserve.objects.polymorphism.ConcreteBar2;
import org.conserve.objects.polymorphism.ExtendedFooContainer;
import org.conserve.objects.polymorphism.ImplementerA;
import org.conserve.objects.polymorphism.ImplementerB;
import org.conserve.objects.polymorphism.MyExtendedFooContainer;
import org.conserve.objects.polymorphism.Foo;
import org.conserve.objects.polymorphism.FooContainer;
import org.conserve.objects.polymorphism.FooContainerOwner;
import org.conserve.objects.polymorphism.MyFooContainer;
import org.conserve.objects.recursive.Layer1;
import org.conserve.objects.recursive.Layer2;
import org.conserve.objects.recursive.Layer3;
import org.conserve.objects.schemaupdate.ChangedInheritance;
import org.conserve.objects.schemaupdate.ChangedInheritanceContainer;
import org.conserve.objects.schemaupdate.ContainerObject;
import org.conserve.objects.schemaupdate.NewName;
import org.conserve.objects.schemaupdate.NewNameContainer;
import org.conserve.objects.schemaupdate.NotSubClass;
import org.conserve.objects.schemaupdate.ObjectContainerObject;
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
	public void testDeleteObjects() throws Exception
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
		List<Object> allList = persist.getObjects(Object.class);
		assertEquals(4, allList.size());
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
		// add some data
		SimpleObject so = new SimpleObject();
		persist.saveObject(so);
		persist.close();

		// re-establish database connection
		persist = new PersistenceManager(driver, database, login, password);
		// drop all tables
		persist.dropTable(Object.class);
		persist.close();

		// re-establish database connection
		persist = new PersistenceManager(driver, database, login, password);
		// make sure no objects are in the database
		List<Object> resObjs = persist.getObjects(Object.class);
		assertEquals(0, resObjs.size());
		List<SimpleObject> resSimpObjs = persist.getObjects(SimpleObject.class);
		assertEquals(0, resSimpObjs.size());
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
		persistTwo.close();
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
		SimpleObject otherObject = new SimpleObject();
		otherObject.setCount(87);
		SimplestObject redundantObject = new SimplestObject();
		redundantObject.setFoo(88.0);
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
		pm.updateSchema(RenamedColumn.class);
		pm.close();

		// re-establish connection
		pm = new PersistenceManager(driver, database, login, password);
		// get all RenamedColumn objects
		List<RenamedColumn> res1 = pm.getObjects(RenamedColumn.class, new All());
		assertEquals(res1.size(), 1);
		// make sure all properties match
		RenamedColumn nn = res1.get(0);
		assertEquals(value, nn.getValue());
		assertEquals(name, nn.getName());
		assertEquals(otherObject.getCount(), ((SimpleObject) nn.getOtherObject()).getCount());
		assertEquals((double) redundantObject.getFoo(), (double) ((SimplestObject) nn.getRenamedObject()).getFoo(),
				0.000001);

		// change everything back
		pm.changeName(RenamedColumn.class, OriginalObject.class);
		pm.updateSchema(OriginalObject.class);
		pm.close();

		// re-establish database connection
		pm = new PersistenceManager(driver, database, login, password);

		// get all OriginalObject objects
		List<OriginalObject> res2 = pm.getObjects(OriginalObject.class, new All());
		assertEquals(res2.size(), 1);
		// make sure all properties match
		oo = res2.get(0);
		assertEquals(value, oo.getValue());
		assertEquals(name, oo.getName());
		assertEquals(otherObject.getCount(), ((SimpleObject) oo.getOtherObject()).getCount());
		// make sure the column that has been renamed is preserved
		assertEquals((double) redundantObject.getFoo(), (double) ((SimplestObject) oo.getRedundantObject()).getFoo(),
				0.000001);

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
		// check that objects exist/not exist
		pm = new PersistenceManager(driver, database, login, password);
		List<ObjectToLong> res = pm.getObjects(ObjectToLong.class, new All());
		assertEquals(1, res.size());
		ObjectToLong tmp = res.get(0);
		assertTrue(tmp.getName().equals(name));
		assertNull(tmp.getOtherObject());
		tmp.setOtherObject(42L);
		pm.saveObject(tmp);
		// check that the dependent object has been dropped
		List<SimpleObject> dependents = pm.getObjects(SimpleObject.class, new All());
		assertEquals(0, dependents.size());
		pm.close();
		// change long to object
		pm = new PersistenceManager(driver, database, login, password);
		pm.changeName(ObjectToLong.class, OriginalObject.class);
		pm.close();
		pm = new PersistenceManager(driver, database, login, password);
		pm.updateSchema(OriginalObject.class);
		pm.close();
		pm = new PersistenceManager(driver, database, login, password);
		List<OriginalObject> res1 = pm.getObjects(OriginalObject.class, new All());
		assertEquals(1, res1.size());
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
		// check that Object (oo1.otherObject) do not exist, and SimplestObject
		// (oo2.otherObject) still exists
		pm = new PersistenceManager(driver, database, login, password);
		ObjectToSubclass src = new ObjectToSubclass();
		// get oo1
		src.setValue(1);
		List<ObjectToSubclass> res = pm.getObjects(ObjectToSubclass.class, new Equal(src));
		assertEquals(1, res.size());
		ObjectToSubclass obj = res.get(0);
		assertTrue(obj.getName().equals(name));
		// make sure oo1.otherObject has been deleted, as it is not compatible
		// with SimplestObject
		assertNull(obj.getOtherObject());
		// get oo2
		src.setValue(2);
		res = pm.getObjects(ObjectToSubclass.class, new Equal(src));
		assertEquals(1, res.size());
		obj = res.get(0);
		assertTrue(obj.getName().equals(name));
		// make sure oo2.otherObject still exists, as SimplestObject is an
		// acceptable subclass
		assertNotNull(obj.getOtherObject());

		// change subclass to superclass
		pm = new PersistenceManager(driver, database, login, password);
		pm.changeName(ObjectToSubclass.class, OriginalObject.class);
		pm.close();
		pm = new PersistenceManager(driver, database, login, password);
		pm.updateSchema(OriginalObject.class);
		// verify that the object still exists
		List<OriginalObject> res2 = pm.getObjects(OriginalObject.class, new All());
		assertEquals(2, res2.size());
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
		// check that the integer has been changed into a long
		pm = new PersistenceManager(driver, database, login, password);
		List<IntToLong> res1 = pm.getObjects(IntToLong.class, new All());
		assertEquals(1, res1.size());
		IntToLong obj1 = res1.get(0);
		assertTrue(obj1.getName().equals(name));
		assertEquals(1, obj1.getValue());
		pm.close();

		// change long to int
		pm = new PersistenceManager(driver, database, login, password);
		pm.changeName(IntToLong.class, OriginalObject.class);
		pm.close();
		pm = new PersistenceManager(driver, database, login, password);
		pm.updateSchema(OriginalObject.class);
		pm.close();
		// check that the long is now null, since long can not fit in int
		pm = new PersistenceManager(driver, database, login, password);
		List<OriginalObject> res2 = pm.getObjects(OriginalObject.class, new All());
		assertEquals(1, res2.size());
		OriginalObject obj2 = res2.get(0);
		assertEquals(0, obj2.getValue());
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
		// check that the long no longer exist
		pm = new PersistenceManager(driver, database, login, password);
		List<StringToLong> res1 = pm.getObjects(StringToLong.class, new All());
		assertEquals(1, res1.size());
		StringToLong obj1 = res1.get(0);
		assertNull(obj1.getName());
		// save a long
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
		List<OriginalObject> res2 = pm.getObjects(OriginalObject.class, new All());
		assertEquals(1, res2.size());
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
		oo.setArray(new long[] { 8, 9, 10, 11 });
		pm.saveObject(oo);
		pm.close();

		// change array to long
		pm = new PersistenceManager(driver, database, login, password);
		pm.changeName(OriginalObject.class, ArrayToLong.class);
		pm.close();
		pm = new PersistenceManager(driver, database, login, password);
		pm.updateSchema(ArrayToLong.class);
		pm.close();
		// check that array no longer exist
		pm = new PersistenceManager(driver, database, login, password);
		List<ArrayToLong> res1 = pm.getObjects(ArrayToLong.class, new All());
		assertEquals(1, res1.size());
		ArrayToLong obj1 = res1.get(0);
		assertEquals(0, obj1.getArray());
		// save a long
		obj1.setArray(1);
		pm.saveObject(obj1);
		pm.close();

		// change long to array
		pm = new PersistenceManager(driver, database, login, password);
		pm.changeName(ArrayToLong.class, OriginalObject.class);
		pm.close();
		pm = new PersistenceManager(driver, database, login, password);
		pm.updateSchema(OriginalObject.class);
		pm.close();
		// make sure the array is null
		pm = new PersistenceManager(driver, database, login, password);
		List<OriginalObject> res2 = pm.getObjects(OriginalObject.class, new All());
		assertEquals(1, res2.size());
		OriginalObject obj2 = res2.get(0);
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
	}

	/**
	 * Change a property of an object from OriginalObject to NewName, then back
	 * again.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testChangeNameOfProperty() throws Exception
	{
		// connect to database
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);

		// create test objects
		OriginalObject oo = new OriginalObject();
		oo.setValue(76);
		ContainerObject one = new ContainerObject();
		ContainerObject two = new ContainerObject();
		one.setFoo(oo);

		// store test objects
		pm.saveObject(one);
		pm.saveObject(two);
		pm.close();

		// re-connect to database
		pm = new PersistenceManager(driver, database, login, password);
		// change the name of the property class
		pm.changeName(OriginalObject.class, NewName.class);
		pm.changeName(ContainerObject.class, NewNameContainer.class);
		pm.close();

		// re-connect to database
		pm = new PersistenceManager(driver, database, login, password);
		// update schema
		pm.updateSchema(NewName.class);
		pm.close();

		// re-connect to database
		pm = new PersistenceManager(driver, database, login, password);
		// make sure there is one NewName object
		List<NewName> nnList = pm.getObjects(NewName.class);
		assertEquals(1, nnList.size());
		// make sure there are two NewNameContainer objects
		List<NewNameContainer> ncList = pm.getObjects(NewNameContainer.class);
		assertEquals(2, ncList.size());
		// make sure one of the NewNameContainer objects contain the correct
		// NewName object.
		if (ncList.get(0).getFoo() != null)
		{
			assertEquals(76, ncList.get(0).getFoo().getValue());
		}
		else if (ncList.get(1).getFoo() != null)
		{
			assertEquals(76, ncList.get(1).getFoo().getValue());
		}
		else
		{
			fail("Reference not preserved.");
		}
		// store a new NewName object in the empty NewNameContainer object
		NewName nu = new NewName();
		nu.setValue(18);
		if (ncList.get(0).getFoo() == null)
		{
			ncList.get(0).setFoo(nu);
			pm.saveObject(ncList.get(0));
		}
		else if (ncList.get(1).getFoo() == null)
		{
			ncList.get(1).setFoo(nu);
			pm.saveObject(ncList.get(1));
		}
		pm.close();

		// re-connect to database
		pm = new PersistenceManager(driver, database, login, password);
		// make sure we can retrieve the NewName object we stored previously
		nnList = pm.getObjects(NewName.class);
		assertEquals(2, nnList.size());
		ncList = pm.getObjects(NewNameContainer.class);
		assertEquals(2, ncList.size());
		NewNameContainer nc1 = ncList.get(0);
		NewNameContainer nc2 = ncList.get(1);
		assertTrue((nc1.getFoo().getValue() == 18 && nc2.getFoo().getValue() == 76)
				|| (nc1.getFoo().getValue() == 76 && nc2.getFoo().getValue() == 18));
		pm.close();

	}

	/**
	 * Change a property of an object from a class implementing an interface to
	 * a class that does not implement that interface. Make sure objects are
	 * deleted or not as appropriate.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testChangeInterfaceOfProperty() throws Exception
	{
		// connect to database
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);

		// create test objects
		OriginalObject one = new OriginalObject();
		one.setValue(1);
		OriginalObject two = new OriginalObject();
		two.setValue(2);
		ChangedInheritance three = new ChangedInheritance();
		three.setValue(3);

		pm.saveObject(two);

		ObjectContainerObject coOne = new ObjectContainerObject();
		coOne.setFoo(one);
		ObjectContainerObject coTwo = new ObjectContainerObject();
		coTwo.setFoo(two);
		ObjectContainerObject coThree = new ObjectContainerObject();
		coThree.setFoo(three);

		pm.saveObject(coOne);
		pm.saveObject(coTwo);
		pm.saveObject(coThree);
		// make sure the right number of OriginalObjects were stored
		List<OriginalObject> ooList = pm.getObjects(OriginalObject.class);
		assertEquals(2, ooList.size());
		// make sure the right number of ChangedInheritance objects were stored
		List<ChangedInheritance> ciList = pm.getObjects(ChangedInheritance.class);
		assertEquals(1, ciList.size());
		// make sure the Serializable object was stored
		List<Serializable> serList = pm.getObjects(Serializable.class);
		assertEquals(1, serList.size());
		pm.close();

		// re-connect to database
		pm = new PersistenceManager(driver, database, login, password);
		// change ContainerObject to ChangedInheritanceContainer
		pm.changeName(ObjectContainerObject.class, ChangedInheritanceContainer.class);
		pm.close();

		// re-connect to database
		pm = new PersistenceManager(driver, database, login, password);
		// update schema
		pm.updateSchema(ChangedInheritanceContainer.class);
		pm.close();

		// re-connect to database
		pm = new PersistenceManager(driver, database, login, password);
		// make sure the externally referenced OriginalObject survived intact
		ooList = pm.getObjects(OriginalObject.class);
		assertEquals(1, ooList.size());
		assertEquals(2, ooList.get(0).getValue());
		// make sure the ChangedInheritance object survived intact
		ciList = pm.getObjects(ChangedInheritance.class);
		assertEquals(1, ciList.size());
		assertEquals(3, ciList.get(0).getValue());
		// make sure all containers survived
		List<ChangedInheritanceContainer> containerList = pm.getObjects(ChangedInheritanceContainer.class);
		assertEquals(3, containerList.size());

		// check for the correct three values
		int nullFoundCount = 0;
		boolean chngFound = false;
		for (ChangedInheritanceContainer cont : containerList)
		{
			if (cont.getFoo() == null)
			{
				nullFoundCount++;
			}
			else if (cont.getFoo() instanceof ChangedInheritance)
			{
				chngFound = true;
				assertEquals(3, ((ChangedInheritance) cont.getFoo()).getValue());
			}
			else
			{
				fail("Unknown foo reference in container object.");
			}
		}
		assertEquals(2, nullFoundCount);
		assertTrue(chngFound);
		pm.close();

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

	/**
	 * Test if the change detection works.
	 * 
	 * The change detection will return true iff the queried object exists in
	 * the cache, but a search returns null or a different reference, false
	 * otherwise.
	 * 
	 * If there is no cache reference, the change detection returns false.
	 */
	@Test
	public void testChangeDetection() throws Exception
	{
		// create a database connection
		PersistenceManager pm1 = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm1.dropTable(Object.class);

		// create two test objects
		SimpleObject so1 = new SimpleObject();
		so1.setAge(1l);
		so1.setCount(1);
		so1.setName("One");
		so1.setScale(0.1);
		so1.setValue(1.0);
		SimpleObject so2 = new SimpleObject();
		so2.setAge(2l);
		so2.setCount(2);
		so2.setName("Two");
		so2.setScale(0.2);
		so2.setValue(2.0);

		// make sure unknown objects do not count as changed
		assertFalse(pm1.hasChanged(so1));
		assertFalse(pm1.hasChanged(so2));

		// save one object
		pm1.saveObject(so1);

		// make sure the object still count as un-changed
		assertFalse(pm1.hasChanged(so1));
		assertFalse(pm1.hasChanged(so2));

		// save other object
		pm1.saveObject(so2);

		// make sure the objects still count as un-changed
		assertFalse(pm1.hasChanged(so1));
		assertFalse(pm1.hasChanged(so2));

		// create new database connection
		PersistenceManager pm2 = new PersistenceManager(driver, database, login, password);
		// make sure unknown objects do not count as changed
		assertFalse(pm2.hasChanged(so1));
		assertFalse(pm2.hasChanged(so2));
		// change both objects in new connection
		pm2.saveObject(so1);
		pm2.saveObject(so2);
		assertFalse(pm2.hasChanged(so1));
		assertFalse(pm2.hasChanged(so2));
		// modify so1
		so1.setAge(3l);
		// save it again
		pm2.saveObject(so1);

		// make sure the new (non-existing) so1 is not found in pm1
		assertTrue(pm1.hasChanged(so1));
		assertFalse(pm2.hasChanged(so1));
		// change so1 back so that it will be found
		so1.setAge(1l);
		pm2.saveObject(so1);
		// make sure it is now detected as un-changed
		assertFalse(pm1.hasChanged(so1));
		assertFalse(pm2.hasChanged(so1));

		// delete the original so1
		pm2.deleteObjects(so1);
		// make sure not existing counts as a change
		assertTrue(pm1.hasChanged(so1));
		// the object should be cleared from cache
		assertFalse(pm2.hasChanged(so1));

		// save so1 again, this time under a new database id
		pm2.saveObject(so1);
		// make sure the database id is checked
		assertTrue(pm1.hasChanged(so1));
		assertFalse(pm2.hasChanged(so1));

		// make sure pm2 still has not detected a change
		assertFalse(pm2.hasChanged(so1));
		assertFalse(pm2.hasChanged(so2));

		// drop all objects
		pm1.dropTable(Object.class);
		pm2.dropTable(Object.class);

		pm1.close();
		pm2.close();
		pm1 = new PersistenceManager(driver, database, login, password);
		pm2 = new PersistenceManager(driver, database, login, password);

		// insert both objects
		pm1.saveObject(so1);
		pm1.saveObject(so2);

		// get the same objects from the other database connection
		List<SimpleObject> objects = pm2.getObjects(SimpleObject.class, new Equal(so1));
		assertEquals(1, objects.size());
		// make sure re-loading the object does not count as a change
		assertFalse(pm1.hasChanged(so1));
		assertFalse(pm1.hasChanged(so2));

		// change the object, re-save it
		SimpleObject so1copy = objects.get(0);
		so1copy.setAge(6l);
		pm2.saveObject(so1copy);

		// make sure the change is detected in the first connection manager
		assertTrue(pm1.hasChanged(so1));
		assertFalse(pm1.hasChanged(so2));

		// make sure there are still only two objects
		objects = pm1.getObjects(SimpleObject.class);
		assertEquals(2, objects.size());
		objects = pm2.getObjects(SimpleObject.class);
		assertEquals(2, objects.size());

		pm1.close();
		pm2.close();

	}

	/**
	 * Test deleting objects, make sure the object cache is updated accordingly.
	 */
	@Test
	public void testCacheUpdateOnDelete() throws Exception
	{
		// create a database connection
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);

		// create two test objects
		SimpleObject so1 = new SimpleObject();
		so1.setAge(1l);
		so1.setCount(1);
		so1.setName("One");
		so1.setScale(0.1);
		so1.setValue(1.0);

		// save one object
		pm.saveObject(so1);

		// create a new persistence manager
		PersistenceManager pm2 = new PersistenceManager(driver, database, login, password);
		// delete object again, using new PM
		pm2.deleteObjects(Object.class, new All());
		pm2.close();

		// make sure all objects are gone
		List<SimpleObject> objects = pm.getObjects(SimpleObject.class);
		assertEquals(0, objects.size());

		// re-add the object
		pm.saveObject(so1);

		// make sure it's found
		objects = pm.getObjects(SimpleObject.class);
		assertEquals(1, objects.size());
		pm.close();

	}

	/**
	 * Test dropping tables, make sure the object cache is updated accordingly.
	 */
	@Test
	public void testCacheUpdateOnDrop() throws Exception
	{

		// create a database connection
		PersistenceManager pm1 = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm1.dropTable(Object.class);

		// create two test objects
		SimpleObject so1 = new SimpleObject();
		so1.setAge(1l);
		so1.setCount(1);
		so1.setName("One");
		so1.setScale(0.1);
		so1.setValue(1.0);

		// save one object
		pm1.saveObject(so1);

		// create a new persistence manager, drop all tables
		PersistenceManager pm2 = new PersistenceManager(driver, database, login, password);
		// drop all objects
		pm2.dropTable(Object.class);
		pm2.close();

		// make sure all objects are gone
		List<SimpleObject> objects = pm1.getObjects(SimpleObject.class);
		assertEquals(0, objects.size());

		// re-add the object
		pm1.saveObject(so1);

		// make sure it's found
		objects = pm1.getObjects(SimpleObject.class);
		assertEquals(1, objects.size());
		pm1.close();
	}

	/**
	 * 
	 * Test deleting a single object.
	 * 
	 * 
	 * 
	 */
	@Test
	public void testDeleteObject() throws Exception
	{

		// create a database connection
		PersistenceManager pm1 = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm1.dropTable(Object.class);

		// create two test objects
		SimpleObject so1 = new SimpleObject();
		so1.setAge(1l);
		so1.setCount(1);
		so1.setName("One");
		so1.setScale(0.1);
		so1.setValue(1.0);
		SimpleObject so2 = new SimpleObject();
		so2.setAge(2l);
		so2.setCount(2);
		so2.setName("Two");
		so2.setScale(0.2);
		so2.setValue(2.0);

		pm1.saveObject(so1);
		pm1.saveObject(so2);

		List<SimpleObject> list = pm1.getObjects(SimpleObject.class);
		assertEquals(2, list.size());

		// delete the first object
		assertTrue(pm1.deleteObject(so1));
		list = pm1.getObjects(SimpleObject.class);
		// make sure we only have one remaining object
		assertEquals(1, list.size());
		// make sure that object is so2
		assertEquals(list.get(0).getCount(), so2.getCount());

		// make sure we can't delete the object again
		assertFalse(pm1.deleteObject(so1));

		// delete the remaining object
		assertTrue(pm1.deleteObject(so2));
		// make sure it's gone
		list = pm1.getObjects(SimpleObject.class);
		assertEquals(0, list.size());

		// make sure we can't delete objects again
		assertFalse(pm1.deleteObject(so1));
		assertFalse(pm1.deleteObject(so2));

		pm1.close();
	}

	/**
	 * Test calculating the SUM function in various ways.
	 * 
	 */
	@Test
	public void testSum() throws Exception
	{

		int sum = (200 * 199) / 2;

		// create a database connection
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);

		// create some test data
		for (int x = 0; x < 200; x++)
		{
			SimpleObject so = new SimpleObject();
			so.setAge((long) x);
			so.setCount(x);
			so.setValue(x);
			pm.saveObject(so);
		}

		// get the sum of the Long values
		Number n = pm.calculateAggregate(SimpleObject.class, new Sum("getAge"));
		assertTrue(n instanceof Long);
		assertEquals(sum, n.longValue());

		// get the sum of the int values
		n = pm.calculateAggregate(SimpleObject.class, new Sum("getCount"));
		assertTrue(n instanceof Integer);
		assertEquals(sum, n.intValue());

		// get the sum of the double values
		n = pm.calculateAggregate(SimpleObject.class, new Sum("getValue"));
		assertTrue(n instanceof Double);
		assertEquals(sum, n.doubleValue(), 0.0001);

		// get the sum of the long, int, and double values
		Number[] tmp = pm.calculateAggregate(SimpleObject.class, new AggregateFunction[] { new Sum("getAge"),
				new Sum("getCount"), new Sum("getValue") });
		assertEquals(3, tmp.length);
		assertTrue(tmp[0] instanceof Integer);
		assertTrue(tmp[1] instanceof Long);
		assertTrue(tmp[2] instanceof Double);

		assertEquals(sum, tmp[0].intValue());
		assertEquals(sum, tmp[1].longValue());
		assertEquals(sum, tmp[2].doubleValue(), 0.0001);

		pm.close();

	}

	/**
	 * Test calculating the AVG function in various ways.
	 * 
	 */
	@Test
	public void testAvg() throws Exception
	{

		double average = 199.0 / 2.0;

		// create a database connection
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);

		// create some test data
		for (int x = 0; x < 200; x++)
		{
			SimpleObject so = new SimpleObject();
			so.setAge((long) x);
			so.setCount(x);
			so.setValue(x);
			pm.saveObject(so);
		}

		// get the average of the Long values
		Number n = pm.calculateAggregate(SimpleObject.class, new Average("getAge"));
		assertTrue(n instanceof Double);
		assertEquals(average, n.doubleValue(), 0.0001);

		// get the average of the int values
		n = pm.calculateAggregate(SimpleObject.class, new Average("getCount"));
		assertTrue(n instanceof Double);
		assertEquals(average, n.doubleValue(), 0.0001);

		// get the average of the double values
		n = pm.calculateAggregate(SimpleObject.class, new Average("getValue"));
		assertTrue(n instanceof Double);
		assertEquals(average, n.doubleValue(), 0.0001);

		// get the average of the long, int, and double values
		Number[] tmp = pm.calculateAggregate(SimpleObject.class, new AggregateFunction[] { new Average("getAge"),
				new Average("getCount"), new Average("getValue") });
		assertEquals(3, tmp.length);
		for (int x = 0; x < 3; x++)
		{
			assertTrue(tmp[x] instanceof Double);
			assertEquals(average, tmp[x].doubleValue(), 0.0001);
		}

		pm.close();

	}

	/**
	 * Test calculating the MAX function in various ways.
	 * 
	 */
	@Test
	public void testMax() throws Exception
	{

		int max = 199;

		// create a database connection
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);

		// create some test data
		for (int x = 0; x < 200; x++)
		{
			SimpleObject so = new SimpleObject();
			so.setAge((long) x);
			so.setCount(x);
			so.setValue(x);
			pm.saveObject(so);
		}

		// get the max of the Long values
		Number n = pm.calculateAggregate(SimpleObject.class, new Maximum("getAge"));
		assertTrue(n instanceof Long);
		assertEquals(max, n.longValue());

		// get the max of the int values
		n = pm.calculateAggregate(SimpleObject.class, new Maximum("getCount"));
		assertTrue(n instanceof Integer);
		assertEquals(max, n.intValue());

		// get the max of the double values
		n = pm.calculateAggregate(SimpleObject.class, new Maximum("getValue"));
		assertTrue(n instanceof Double);
		assertEquals(max, n.doubleValue(), 0.0001);

		// get the sum of the long, int, and double values
		Number[] tmp = pm.calculateAggregate(SimpleObject.class, new AggregateFunction[] { new Maximum("getAge"),
				new Maximum("getCount"), new Maximum("getValue") });
		assertEquals(3, tmp.length);
		assertTrue(tmp[0] instanceof Integer);
		assertTrue(tmp[1] instanceof Long);
		assertTrue(tmp[2] instanceof Double);

		assertEquals(max, tmp[0].intValue());
		assertEquals(max, tmp[1].longValue());
		assertEquals(max, tmp[2].doubleValue(), 0.0001);

		pm.close();

	}

	/**
	 * Test calculating the MIN function in various ways.
	 * 
	 */
	@Test
	public void testMin() throws Exception
	{

		int min = -100;

		// create a database connection
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);

		// create some test data
		for (int x = -100; x < 100; x++)
		{
			SimpleObject so = new SimpleObject();
			so.setAge((long) x);
			so.setCount(x);
			so.setValue(x);
			pm.saveObject(so);
		}

		// get the min of the Long values
		Number n = pm.calculateAggregate(SimpleObject.class, new Minimum("getAge"));
		assertTrue(n instanceof Long);
		assertEquals(min, n.longValue());

		// get the min of the int values
		n = pm.calculateAggregate(SimpleObject.class, new Minimum("getCount"));
		assertTrue(n instanceof Integer);
		assertEquals(min, n.intValue());

		// get the min of the double values
		n = pm.calculateAggregate(SimpleObject.class, new Minimum("getValue"));
		assertTrue(n instanceof Double);
		assertEquals(min, n.doubleValue(), 0.0001);

		// get the sum of the long, int, and double values
		Number[] tmp = pm.calculateAggregate(SimpleObject.class, new AggregateFunction[] { new Minimum("getAge"),
				new Minimum("getCount"), new Minimum("getValue") });
		assertEquals(3, tmp.length);
		assertTrue(tmp[0] instanceof Integer);
		assertTrue(tmp[1] instanceof Long);
		assertTrue(tmp[2] instanceof Double);

		assertEquals(min, tmp[0].intValue());
		assertEquals(min, tmp[1].longValue());
		assertEquals(min, tmp[2].doubleValue(), 0.0001);

		pm.close();

	}

	/**
	 * Test calculating the various SQL aggregate functions.
	 * 
	 */
	@Test
	public void testAggregate() throws Exception
	{

		int sum = (200 * 199) / 2;
		int min = 0;
		int max = 199;
		double avg = 199 / 2;

		// create a database connection
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);

		// create some test data
		for (int x = 0; x < 200; x++)
		{
			SimpleObject so = new SimpleObject();
			so.setAge((long) x);
			so.setCount(x);
			so.setValue(x);
			pm.saveObject(so);
		}

		// get the sum, max, min, and average of the long values
		Number[] tmp = pm.calculateAggregate(SimpleObject.class, new AggregateFunction[] { new Sum("getAge"),
				new Maximum("getAge"), new Minimum("getAge"), new Average("getAge"), });
		assertEquals(4, tmp.length);
		//all aggregates of a long field are long...
		assertTrue(tmp[0] instanceof Long);
		assertTrue(tmp[1] instanceof Long);
		assertTrue(tmp[2] instanceof Long);
		//...except average, which is always double
		assertTrue(tmp[3] instanceof Double);

		//check that the correct values are returned
		assertEquals(sum, tmp[0].longValue());
		assertEquals(max, tmp[1].longValue());
		assertEquals(min, tmp[2].longValue());
		assertEquals(avg, tmp[3].doubleValue(), 0.0001);

		pm.close();

	}
	
	/**
	 * Test calculating the various SQL aggregate functions on an interface.
	 * 
	 */
	@Test
	public void testAggregateOnInterface() throws Exception
	{

		int sum = (200 * 199) / 2;
		int min = 0;
		int max = 199;
		double avg = 199 / 2;

		// create a database connection
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);

		// create some test data
		int x = 0;
		for (; x < 100; x++)
		{
			ImplementerA so = new ImplementerA();
			so.setSomeValue(x);
			pm.saveObject(so);
		}
		for (; x < 200; x++)
		{
			ImplementerB so = new ImplementerB();
			so.setSomeValue(x);
			pm.saveObject(so);
		}

		// get the sum, max, min, and average of the long values
		Number[] tmp = pm.calculateAggregate(SubInterface.class, new AggregateFunction[] { new Sum("getSomeValue"),
				new Maximum("getSomeValue"), new Minimum("getSomeValue"), new Average("getSomeValue"), });
		assertEquals(4, tmp.length);
		//all aggregates of a long field are long...
		assertTrue(tmp[0] instanceof Long);
		assertTrue(tmp[1] instanceof Long);
		assertTrue(tmp[2] instanceof Long);
		//...except average, which is always double
		assertTrue(tmp[3] instanceof Double);

		//check that the correct values are returned
		assertEquals(sum, tmp[0].longValue());
		assertEquals(max, tmp[1].longValue());
		assertEquals(min, tmp[2].longValue());
		assertEquals(avg, tmp[3].doubleValue(), 0.0001);

		pm.close();

	}
}
