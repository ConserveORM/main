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
package com.github.conserveorm;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.Serializable;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
import java.util.Properties;
import java.util.Set;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.conserveorm.PersistenceManager;
import com.github.conserveorm.SearchListener;
import com.github.conserveorm.adapter.AdapterBase;
import com.github.conserveorm.aggregate.AggregateFunction;
import com.github.conserveorm.aggregate.Average;
import com.github.conserveorm.aggregate.Count;
import com.github.conserveorm.aggregate.Maximum;
import com.github.conserveorm.aggregate.Minimum;
import com.github.conserveorm.aggregate.Sum;
import com.github.conserveorm.connection.ConnectionWrapper;
import com.github.conserveorm.connection.DataConnectionPool;
import com.github.conserveorm.exceptions.SchemaPermissionException;
import com.github.conserveorm.objects.AllPrimitives;
import com.github.conserveorm.objects.ArrayContainingObject;
import com.github.conserveorm.objects.Author;
import com.github.conserveorm.objects.BadColumnNames;
import com.github.conserveorm.objects.BaseInterface;
import com.github.conserveorm.objects.BlobClobObject;
import com.github.conserveorm.objects.Book;
import com.github.conserveorm.objects.ClassContainingObject;
import com.github.conserveorm.objects.ColumnNameObject;
import com.github.conserveorm.objects.ComplexArrayObject;
import com.github.conserveorm.objects.ComplexObject;
import com.github.conserveorm.objects.DataParser;
import com.github.conserveorm.objects.DateObject;
import com.github.conserveorm.objects.EnumContainer;
import com.github.conserveorm.objects.InfiniteRepititionCollection;
import com.github.conserveorm.objects.LessSimpleObject;
import com.github.conserveorm.objects.ListContainingObject;
import com.github.conserveorm.objects.MyEnum;
import com.github.conserveorm.objects.NonExistingClass;
import com.github.conserveorm.objects.ObjectArrayContainingObject;
import com.github.conserveorm.objects.SelfContainingObject;
import com.github.conserveorm.objects.SimpleObject;
import com.github.conserveorm.objects.SimpleObjectContainer;
import com.github.conserveorm.objects.SimplestObject;
import com.github.conserveorm.objects.StringArrayContainer;
import com.github.conserveorm.objects.SubInterface;
import com.github.conserveorm.objects.demo.BarTextObject;
import com.github.conserveorm.objects.demo.FooTextObject;
import com.github.conserveorm.objects.demo.Person;
import com.github.conserveorm.objects.demo.TextObject;
import com.github.conserveorm.objects.id.IdContainer;
import com.github.conserveorm.objects.id.WithIdLong;
import com.github.conserveorm.objects.id.WithIdString;
import com.github.conserveorm.objects.id.WithIdStringAndLong;
import com.github.conserveorm.objects.polymorphism.AbstractBar;
import com.github.conserveorm.objects.polymorphism.ConcreteBar1;
import com.github.conserveorm.objects.polymorphism.ConcreteBar2;
import com.github.conserveorm.objects.polymorphism.ExtendedFooContainer;
import com.github.conserveorm.objects.polymorphism.Foo;
import com.github.conserveorm.objects.polymorphism.FooContainer;
import com.github.conserveorm.objects.polymorphism.FooContainerOwner;
import com.github.conserveorm.objects.polymorphism.ImplementerA;
import com.github.conserveorm.objects.polymorphism.ImplementerB;
import com.github.conserveorm.objects.polymorphism.MyExtendedFooContainer;
import com.github.conserveorm.objects.polymorphism.MyFooContainer;
import com.github.conserveorm.objects.polymorphism.MyNonFooContainer;
import com.github.conserveorm.objects.recursive.Layer1;
import com.github.conserveorm.objects.recursive.Layer2;
import com.github.conserveorm.objects.recursive.Layer3;
import com.github.conserveorm.objects.schemaupdate.ChangedInheritance;
import com.github.conserveorm.objects.schemaupdate.ChangedInheritanceContainer;
import com.github.conserveorm.objects.schemaupdate.ContainerObject;
import com.github.conserveorm.objects.schemaupdate.NewName;
import com.github.conserveorm.objects.schemaupdate.NewNameContainer;
import com.github.conserveorm.objects.schemaupdate.NotSubClass;
import com.github.conserveorm.objects.schemaupdate.ObjectContainerObject;
import com.github.conserveorm.objects.schemaupdate.OriginalObject;
import com.github.conserveorm.objects.schemaupdate.RemovedColumn;
import com.github.conserveorm.objects.schemaupdate.RenamedColumn;
import com.github.conserveorm.objects.schemaupdate.SubClass;
import com.github.conserveorm.objects.schemaupdate.changedcolumns.ArrayToLong;
import com.github.conserveorm.objects.schemaupdate.changedcolumns.IndicesFull;
import com.github.conserveorm.objects.schemaupdate.changedcolumns.IndicesReduced;
import com.github.conserveorm.objects.schemaupdate.changedcolumns.IntToLong;
import com.github.conserveorm.objects.schemaupdate.changedcolumns.NarrowColumnObject;
import com.github.conserveorm.objects.schemaupdate.changedcolumns.ObjectToLong;
import com.github.conserveorm.objects.schemaupdate.changedcolumns.ObjectToSubclass;
import com.github.conserveorm.objects.schemaupdate.changedcolumns.StringToLong;
import com.github.conserveorm.objects.schemaupdate.changedcolumns.WideColumnObject;
import com.github.conserveorm.objects.schemaupdate.changedcolumns.WithIndex;
import com.github.conserveorm.objects.schemaupdate.changedcolumns.WithoutIndex;
import com.github.conserveorm.objects.schemaupdate.copydown.AfterBottom;
import com.github.conserveorm.objects.schemaupdate.copydown.AfterTop;
import com.github.conserveorm.objects.schemaupdate.copydown.BeforeBottom;
import com.github.conserveorm.objects.schemaupdate.copydown.BeforeTop;
import com.github.conserveorm.objects.schemaupdate.copydown.ModifiedBottom;
import com.github.conserveorm.objects.schemaupdate.copydown.ModifiedMiddle;
import com.github.conserveorm.objects.schemaupdate.copydown.ModifiedTop;
import com.github.conserveorm.objects.schemaupdate.copydown.OriginalBottom;
import com.github.conserveorm.objects.schemaupdate.copydown.OriginalMiddle;
import com.github.conserveorm.objects.schemaupdate.copydown.OriginalTop;
import com.github.conserveorm.objects.sorting.BarSortable;
import com.github.conserveorm.objects.sorting.FooSortable;
import com.github.conserveorm.objects.sorting.Sortable;
import com.github.conserveorm.select.All;
import com.github.conserveorm.select.And;
import com.github.conserveorm.select.Or;
import com.github.conserveorm.select.discriminators.Different;
import com.github.conserveorm.select.discriminators.Equal;
import com.github.conserveorm.select.discriminators.Greater;
import com.github.conserveorm.select.discriminators.GreaterOrEqual;
import com.github.conserveorm.select.discriminators.Less;
import com.github.conserveorm.select.discriminators.LessOrEqual;
import com.github.conserveorm.select.discriminators.Like;
import com.github.conserveorm.select.discriminators.NotNull;
import com.github.conserveorm.select.discriminators.Null;
import com.github.conserveorm.sort.Ascending;
import com.github.conserveorm.sort.Descending;
import com.github.conserveorm.sort.Order;
import com.github.conserveorm.test.TestTools;
import com.github.conserveorm.tools.Defaults;
import com.github.conserveorm.tools.generators.NameGenerator;


/**
 * Integration test of various functionality.
 * 
 * @author Erik Berglund
 * 
 */
public abstract class PersistTest
{
	// Settings for test
	protected String driver;
	protected String database;
	protected String login;
	protected String password;
	protected String secondDatabase;

	private static final Logger LOGGER = Logger.getLogger(Defaults.LOGGER_NAME);
	// create a log handler that outputs all warning events to the console
	static
	{
		LOGGER.setUseParentHandlers(false);
		ConsoleHandler consoleHandler = new ConsoleHandler();
		LOGGER.addHandler(consoleHandler);
		Level level = Level.INFO;
		LOGGER.setLevel(level);
		consoleHandler.setLevel(level);
	}

	/**
	 * 
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception
	{
		setupConnectionConstants();
		deleteAll();
	}
	
	/**
	 * Classes extending this one should set the database connection
	 * values (password, username, driver, database URL) 
	 * to suit the database engine being tested.
	 * @return 
	 */
	protected abstract void setupConnectionConstants();

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception
	{
		//deleteAll();
	}

	protected void deleteAll() throws SQLException
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		persist.deleteObjects(new Object());// clear everything
		persist.close(); 
	}

	/**
	 * Test method for
	 * {@link com.github.conserveorm.PersistenceManager#saveObject(java.lang.Object)}.
	 */
	@Test
	public void testAddSimplestObject() throws Exception
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		persist.dropTable(Object.class);
	
		// create a test object
		SimplestObject so = new SimplestObject();
		so.setFoo(0.67);
		persist.saveObject(so);
		persist.close();
	}

	/**
	 * Test method for
	 * {@link com.github.conserveorm.PersistenceManager#saveObject(java.lang.Object)}.
	 */
	@Test
	public void testAddObject() throws Exception
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		persist.dropTable(Object.class);

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
		
		//make sure only one object exists
		List<LessSimpleObject>objects = persist.getObjects(LessSimpleObject.class, new All());
		assertEquals(1,objects.size());
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
		persist.dropTable(Object.class);
		

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
		// remove all existing data
		persist.dropTable(Object.class);

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
		List<Object> allObjects = persist.getObjects(new Object());
		assertEquals(2, allObjects.size());
		// delete one LessSimpleObject
		SimpleObject pattern = new LessSimpleObject();
		persist.deleteObjects(LessSimpleObject.class, new GreaterOrEqual(pattern));
		allObjects = persist.getObjects(new Object());
		assertEquals(1, allObjects.size());
		// delete all objects
		persist.deleteObjects(new Object());
		allObjects = persist.getObjects(new Object());
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
		// drop all existing data
		persist.dropTable(Object.class);

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
		List<Object> allList = persist.getObjects(Object.class, new All());
		assertEquals(4, allList.size());
		List<ComplexObject> complexList = persist.getObjects(new ComplexObject());
		assertEquals(2, complexList.size());
		ComplexObject a = complexList.get(0);
		ComplexObject b = complexList.get(1);
		assertEquals(a.getData(), b.getData());
		assertTrue(a.getData() != null);
		assertEquals(3, a.getData().length);
		assertEquals(a.getData()[2],3.3,0.01);
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
		persist.dropTable(Object.class);

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
		List<ComplexArrayObject> res = persist.getObjects(new ComplexArrayObject());
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
		persist.dropTable(Object.class);

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
		List<ComplexArrayObject> res = persist.getObjects(new ComplexArrayObject());
		assertEquals(1, res.size());
		ComplexArrayObject ob1 = res.get(0);
		// check that the ComplexArrayObject contains 3 ComplexObjects
		assertEquals(3, ob1.getData().length);

		//delete the ComplexArrayObject, make sure one ComplexObject is untouched because of  external reference
		persist.deleteObjects(new ComplexArrayObject());
		List<ComplexObject> tmp = persist.getObjects(new ComplexObject());
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
		persist.dropTable(Object.class);

		int arraySize = 5;

		ComplexObject co = new ComplexObject();
		SimplestObject[][][] array = new SimplestObject[arraySize][arraySize][1];
		for (int x = 0; x < array.length; x++)
		{
			for (int y = 0; y < array[x].length; y++)
			{
				array[x][y][0] = new SimplestObject();
			}
		}
		co.setObject(array);
		co.setSimplestObject(new SimplestObject());
		persist.saveObject(co);
		ComplexObject co2 = new ComplexObject();
		SimplestObject[][][] array2 = new SimplestObject[1][][];
		array2[0] = array[0];
		co2.setObject(array2);
		persist.saveObject(co2);
		persist.close();
		persist = new PersistenceManager(driver, database, login, password);
		List<ComplexObject> cos = persist.getObjects(new ComplexObject());
		assertEquals(2, cos.size());
		co = cos.get(0);
		co2 = cos.get(1);
		if(co.getSimplestObject()==null)
		{
			co = cos.get(1);
			co2 = cos.get(0);
		}
		SimplestObject[][][] resArray = (SimplestObject[][][]) co.getObject();
		assertTrue(resArray != null);
		assertEquals(arraySize, resArray.length);
		assertEquals(arraySize, resArray[0].length);
		assertEquals(1, resArray[0][0].length);

		for (int x = 0; x < resArray.length; x++)
		{
			for (int y = 0; y < resArray[0].length; y++)
			{
				assertTrue(resArray[x][y][0] != null);
			}
		}
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
		persist.dropTable(Object.class);

		SimplestObject so = new SimplestObject();
		so.setFoo(1.0);
		ComplexObject co = new ComplexObject();
		co.setObject(so);
		persist.saveObject(co);
		// close the persistence object

		// re-open the persistence object
		// create a SimplestObject so that no results will match
		so = new SimplestObject();
		so.setFoo(2.0);
		// make sure this matches no results
		ComplexObject search = new ComplexObject();
		search.setObject(so);
		List<ComplexObject> none = persist.getObjects(search);
		assertEquals(0, none.size());
		// change the search so that one object is returned
		so.setFoo(1.0);
		List<ComplexObject> one = persist.getObjects(search);
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
		// clear the database
		persist.dropTable(Object.class);

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
		List<ComplexObject> none = persist.getObjects(search);
		assertEquals(0, none.size());
		// change the search so that one object is returned
		searchArray[1] = 5;
		searchArray[2] = 6;
		List<ComplexObject> one = persist.getObjects(search);
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
		persist.dropTable(Object.class);

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
		List<ComplexObject> none = persist.getObjects(search);
		assertEquals(0, none.size());
		// change the search so that one object is returned
		searchArray[1] = new SimplestObject();
		searchArray[1].setFoo(2.0);
		searchArray[2].setFoo(3.0);
		List<ComplexObject> one = persist.getObjects(search);
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
		persist.dropTable(Object.class);

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
		List<ComplexObject> none = persist.getObjects(search);
		assertEquals(0, none.size());
		// change the search so that one object is returned
		searchArray[1][1] = new SimplestObject();
		searchArray[1][1].setFoo(2.0);
		searchArray[1][2].setFoo(3.0);
		List<ComplexObject> one = persist.getObjects(search);
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
		// create connection
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		// drop existing data
		persist.dropTable(Object.class);

		// insert test data
		ArrayList<Double> foo = new ArrayList<Double>();
		foo.add(6.9);
		foo.add(1.1);
		persist.saveObject(foo);
		List<Object> all = persist.getObjects(new Object());
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
		// create connection
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		// drop existing data
		persist.dropTable(Object.class);

		// insert test data
		ArrayList<Double> foo = new ArrayList<Double>();
		foo.add(6.9);
		foo.add(1.1);
		persist.saveObject(foo);
		persist.close();

		persist = new PersistenceManager(driver, database, login, password);

		@SuppressWarnings("rawtypes")
		List<ArrayList> result = persist.getObjects(ArrayList.class, new All());
		assertEquals(1, result.size());
		ArrayList<?> first = result.get(0);
		assertEquals(2, first.size());
		assertEquals(6.9, first.get(0));
		assertEquals(1.1, first.get(1));
		List<Object> all = persist.getObjects(new Object());
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
		persist.dropTable(Object.class);

		persist.saveObject(map);
		List<Object> all = persist.getObjects(new Object());
		assertEquals(1, all.size());
		persist.close();
	}

	/**
	 * Saves an instance of Map and loads it, verifying that the contents are
	 * still the same.
	 */
	@SuppressWarnings("rawtypes")
	@Test
	public void testMapLoad() throws Exception
	{
		Map<String, Double> map = new HashMap<String, Double>();
		map.put("KEY", 3.0);
		map.put("ANOTHERKEY", 4.0);
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		persist.dropTable(Object.class);

		persist.saveObject(map);
		persist.close();

		persist = new PersistenceManager(driver, database, login, password);
		List<HashMap> results = persist.getObjects(HashMap.class, new All());
		assertEquals(1, results.size());
		HashMap<?, ?> first = results.get(0);
		assertEquals(2, first.size());
		assertEquals(3.0, first.get("KEY"));
		assertEquals(4.0, first.get("ANOTHERKEY"));
		List<Object> all = persist.getObjects(new Object());
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
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		persist.dropTable(Object.class);

		assertEquals(0, persist.getCount(Collection.class, new All()));
		// store some ArrayLists in the database
		List<String> list = new ArrayList<String>();
		list.add("VALUE");
		list.add("ANOTHER VALUE");
		list.add("THIRD VALUE");
		list.add("FOURTH VALUE");
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
		long count = persist.getCount(Collection.class, new All());
		assertEquals(3, count);
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
		searchResults = persist.getObjects(Collection.class, new Equal(unorderedSearchSet, false));
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
		persist.dropTable(Object.class);

		// create a simple object
		SimpleObject object = new SimpleObject();
		object.setAge(500L);
		// save it
		persist.saveObject(object);
		// make sure only one object exists
		List<SimpleObject> allObjects = persist.getObjects(new SimpleObject());
		assertEquals(1, allObjects.size());
		// change the object
		object.setAge(1000L);
		// save the object again
		persist.saveObject(object);
		// make sure there's still only one object
		allObjects = persist.getObjects(new SimpleObject());
		assertEquals(1, allObjects.size());
		// re-open the database connection
		persist.close();
		persist = new PersistenceManager(driver, database, login, password);
		// make sure the object has been changed
		allObjects = persist.getObjects(new SimpleObject());
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
		allObjects = persist.getObjects(new SimpleObject());
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
		List<ComplexObject> allObjects = persist.getObjects(new ComplexObject());
		assertEquals(1, allObjects.size());
		// change the object
		ages[1] = 20;
		// save the object again
		persist.saveObject(object);
		// make sure there's still only one object
		allObjects = persist.getObjects(new ComplexObject());
		assertEquals(1, allObjects.size());
		// re-open the database connection
		persist.close();
		persist = new PersistenceManager(driver, database, login, password);
		// make sure the object has been changed
		allObjects = persist.getObjects(new ComplexObject());
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
		allObjects = persist.getObjects(new ComplexObject());
		assertEquals(2, allObjects.size());
		first = allObjects.get(0);
		ComplexObject second = allObjects.get(1);
		Integer[] a = (Integer[]) first.getObject();
		Integer[] b = (Integer[]) second.getObject();
		if(a[0]==1)
		{
			assertEquals(1,(int)a[0]);
			assertEquals(20,(int)a[1]);
			assertEquals(3,(int)a[2]);
			assertEquals(5,(int)b[0]);
			assertEquals(60,(int)b[1]);
			assertEquals(7,(int)b[2]);
		}
		else
		{
			assertEquals(1,(int)b[0]);
			assertEquals(20,(int)b[1]);
			assertEquals(3,(int)b[2]);
			assertEquals(5,(int)a[0]);
			assertEquals(60,(int)a[1]);
			assertEquals(7,(int)a[2]);
			
		}

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
		persist.dropTable(Object.class);

		ConnectionWrapper cw = persist.getConnectionWrapper();
		// add a large number of entries
		for (int x = 0; x < 200; x++)
		{
			SimplestObject so = new SimplestObject();
			if(x>50)
			{
				so.setFoo((double) x);
			}
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
		
		Number n = persist.calculateAggregate(SimplestObject.class, new Count("getFoo"),new All());
		assertEquals(149l,n);
		
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
			System.out.println(NameGenerator.getSystemicName(c));
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
		persist.dropTable(Object.class);

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
		persist.dropTable(Object.class);

		DateObject dateO = new DateObject();
		dateO.setDate(new java.sql.Date(System.currentTimeMillis()));
		dateO.setTime(new java.sql.Time(System.currentTimeMillis()));
		dateO.setTimeStamp(new java.sql.Timestamp(System.currentTimeMillis()));
		persist.saveObject(dateO);

		// make sure only one object exists
		List<Object> all = persist.getObjects(new Object());
		assertEquals(1, all.size());
		assertEquals(dateO.getTime().getTime(), ((DateObject)all.get(0)).getTime().getTime());
		assertEquals(dateO.getDate().getTime(), ((DateObject)all.get(0)).getDate().getTime());

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
		persist.dropTable(Object.class);

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
		List<BlobClobObject> all = persist.getObjects(new BlobClobObject());
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
		persist.dropTable(Object.class);

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
		List<SimplestObject> list = persist.getObjects(new SimplestObject());
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
		persist.dropTable(Object.class);

		ConnectionWrapper cw = persist.getConnectionWrapper();
		// add a large number of entries
		for (int x = 199; x >= 0; x--)
		{
			SimplestObject so = new SimplestObject();
			so.setFoo((double) x);
			ComplexObject co = new ComplexObject();
			co.setSimplestObject(so);
			persist.saveObject(cw, co);
		}
		cw.commitAndDiscard();
		// make sure all entries have been added
		List<ComplexObject> list = persist.getObjects(new ComplexObject());
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
		persist.dropTable(Object.class);

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
		List<SimplestObject> list = persist.getObjects(new SimplestObject());
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
		persist.dropTable(Object.class);
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

		// find all authors with the first name 'Isaac'.
		Author asimov = new Author();
		asimov.setFirstName("Isaac");
		List<Author> asimovAuthors = persist.getObjects(Author.class, new Equal(asimov));
		assertEquals(1, asimovAuthors.size());

		persist.close();
	}
	
	/**
	 * Test the extended book/author example.
	 */
	@Test
	public void testAuthorBookExample2() throws Exception
	{

		this.createAuthors();
		// open a new connection
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);

		//find all books by authors that have written about crime
		Author crimeAuthor = new Author();
		Book crimeBook = new Book();
		crimeBook.addKeyWord("crime");
		crimeAuthor.addBook(crimeBook);
		Book seekBook = new Book();
		seekBook.addAuthor(crimeAuthor);
		List<Book> crimeAuthorBooks = 
		           persist.getObjects(Book.class, new Equal(seekBook));
		assertEquals(4,crimeAuthorBooks.size());
		
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
		
		//add some self-containing objects, make sure we can delete them with a general
		//search on a superclass.

		persist = new PersistenceManager(driver, database, login, password);
		 l1 = new Layer1();
		l1.setLayer2(new Layer2());
		l1.getLayer2().setLayer3(new Layer3());
		l1.getLayer2().getLayer3().setLayer1(l1);
		persist.saveObject(l1);
		persist.deleteObjects(SelfContainingObject.class,new All());
		persist.close();
		

		persist = new PersistenceManager(driver, database, login, password);
		assertEquals(3,persist.getCount(Object.class, new All()));
		persist.deleteObjects(Object.class, new All());
		assertEquals(0,persist.getCount(Object.class, new All()));
		persist.close();
		

	}

	/**
	 * Test deletion of self-containing objects.
	 */
	@Test
	public void testDeleteSelfContaining() throws Exception
	{

		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		persist.dropTable(Object.class);

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
		persist.close();
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
		persist.dropTable(Object.class);

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

		// assert that the Foo object can't be retrieved with a ConcreteBar1
		// object if we use strict inheritance
		ConcreteBar1 cbar1 = new ConcreteBar1();
		cbar1.setAbstractName("f");
		cbar1.setConcreteName("C");
		searchFoo.setBar(cbar1);
		foos = persist.getObjects(Foo.class, new Equal(searchFoo, true));
		assertEquals(0, foos.size());

		// assert that the Foo object can be retrieved with a ConcreteBar1
		// object if we don't use strict inheritance
		cbar1 = new ConcreteBar1();
		cbar1.setAbstractName("f");
		cbar1.setConcreteName("C");
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
		persist.dropTable(Object.class);

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
		// clean out all existing data
		persist.dropTable(Object.class);
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
		List<Object> resObjs = persist.getObjects(Object.class, new All());
		assertEquals(0, resObjs.size());
		List<SimpleObject> resSimpObjs = persist.getObjects(SimpleObject.class, new All());
		assertEquals(0, resSimpObjs.size());
		persist.close();

		// re-establish connection
		persist = new PersistenceManager(driver, database, login, password);
		// insert some superclass objects
		OriginalObject oo = new OriginalObject();
		persist.saveObject(oo);
		oo = new OriginalObject();
		persist.saveObject(oo);
		// insert subclass objects
		SubClass sc = new SubClass();
		persist.saveObject(sc);
		sc = new SubClass();
		persist.saveObject(sc);
		// make sure the correct number of objects are stored
		assertEquals(4, persist.getCount(OriginalObject.class, new All()));
		assertEquals(2, persist.getCount(SubClass.class, new All()));
		// drop the subclass
		persist.dropTable(SubClass.class);
		persist.close();

		// re-establish connection
		persist = new PersistenceManager(driver, database, login, password);
		// make sure superclass is still there
		assertEquals(2, persist.getCount(OriginalObject.class, new All()));
		assertEquals(0, persist.getCount(SubClass.class, new All()));
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

		persistOne.dropTable(Object.class);
		persistTwo.dropTable(Object.class);

		// create a simple object
		SimpleObject so = new SimpleObject();
		so.setName("foo bar");

		// save the object
		persistOne.saveObject(so);
		
		//create and drop a complex object to prevent some database engines from bugging out
		persistOne.saveObject(new ComplexObject());
		persistOne.saveObject(new SimplestObject());
		persistOne.deleteObjects(ComplexObject.class, new All());
		persistOne.deleteObjects(SimplestObject.class, new All());

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
		
		
		so.setName(null);
		so.setAge(null);
		persistOne.saveObject(so);
		persistTwo.refresh(copy);
		assertNull(copy.getName());
		assertNull(copy.getAge());
		so.setName("updated");
		so.setAge(42l);
		persistOne.saveObject(so);
		persistTwo.refresh(copy);
		assertNotNull(copy.getName());
		assertNotNull(copy.getAge());
		assertEquals("updated",copy.getName());
		assertEquals(42l,(long)copy.getAge());
		
		persistOne.deleteObject(so);
		assertEquals(0,persistOne.getCount(SimpleObject.class, new All()));
		assertEquals(0,persistTwo.getCount(SimpleObject.class, new All()));
		persistTwo.close();
		
		//test refreshing some complex objects
		ComplexObject co = new ComplexObject();
		co.setSimplestObject(new SimplestObject());
		co.getSimplestObject().setFoo(42.0);
		persistOne.saveObject(co);
		persistTwo = new PersistenceManager(driver, database, login, password);
		assertEquals(1,persistTwo.getCount(ComplexObject.class, new All()));
		assertEquals(1,persistTwo.getCount(SimplestObject.class, new All()));
		List<ComplexObject>cos = persistTwo.getObjects(ComplexObject.class, new All());
		assertEquals(1,cos.size());
		ComplexObject complexCopy = cos.get(0);
		assertEquals(co.getSimplestObject().getFoo(),complexCopy.getSimplestObject().getFoo());
		
		co.getSimplestObject().setFoo(18.0);
		persistOne.saveObject(co);
		persistTwo.refresh(complexCopy);
		assertEquals(co.getSimplestObject().getFoo(),complexCopy.getSimplestObject().getFoo());
		
		co.getSimplestObject().setFoo(null);
		persistOne.saveObject(co);
		persistTwo.refresh(complexCopy);
		assertNull(complexCopy.getSimplestObject().getFoo());
		
		
		co.setSimplestObject(null);
		persistOne.saveObject(co);
		persistTwo.refresh(complexCopy);
		assertNull(complexCopy.getSimplestObject());
		assertEquals(0,persistOne.getCount(SimplestObject.class, new All()));
		
		co.setSimplestObject(new SimplestObject());
		persistOne.saveObject(co);
		persistTwo.refresh(complexCopy);
		assertNotNull(complexCopy.getSimplestObject());

		persistTwo.close();
		persistOne.close();
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
		persistOne.close();
		persistTwo.close();
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
		
		boolean thrown = false;
		persistOne = new PersistenceManager(driver, database, login, password);
		try
		{
			//this should throw an exception
			persistOne.duplicate(persistOne);
		}
		catch(IllegalArgumentException e)
		{
			thrown = true;
		}
		assertTrue("Copying database into itself did not throw exception",thrown);
		
		persistOne.close();
	}

	/**
	 * Test saving/loading Calendar objects.
	 * 
	 */
	@Test
	public void testCalendar() throws Exception
	{

		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		persist.dropTable(Object.class);

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
		TestTools testTools = new TestTools(pm.getPersist());
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
		testTools.changeName(OriginalObject.class, NewName.class);
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
		TestTools testTools = new TestTools(pm.getPersist());
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
		testTools.changeName(OriginalObject.class, RenamedColumn.class);
		// update schema
		pm.updateSchema(RenamedColumn.class);
		pm.close();

		// re-establish connection
		pm = new PersistenceManager(driver, database, login, password);
		testTools = new TestTools(pm.getPersist());
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
		testTools.changeName(RenamedColumn.class, OriginalObject.class);
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
		new TestTools(pm.getPersist()).changeName(OriginalObject.class, RemovedColumn.class);
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
		new TestTools(pm.getPersist()).changeName(RemovedColumn.class, OriginalObject.class);
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
		Object o = new Object();
		pm.saveObject(o);
		OriginalObject oo = new OriginalObject();
		oo.setName(name);
		oo.setValue(value);
		oo.setOtherObject(otherObject);
		oo.setRedundantObject(redundantObject);
		pm.saveObject(oo);
		//remove the first object
		pm.deleteObject(o);
		
		pm.close();

		// change object to long
		pm = new PersistenceManager(driver, database, login, password);
		assertEquals(1,pm.getCount(SimplestObject.class,new All()));
		new TestTools(pm.getPersist()).changeName(OriginalObject.class, ObjectToLong.class);
		pm.updateSchema(ObjectToLong.class);
		pm.close();
		
		// check that objects exist/not exist
		pm = new PersistenceManager(driver, database, login, password);
		List<ObjectToLong> res = pm.getObjects(ObjectToLong.class, new All());
		assertEquals(1, res.size());
		ObjectToLong tmp = res.get(0);
		assertTrue(tmp.getName().equals(name));
		assertNull(tmp.getOtherObject());
		assertNotNull(tmp.getRedundantObject());
		assertEquals(1,pm.getCount(SimplestObject.class,new All()));
		// check that the dependent object has been dropped
		List<SimpleObject> dependents = pm.getObjects(SimpleObject.class, new All());
		assertEquals(0, dependents.size());
		//re-save the object with a long value
		tmp.setOtherObject(42L);
		pm.saveObject(tmp);
		//make sure redundantobject still exists
		res = pm.getObjects(ObjectToLong.class, new All());
		assertNotNull(res.get(0).getRedundantObject());
		
		// change long to object
		new TestTools(pm.getPersist()).changeName(ObjectToLong.class, OriginalObject.class);
		pm.updateSchema(OriginalObject.class);
		pm.close();
		
		//check that everything is like we want it
		pm = new PersistenceManager(driver, database, login, password);
		List<OriginalObject> res1 = pm.getObjects(OriginalObject.class, new All());
		assertEquals(1, res1.size());
		OriginalObject tmp1 = res1.get(0);
		assertNotNull(tmp1.getRedundantObject());
		//this should be null because there is no way to convert from long to Object.
		assertNull(tmp1.getOtherObject());
		assertEquals(0,pm.getCount(ObjectToLong.class, new All()));
		assertEquals(1,pm.getCount(SimplestObject.class,new All()));
		//re-save the object with an object value
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
		new TestTools(pm.getPersist()).changeName(OriginalObject.class, ObjectToSubclass.class);
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
		pm.close();

		// change subclass to superclass
		pm = new PersistenceManager(driver, database, login, password);
		new TestTools(pm.getPersist()).changeName(ObjectToSubclass.class, OriginalObject.class);
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
		new TestTools(pm.getPersist()).changeName(OriginalObject.class, IntToLong.class);
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
		new TestTools(pm.getPersist()).changeName(IntToLong.class, OriginalObject.class);
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
		new TestTools(pm.getPersist()).changeName(OriginalObject.class, StringToLong.class);
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
		new TestTools(pm.getPersist()).changeName(StringToLong.class, OriginalObject.class);
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
		TestTools testTools = new TestTools(pm.getPersist());
		testTools.changeName(OriginalObject.class, ArrayToLong.class);
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
		testTools = new TestTools(pm.getPersist());
		testTools.changeName(ArrayToLong.class, OriginalObject.class);
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
		new TestTools(pm.getPersist()).changeName(NotSubClass.class, SubClass.class);
		pm.close();

		pm = new PersistenceManager(driver, database, login, password);
		// change the database schema
		pm.updateSchema(SubClass.class);
		pm.close();

		pm = new PersistenceManager(driver, database, login, password);
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
		new TestTools(pm.getPersist()).changeName(SubClass.class, NotSubClass.class);
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
		// search NotSubClass, make sure only matching object is returned
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
		new TestTools(pm.getPersist()).changeName(OriginalObject.class, NewName.class);
		new TestTools(pm.getPersist()).changeName(ContainerObject.class, NewNameContainer.class);
		pm.close();

		// re-connect to database
		pm = new PersistenceManager(driver, database, login, password);
		// update schema
		pm.updateSchema(NewName.class);
		pm.close();

		// re-connect to database
		pm = new PersistenceManager(driver, database, login, password);
		// make sure there is one NewName object
		List<NewName> nnList = pm.getObjects(NewName.class, new All());
		assertEquals(1, nnList.size());
		// make sure there are two NewNameContainer objects
		List<NewNameContainer> ncList = pm.getObjects(NewNameContainer.class, new All());
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
		nnList = pm.getObjects(NewName.class, new All());
		assertEquals(2, nnList.size());
		ncList = pm.getObjects(NewNameContainer.class, new All());
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

		// create external reference for object two
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
		List<OriginalObject> ooList = pm.getObjects(OriginalObject.class, new All());
		assertEquals(2, ooList.size());
		// make sure the right number of ChangedInheritance objects were stored
		List<ChangedInheritance> ciList = pm.getObjects(ChangedInheritance.class, new All());
		assertEquals(1, ciList.size());
		// make sure the Serializable object was stored
		List<Serializable> serList = pm.getObjects(Serializable.class, new All());
		assertEquals(1, serList.size());
		// make sure the containers are saved
		List<ObjectContainerObject> contList = pm.getObjects(ObjectContainerObject.class, new All());
		assertEquals(3, contList.size());
		assertNotNull(contList.get(0).getFoo());
		assertNotNull(contList.get(1).getFoo());
		assertNotNull(contList.get(2).getFoo());
		pm.close();

		// re-connect to database
		pm = new PersistenceManager(driver, database, login, password);
		// change ContainerObject to ChangedInheritanceContainer
		new TestTools(pm.getPersist()).changeName(ObjectContainerObject.class, ChangedInheritanceContainer.class);
		pm.close();

		// re-connect to database
		pm = new PersistenceManager(driver, database, login, password);
		// update schema
		pm.updateSchema(ChangedInheritanceContainer.class);
		pm.close();

		// re-connect to database
		pm = new PersistenceManager(driver, database, login, password);
		// make sure the externally referenced OriginalObject survived intact
		ooList = pm.getObjects(OriginalObject.class, new All());
		assertEquals(1, ooList.size());
		assertEquals(2, ooList.get(0).getValue());
		// make sure the ChangedInheritance object survived intact
		ciList = pm.getObjects(ChangedInheritance.class, new All());
		assertEquals(1, ciList.size());
		assertEquals(3, ciList.get(0).getValue());
		// make sure all containers survived
		List<ChangedInheritanceContainer> containerList = pm.getObjects(ChangedInheritanceContainer.class, new All());
		assertEquals(3, containerList.size());

		// check for the correct three values
		int nullFoundCount = 0;
		boolean chngFound = false;
		for (ChangedInheritanceContainer cont : containerList)
		{
			if (cont.getFoo() == null)
			{
				nullFoundCount++;
			} else if (cont.getFoo() instanceof ChangedInheritance)
			{
				chngFound = true;
				assertEquals(3, ((ChangedInheritance) cont.getFoo()).getValue());
			} else
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
		new TestTools(pm.getPersist()).changeName(OriginalObject.class, ChangedInheritance.class);

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
		new TestTools(pm.getPersist()).changeName(ChangedInheritance.class, OriginalObject.class);
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
		TestTools tt = new TestTools(pm.getPersist());
		tt.changeName(OriginalTop.class, ModifiedTop.class);
		tt.changeName(OriginalMiddle.class, ModifiedMiddle.class);
		tt.changeName(OriginalBottom.class, ModifiedBottom.class);

		pm.updateSchema(ModifiedBottom.class);
		pm.close();

		// get all ModifiedBottom with foo==1
		pm = new PersistenceManager(driver, database, login, password);
		ModifiedBottom ot = new ModifiedBottom();
		ot.setFoo(1);
		List<ModifiedBottom> res = pm.getObjects(ot);
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

		TestTools testTools = new TestTools(pm.getPersist());
		// add two Bottom objects.
		ModifiedBottom b1 = new ModifiedBottom();
		b1.setFoo(1);
		ModifiedBottom b2 = new ModifiedBottom();
		b2.setFoo(2);
		pm.saveObject(b1);
		pm.saveObject(b2);

		testTools.changeName(ModifiedTop.class, OriginalTop.class);
		testTools.changeName(ModifiedMiddle.class, OriginalMiddle.class);
		testTools.changeName(ModifiedBottom.class, OriginalBottom.class);

		pm.updateSchema(OriginalBottom.class);
		pm.close();

		// get all OriginalTop with foo==1
		pm = new PersistenceManager(driver, database, login, password);
		OriginalTop ot = new OriginalTop();
		ot.setFoo(1);
		List<OriginalTop> res = pm.getObjects(ot);
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
		a.addStr("one");
		a.addStr("two");
		a.addStr("three");
		a.addStr("four");
		pm.saveObject(a);
		ListContainingObject b = new ListContainingObject();
		b.addStr("five");
		b.addStr("six");
		b.addStr("seven");
		b.addStr("eight");
		pm.saveObject(b);
		pm.close();

		pm = new PersistenceManager(driver, database, login, password);

		// search
		List<ListContainingObject> res = pm.getObjects(ListContainingObject.class, new All());
		assertEquals(2, res.size());
		for (ListContainingObject r : res)
		{
			assertNotNull(r.getList());
			assertTrue("Size of should be greater than 3, was " + r.getList().size(),3 < r.getList().size());
			assertNotNull(r.getList().get(0));
		}
		// make sure all objects are deleted
		pm.deleteObjects(Object.class,new All());
		ConnectionWrapper cw = pm.getConnectionWrapper();
		ResultSet rs = cw.prepareStatement("SELECT COUNT(*) FROM "+Defaults.HAS_A_TABLENAME).executeQuery();
		rs.next();
		assertEquals(0,rs.getLong(1));
		rs.close();
		cw.commitAndDiscard();
		pm.close();
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

		pm2.close();
		pm1.close();
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
		objects = pm1.getObjects(SimpleObject.class, new All());
		assertEquals(2, objects.size());
		objects = pm2.getObjects(SimpleObject.class, new All());
		assertEquals(2, objects.size());

		pm2.close();
		pm1.close();

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
		List<SimpleObject> objects = pm.getObjects(SimpleObject.class, new All());
		assertEquals(0, objects.size());

		// re-add the object
		pm.saveObject(so1);

		// make sure it's found
		objects = pm.getObjects(SimpleObject.class, new All());
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
		List<SimpleObject> objects = pm1.getObjects(SimpleObject.class, new All());
		assertEquals(0, objects.size());

		// re-add the object
		pm1.saveObject(so1);

		// make sure it's found
		objects = pm1.getObjects(SimpleObject.class, new All());
		assertEquals(1, objects.size());
		pm1.close();
	}

	/**
	 * Test deleting a single object.
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

		List<SimpleObject> list = pm1.getObjects(SimpleObject.class, new All());
		assertEquals(2, list.size());

		// delete the first object
		assertTrue(pm1.deleteObject(so1));
		list = pm1.getObjects(SimpleObject.class, new All());
		// make sure we only have one remaining object
		assertEquals(1, list.size());
		// make sure that object is so2
		assertEquals(list.get(0).getCount(), so2.getCount());

		// make sure we can't delete the object again
		assertFalse(pm1.deleteObject(so1));

		// delete the remaining object
		assertTrue(pm1.deleteObject(so2));
		// make sure it's gone
		list = pm1.getObjects(SimpleObject.class, new All());
		assertEquals(0, list.size());

		// make sure we can't delete objects again
		assertFalse(pm1.deleteObject(so1));
		assertFalse(pm1.deleteObject(so2));

		pm1.close();
		
		pm1 = new PersistenceManager(driver, database, login, password);
		//add a SimpleObject 
		so1 = new SimpleObject();
		so1.setCount(1);
		pm1.saveObject(so1);
		//add a SimpleObject as part of another object
		so2 = new SimpleObject();
		SimpleObjectContainer soc = new SimpleObjectContainer();
		soc.setSimpleObject(so2);
		pm1.saveObject(soc);
		//delete all simpleobjects, make sure one is left
		assertEquals(1,pm1.deleteObjects(SimpleObject.class, new All()));
		assertEquals(1,pm1.getCount(SimpleObject.class, new All()));
		//delete the containing object
		pm1.deleteObject(soc);
		//make sure the contained object is also gone
		assertEquals(0,pm1.getCount(SimpleObject.class,new All()));
		pm1.close();
	}

	/**
	 * Test calculating the SUM function in various ways.
	 * 
	 */
	@Test
	public void testSum() throws Exception
	{

		long sum = (200 * 199) / 2;

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
		assertEquals(Long.class, n.getClass());
		assertEquals(sum, n.longValue());

		// get the sum of the int values
		n = pm.calculateAggregate(SimpleObject.class, new Sum("getCount"));
		assertEquals(Long.class, n.getClass());
		assertEquals(sum, n.longValue());

		// get the sum of the double values
		n = pm.calculateAggregate(SimpleObject.class, new Sum("getValue"));
		assertEquals(Double.class, n.getClass());
		assertEquals(sum, n.doubleValue(), 0.0001);

		// get the sum of the long, int, and double values
		Number[] tmp = pm.calculateAggregate(SimpleObject.class,
				new AggregateFunction[] { new Sum("getAge"), new Sum("getCount"), new Sum("getValue") });
		assertEquals(3, tmp.length);
		assertEquals(Long.class, tmp[0].getClass());
		assertEquals(Long.class, tmp[1].getClass());
		assertEquals(Double.class, tmp[2].getClass());

		assertEquals(sum, tmp[0].longValue());
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
		assertEquals(Double.class, n.getClass());
		// assertEquals(average, n.doubleValue(), 0.0001);

		// get the average of the int values
		n = pm.calculateAggregate(SimpleObject.class, new Average("getCount"));
		assertEquals(Double.class, n.getClass());
		// assertEquals(average, n.doubleValue(), 0.0001);

		// get the average of the double values
		n = pm.calculateAggregate(SimpleObject.class, new Average("getValue"));
		assertEquals(Double.class, n.getClass());
		assertEquals(average, n.doubleValue(), 0.0001);

		// get the average of the long, int, and double values
		Number[] tmp = pm.calculateAggregate(SimpleObject.class,
				new AggregateFunction[] { new Average("getAge"), new Average("getCount"), new Average("getValue") });
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
		Number[] tmp = pm.calculateAggregate(SimpleObject.class,
				new AggregateFunction[] { new Maximum("getAge"), new Maximum("getCount"), new Maximum("getValue") });
		assertEquals(3, tmp.length);
		assertEquals(Long.class, tmp[0].getClass());
		assertEquals(Integer.class, tmp[1].getClass());
		assertEquals(Double.class, tmp[2].getClass());

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
		Number[] tmp = pm.calculateAggregate(SimpleObject.class,
				new AggregateFunction[] { new Minimum("getAge"), new Minimum("getCount"), new Minimum("getValue") });
		assertEquals(3, tmp.length);
		assertEquals(Long.class, tmp[0].getClass());
		assertEquals(Integer.class, tmp[1].getClass());
		assertEquals(Double.class, tmp[2].getClass());

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
		double avg = 199.0 / 2.0;

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
		// all aggregates of a long field are long...
		assertEquals(Long.class, tmp[0].getClass());
		assertEquals(Long.class, tmp[1].getClass());
		assertEquals(Long.class, tmp[2].getClass());
		// ...except average, which is always double
		assertEquals(Double.class, tmp[3].getClass());

		// check that the correct values are returned
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
		double avg = 199.0 / 2.0;

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
		// add some null values
		for (; x < 300; x++)
		{
			ImplementerB so = new ImplementerB();
			pm.saveObject(so);
		}

		// get the sum, max, min, and average of the long values
		Number[] tmp = pm.calculateAggregate(SubInterface.class, new AggregateFunction[] { new Sum("getSomeValue"),
				new Maximum("getSomeValue"), new Minimum("getSomeValue"), new Average("getSomeValue"), });
		assertEquals(4, tmp.length);
		// sum of an int field is long...
		assertEquals(Long.class, tmp[0].getClass());
		// max of an int field is int
		assertEquals(Integer.class, tmp[1].getClass());
		// min of an int field is int
		assertEquals(Integer.class, tmp[2].getClass());
		// ...and average is always double
		assertTrue(tmp[3] instanceof Double);

		// check that the correct values are returned
		assertEquals(sum, tmp[0].longValue());
		assertEquals(max, tmp[1].intValue());
		assertEquals(min, tmp[2].intValue());
		assertEquals(avg, tmp[3].doubleValue(), 0.0001);

		pm.close();

	}

	/**
	 * Test if protection entries are correctly handled when we perform schema
	 * update task move subclass from one class to another.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testProtectionEntryMaintenanceOnMoveClassToNewSuperclass() throws Exception
	{
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);

		// Move property from one superclass to another, incompatible, one.

		// drop all tables
		pm.dropTable(Object.class);
		// create two OriginalObjects to make sure they are preserved
		pm.saveObject(new OriginalObject());
		pm.saveObject(new OriginalObject());
		// create two SubClass, store them in ContainerObject
		SubClass ns = new SubClass();
		ns.setName("foo");
		ContainerObject co = new ContainerObject();
		co.setFoo(ns);
		// protect both object and container
		pm.saveObject(co);
		pm.saveObject(ns);

		ns = new SubClass();
		ns.setName("bar");
		co = new ContainerObject();
		co.setFoo(ns);
		// protect both object and container
		pm.saveObject(ns);
		pm.saveObject(co);

		// check that objects were saved correctly
		assertEquals(2, pm.getCount(ContainerObject.class, new All()));
		assertEquals(4, pm.getCount(OriginalObject.class, new All()));
		assertEquals(2, pm.getCount(SubClass.class, new All()));
		

		// rename SubClass to NotSubClass, move from OrignialObject to Object
		new TestTools(pm.getPersist()).changeName(SubClass.class, NotSubClass.class);
		// change the database schema
		pm.updateSchema(NotSubClass.class);
		pm.close();

		pm = new PersistenceManager(driver, database, login, password);
		// pm should now contain two ContainerObjects where foo == null.
		List<ContainerObject> coList = pm.getObjects(ContainerObject.class, new All());
		assertEquals(2, coList.size());
		assertNull(coList.get(0).getFoo());
		assertNull(coList.get(1).getFoo());
		assertEquals(2, pm.getCount(OriginalObject.class, new All()));
		// make sure all OriginalObject have name == null
		List<OriginalObject> ooList = pm.getObjects(OriginalObject.class, new All());
		assertEquals(2, ooList.size());
		for (OriginalObject oo : ooList)
		{
			assertNull(oo.getName());
		}
		// make sure all NotSubClass has name != null
		List<NotSubClass> nsList = pm.getObjects(NotSubClass.class, new All());
		assertEquals(2, nsList.size());
		assertNotNull(nsList.get(0).getName());
		assertNotNull(nsList.get(1).getName());

		// delete all OriginalObject
		pm.deleteObjects(OriginalObject.class, new All());
		// make sure there are no OriginalObject since they are not protected by
		// ContainerObject
		assertEquals(0, pm.getCount(OriginalObject.class, new All()));
		// make sure there are still two NotSubClass
		assertEquals(2, pm.getCount(NotSubClass.class, new All()));
		// make sure there are only four objects
		assertEquals(4, pm.getCount(Object.class, new All()));

		// delete all ContainerObject
		pm.deleteObjects(ContainerObject.class, new All());
		// make sure there are no ContainerObject
		assertEquals(0, pm.getCount(ContainerObject.class, new All()));
		// make sure there are no OriginalObject
		assertEquals(0, pm.getCount(OriginalObject.class, new All()));
		// make sure there are still two NotSubClass
		assertEquals(2, pm.getCount(NotSubClass.class, new All()));
		// make sure there are only two objects
		assertEquals(2, pm.getCount(Object.class, new All()));

		// delete all NotSubClass
		pm.deleteObjects(NotSubClass.class, new All());
		// make sure there are no NotSubClass
		assertEquals(0, pm.getCount(NotSubClass.class, new All()));

		// make sure there are no objects at all
		assertEquals(0, pm.getCount(Object.class, new All()));

		pm.close();
	}

	/**
	 * Test if protection entries are correctly handled when a property is
	 * changed from a superclass to one of its subclasses.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testProtectionEntryMaintenanceOnChangePropertyToSubclass() throws Exception
	{
		// Move property from one superclass to subclass.
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);
		// create two OriginalObjects to give OriginalObject and SubClass
		// different IDs
		pm.saveObject(new OriginalObject());
		pm.saveObject(new OriginalObject());
		// create two OriginalObject, store them in ContainerObject
		OriginalObject oo = new OriginalObject();
		oo.setName("foo");
		ContainerObject co = new ContainerObject();
		co.setFoo(oo);
		pm.saveObject(co);

		oo = new OriginalObject();
		oo.setName("bar");
		co = new ContainerObject();
		co.setFoo(oo);
		pm.saveObject(co);
		pm.close();

		pm = new PersistenceManager(driver, database, login, password);
		// rename OriginalObject to SubClass, move from superclass to subclass
		new TestTools(pm.getPersist()).changeName(OriginalObject.class, SubClass.class);
		pm.close();

		pm = new PersistenceManager(driver, database, login, password);
		// change the database schema
		pm.updateSchema(SubClass.class);
		pm.close();

		pm = new PersistenceManager(driver, database, login, password);

		// make sure loading ContainerObject still contains the correct data
		List<ContainerObject> coList = pm.getObjects(ContainerObject.class, new All());
		assertEquals(2, coList.size());
		String zeroFoo = coList.get(0).getFoo().getName();
		String oneFoo = coList.get(1).getFoo().getName();
		assertTrue(zeroFoo.equals("bar") || zeroFoo.equals("foo"));
		assertTrue(oneFoo.equals("bar") || oneFoo.equals("foo"));
		// deleting all OriginalObject or SubClass should now result in no
		// change, as they are protected by ContainerObject
		// assert that there are still four OriginalObject and two SubClass
		// items
		assertEquals(4, pm.getCount(OriginalObject.class, new All()));
		assertEquals(4, pm.getCount(SubClass.class, new All()));
		// delete all OriginalObject items
		pm.deleteObjects(OriginalObject.class, new All());
		// two should remain, since they are protected
		assertEquals(2, pm.getCount(OriginalObject.class, new All()));
		assertEquals(2, pm.getCount(SubClass.class, new All()));
		// delete all SubClass items
		pm.deleteObjects(SubClass.class);
		// two should remain, since they are protected
		assertEquals(2, pm.getCount(OriginalObject.class, new All()));
		assertEquals(2, pm.getCount(SubClass.class, new All()));
		// deleting all ContainerObject should delete all SubClass and
		// OriginalObject, as they have no external reference
		pm.deleteObjects(ContainerObject.class, new All());
		assertEquals(0, pm.getCount(ContainerObject.class, new All()));
		assertEquals(0, pm.getCount(OriginalObject.class, new All()));
		assertEquals(0, pm.getCount(SubClass.class, new All()));

		pm.close();
	}

	/**
	 * Test if protection entries are correctly handled when a property is
	 * changed from a subclass to one of its superclasses.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testProtectionEntryMaintenanceOnChangePropertyToSuperclass() throws Exception
	{
		// Change property from one subclass to superclass.
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);
		// create two OriginalObjects to give OriginalObject and SubClass
		// different IDs
		pm.saveObject(new OriginalObject());
		pm.saveObject(new OriginalObject());
		// create two SubClass, store them in ContainerObject
		SubClass ns = new SubClass();
		ns.setName("foo");
		ContainerObject co = new ContainerObject();
		co.setFoo(ns);
		pm.saveObject(co);

		ns = new SubClass();
		ns.setName("bar");
		co = new ContainerObject();
		co.setFoo(ns);
		pm.saveObject(co);
		pm.close();

		pm = new PersistenceManager(driver, database, login, password);
		// rename SubClass to a non-existing class, thus removing the subclass
		new TestTools(pm.getPersist()).changeName(SubClass.class, "com.github.conserveorm.NonExistingFakeClass", "FAKE_TABLE",
				"C__ARRAY_FAKE_TABLE");
		// change the database schema
		pm.updateSchema(OriginalObject.class);
		pm.close();

		pm = new PersistenceManager(driver, database, login, password);
		// make sure loading ContainerObject still contains the correct data
		List<ContainerObject> coList = pm.getObjects(ContainerObject.class, new All());
		assertEquals(2, coList.size());
		String zeroFoo = coList.get(0).getFoo().getName();
		String oneFoo = coList.get(1).getFoo().getName();
		assertTrue(zeroFoo.equals("bar") || oneFoo.equals("bar"));
		assertTrue(zeroFoo.equals("foo") || oneFoo.equals("foo"));
		// deleting all OriginalObject or SubClass should now result in no
		// change, as they are protected by ContainerObject
		// assert that there are still four OriginalObject and two SubClass
		// items
		assertEquals(4, pm.getCount(OriginalObject.class, new All()));
		assertEquals(0, pm.getCount(SubClass.class, new All()));
		// delete all OriginalObject/SubClass items
		pm.deleteObjects(OriginalObject.class, new All());
		assertEquals(2, pm.getCount(OriginalObject.class, new All()));
		// check that the contents are preserved
		coList = pm.getObjects(ContainerObject.class, new All());
		assertEquals(2, coList.size());
		assertTrue(zeroFoo.equals("bar") || oneFoo.equals("bar"));
		assertTrue(zeroFoo.equals("foo") || oneFoo.equals("foo"));
		assertEquals(0, pm.getCount(SubClass.class, new All()));
		pm.deleteObjects(SubClass.class);
		assertEquals(2, pm.getCount(OriginalObject.class, new All()));
		assertEquals(0, pm.getCount(SubClass.class, new All()));
		// deleting all ContainerObject should delete all SubClass and
		// OriginalObject, as they have no external reference
		pm.deleteObjects(ContainerObject.class, new All());
		assertEquals(0, pm.getCount(OriginalObject.class, new All()));
		assertEquals(0, pm.getCount(SubClass.class, new All()));

		pm.close();
	}

	/**
	 * Test if protection entries are correctly maintained when moving a field
	 * up or down in a class hierarchy.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testProtectionEntryMaintenanceOnMoveField() throws Exception
	{
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);

		// create some test data
		BeforeBottom bb = new BeforeBottom();
		bb.setFoo(new OriginalObject());
		pm.saveObject(bb);
		bb = new BeforeBottom();
		bb.setFoo(new OriginalObject());
		pm.saveObject(bb);

		// move a field up in the hierarchy
		TestTools tt = new TestTools(pm.getPersist());
		tt.changeName(BeforeBottom.class, AfterBottom.class);
		tt.changeName(BeforeTop.class, AfterTop.class);
		pm.updateSchema(AfterBottom.class);
		pm.close();

		// check that the protection entry has been updated to point from the
		// new containing class
		pm = new PersistenceManager(driver, database, login, password);
		ConnectionWrapper cw = pm.getConnectionWrapper();
		StringBuilder query = new StringBuilder("SELECT COUNT(*) FROM ");
		query.append(Defaults.HAS_A_TABLENAME);
		query.append(" WHERE OWNER_TABLE = ? AND PROPERTY_TABLE = ?");
		PreparedStatement ps = cw.prepareStatement(query.toString());
		AdapterBase adapter = pm.getPersist().getAdapter();
		NameGenerator.getTableName(AfterTop.class, adapter);
		ps.setInt(1, adapter.getPersist().getTableNameNumberMap().getNumber(cw, AfterTop.class));
		ps.setInt(2, adapter.getPersist().getTableNameNumberMap().getNumber(cw, OriginalObject.class));

		ResultSet rs = ps.executeQuery();
		if (rs.next())
		{
			assertEquals(2, rs.getInt(1));
		} 
		else
		{
			fail("No results returned");
		}
		ps.close();
		
		//check that the old protection entries are gone
		ps = cw.prepareStatement(query.toString());
		ps.setInt(1, adapter.getPersist().getTableNameNumberMap().getNumber(cw,AfterBottom.class));
		ps.setInt(2, adapter.getPersist().getTableNameNumberMap().getNumber(cw,OriginalObject.class));
		rs = ps.executeQuery();
		if (rs.next())
		{
			assertEquals(0, rs.getInt(1));
		} 
		else
		{
			fail("No results returned");
		}
		ps.close();
		pm.close();
	}

	/**
	 * Test if protection entries are correctly maintained when removing a field
	 * from a class.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testProtectionEntryMaintenanceOnDeleteField() throws Exception
	{
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);

		// Test 1: Create some entries that are part of another object
		OriginalObject oo = new OriginalObject();
		oo.setRedundantObject(new SimpleObject());
		pm.saveObject(oo);
		oo = new OriginalObject();
		oo.setRedundantObject(new SimpleObject());
		pm.saveObject(oo);
		assertEquals(2, pm.getCount(OriginalObject.class, new All()));
		assertEquals(2, pm.getCount(SimpleObject.class, new All()));
		// then drop that row from the containing object class
		new TestTools(pm.getPersist()).changeName(OriginalObject.class, RemovedColumn.class);
		// change the database schema
		pm.updateSchema(RemovedColumn.class);
		pm.close();

		pm = new PersistenceManager(driver, database, login, password);
		// make sure the contained objects are gone
		assertEquals(2, pm.getCount(RemovedColumn.class, new All()));
		assertEquals(0, pm.getCount(SimpleObject.class, new All()));
		pm.close();

		pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);

		// Test 2: Create some entries that are part of another object.
		oo = new OriginalObject();
		oo.setRedundantObject(new SimpleObject());
		pm.saveObject(oo);
		// also create an external reference for the contained object
		pm.saveObject(oo.getRedundantObject());
		oo = new OriginalObject();
		oo.setRedundantObject(new SimpleObject());
		pm.saveObject(oo);
		pm.saveObject(oo.getRedundantObject());
		assertEquals(2, pm.getCount(OriginalObject.class, new All()));
		assertEquals(2, pm.getCount(SimpleObject.class, new All()));
		// then drop that row from the containing object class
		new TestTools(pm.getPersist()).changeName(OriginalObject.class, RemovedColumn.class);
		pm.close();

		pm = new PersistenceManager(driver, database, login, password);
		// change the database schema
		pm.updateSchema(RemovedColumn.class);
		pm.close();

		pm = new PersistenceManager(driver, database, login, password);
		// make sure the contained objects are still there
		assertEquals(2, pm.getCount(RemovedColumn.class, new All()));
		assertEquals(2, pm.getCount(SimpleObject.class, new All()));

		// delete the former containing objects
		pm.deleteObjects(RemovedColumn.class, new All());
		// make sure former contained objects are still there
		assertEquals(0, pm.getCount(RemovedColumn.class, new All()));
		assertEquals(2, pm.getCount(SimpleObject.class, new All()));

		// drop the former containing object table
		pm.dropTable(RemovedColumn.class);
		// make sure the former contained objects are still there
		assertEquals(2, pm.getCount(SimpleObject.class, new All()));

		pm.close();
	}

	/**
	 * Test if protection entries are correctly maintained when changing the
	 * type of a field.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testProtectionEntryMaintenanceOnChangeField() throws Exception
	{
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);
		// Test 1: Create some entries that are part of another object
		OriginalObject oo = new OriginalObject();
		oo.setOtherObject(new SimpleObject());
		pm.saveObject(oo);
		oo = new OriginalObject();
		oo.setOtherObject(new SimpleObject());
		pm.saveObject(oo);
		assertEquals(2, pm.getCount(OriginalObject.class, new All()));
		assertEquals(2, pm.getCount(SimpleObject.class, new All()));
		// then change the field from reference to Long
		new TestTools(pm.getPersist()).changeName(OriginalObject.class, ObjectToLong.class);
		pm.updateSchema(ObjectToLong.class);
		pm.close();

		pm = new PersistenceManager(driver, database, login, password);
		// make sure the contained objects are gone
		assertEquals(2, pm.getCount(ObjectToLong.class, new All()));
		assertEquals(0, pm.getCount(SimpleObject.class, new All()));
		pm.close();

		pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);

		// Test 2: Create some entries that are part of another object.
		oo = new OriginalObject();
		oo.setOtherObject(new SimpleObject());
		pm.saveObject(oo);
		// save reference to the contained object
		pm.saveObject(oo.getOtherObject());
		oo = new OriginalObject();
		oo.setOtherObject(new SimpleObject());
		pm.saveObject(oo);
		pm.saveObject(oo.getOtherObject());
		// check that insertion worked
		assertEquals(2, pm.getCount(OriginalObject.class, new All()));
		assertEquals(2, pm.getCount(SimpleObject.class, new All()));
		// then change the field from reference to Long
		new TestTools(pm.getPersist()).changeName(OriginalObject.class, ObjectToLong.class);
		pm.updateSchema(ObjectToLong.class);
		pm.close();

		pm = new PersistenceManager(driver, database, login, password);
		// make sure the contained objects are still there
		assertEquals(2, pm.getCount(ObjectToLong.class, new All()));
		assertEquals(2, pm.getCount(SimpleObject.class, new All()));

		// delete the former containing objects
		pm.deleteObjects(ObjectToLong.class, new All());
		// make sure the former contained objects are still there
		assertEquals(0, pm.getCount(ObjectToLong.class, new All()));
		assertEquals(2, pm.getCount(SimpleObject.class, new All()));

		// drop the former containing class
		pm.dropTable(ObjectToLong.class);
		// make sure the former contained objects are still there
		assertEquals(2, pm.getCount(SimpleObject.class, new All()));

		pm.close();
	}

	/**
	 * Test if protection entries are correctly maintained when removing an
	 * interface from a class.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testProtectionEntryMaintenanceOnDeleteInterface() throws Exception
	{
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);

		// Test 1: Create some entries that are part of another object
		// the containing object references an interface that the contained
		// objects implement
		FooContainerOwner fco = new FooContainerOwner();
		fco.setFooContainer(new MyFooContainer());
		pm.saveObject(fco);
		fco = new FooContainerOwner();
		fco.setFooContainer(new MyFooContainer());
		pm.saveObject(fco);
		// make sure everything is saved
		assertEquals(2, pm.getCount(FooContainerOwner.class, new All()));
		assertEquals(2, pm.getCount(MyFooContainer.class, new All()));
		assertEquals(2, pm.getCount(FooContainer.class, new All()));
		assertEquals(4, pm.getCount(Object.class, new All()));
		// remove the interface from the contained objects
		new TestTools(pm.getPersist()).changeName(MyFooContainer.class, MyNonFooContainer.class);
		pm.updateSchema(MyNonFooContainer.class);
		pm.close();
		// make sure the contained objects are gone
		pm = new PersistenceManager(driver, database, login, password);
		assertEquals(2, pm.getCount(FooContainerOwner.class, new All()));
		assertEquals(0, pm.getCount(MyNonFooContainer.class, new All()));
		assertEquals(0, pm.getCount(FooContainer.class, new All()));
		assertEquals(2, pm.getCount(Object.class, new All()));
		pm.close();

		pm = new PersistenceManager(driver, database, login, password);
		pm.dropTable(Object.class);
		pm.close();

		// Test 2: Create some entries that are part of another object
		pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		fco = new FooContainerOwner();
		fco.setFooContainer(new MyFooContainer());
		pm.saveObject(fco);
		// create external reference to contained object
		pm.saveObject(fco.getFooContainer());
		fco = new FooContainerOwner();
		fco.setFooContainer(new MyFooContainer());
		pm.saveObject(fco);
		pm.saveObject(fco.getFooContainer());
		// make sure everything is saved
		assertEquals(2, pm.getCount(FooContainerOwner.class, new All()));
		assertEquals(2, pm.getCount(MyFooContainer.class, new All()));
		assertEquals(2, pm.getCount(FooContainer.class, new All()));
		// remove the interface from the contained objects
		new TestTools(pm.getPersist()).changeName(MyFooContainer.class, MyNonFooContainer.class);
		pm.updateSchema(MyNonFooContainer.class);
		pm.close();
		// make sure the contained objects are still there.
		pm = new PersistenceManager(driver, database, login, password);
		assertEquals(2, pm.getCount(FooContainerOwner.class, new All()));
		assertEquals(2, pm.getCount(MyNonFooContainer.class, new All()));
		assertEquals(0, pm.getCount(FooContainer.class, new All()));

		// delete the FooContainerOwner
		pm.deleteObjects(FooContainerOwner.class, new All());
		// make sure MyNonFooContainer objects are still there
		assertEquals(2, pm.getCount(MyNonFooContainer.class, new All()));

		// drop FooContainerOwern table
		pm.dropTable(FooContainerOwner.class);
		// make sure MyNonFooContainer objects are still there
		assertEquals(2, pm.getCount(MyNonFooContainer.class, new All()));

		pm.close();
	}

	/**
	 * Test that searching for classes that do not exist does not throw an
	 * exception.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testNonExistingClass() throws Exception
	{
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);
		pm.close();

		pm = new PersistenceManager(driver, database, login, password);
		// test deleting object of non-existing class
		assertEquals(0, pm.deleteObjects(NonExistingClass.class, new All()));
		// test counting objects of non-existing class
		assertEquals(0, pm.getCount(NonExistingClass.class, new All()));
		assertEquals(0, pm.getCount(new NonExistingClass()));
		// test sum of object of non-existing class
		assertEquals(0.0,pm.calculateAggregate(NonExistingClass.class, new Sum("getFoo"), new All()));
		//test max, min, avg of non-existing class
		AggregateFunction[] functions = new AggregateFunction[]{new Maximum("getFoo"),new Minimum("getFoo"),new Average("getFoo")};
		Number[] numbers = pm.calculateAggregate(NonExistingClass.class, functions, new All());
		assertEquals(3,numbers.length);
		for(int x = 0;x<numbers.length;x++)
		{
			assertTrue("Result is not Double but "+ numbers[x].getClass(),numbers[x] instanceof Double);
			assertTrue(((Double)numbers[x]).isNaN());
		}
		functions = new AggregateFunction[]{new Maximum("getBar"),new Minimum("getBar"),new Average("getBar")};
		numbers = pm.calculateAggregate(NonExistingClass.class, functions, new All());
		assertEquals(3,numbers.length);
		for(int x = 0;x<numbers.length-1;x++)
		{
			assertTrue("Result is not Float but "+ numbers[x].getClass(),numbers[x] instanceof Float);
			assertTrue(((Float)numbers[x]).isNaN());
		}
		assertTrue("Result is not Double but "+ numbers[2].getClass(),numbers[2] instanceof Double);
		assertTrue(((Double)numbers[2]).isNaN());
		
		// test getting objects of non-existing class
		List<NonExistingClass> res = pm.getObjects(NonExistingClass.class, new All());
		assertEquals(0, res.size());
		res = pm.getObjects(new NonExistingClass());
		assertEquals(0, res.size());
		pm.close();
	}

	/**
	 * Test adding and removing indices.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testIndex() throws Exception
	{
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);

		WithIndex wi = new WithIndex();
		wi.setValue("foobar");
		pm.saveObject(wi);
		pm.close();

		// change to object without indices
		pm = new PersistenceManager(driver, database, login, password);
		new TestTools(pm.getPersist()).changeName(WithIndex.class, WithoutIndex.class);
		pm.updateSchema(WithoutIndex.class);
		pm.close();

		pm = new PersistenceManager(driver, database, login, password);
		List<WithoutIndex> res1 = pm.getObjects(WithoutIndex.class, new All());
		assertEquals(1, res1.size());
		assertEquals("foobar", res1.get(0).getValue());
		pm.close();

		// change back to object with indices
		pm = new PersistenceManager(driver, database, login, password);
		new TestTools(pm.getPersist()).changeName(WithoutIndex.class, WithIndex.class);
		pm.updateSchema(WithIndex.class);
		pm.close();

		pm = new PersistenceManager(driver, database, login, password);
		List<WithIndex> res2 = pm.getObjects(WithIndex.class, new All());
		assertEquals(1, res2.size());
		assertEquals("foobar", res2.get(0).getValue());
		pm.close();
	}

	/**
	 * Test on functionality to iterate over search results instead of returning
	 * all as a list.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testIterativeSearch() throws Exception
	{
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);

		// create 42 simpleobjects
		final int testCount = 42;
		for (int x = 0; x < testCount; x++)
		{
			SimpleObject so = new SimpleObject();
			// reverse order of COUNT column relative to C__ID
			so.setCount(testCount - x);
			pm.saveObject(so);
		}

		// make sure all objects are still there
		assertEquals(testCount, pm.getCount(SimpleObject.class, new All()));

		// use an array to get around inner class limitations
		final int[] numberFound = new int[2];
		// find all objects
		pm.getObjects(SimpleObject.class, new SearchListener<SimpleObject>()
		{
			@Override
			public void objectFound(SimpleObject object)
			{
				if (numberFound[0] > 0)
				{
					// check that we're going descending order
					assertEquals(numberFound[1] - 1, object.getCount());
				}
				numberFound[0]++;
				numberFound[1] = object.getCount();
			}
		}, new All());
		assertEquals(testCount, numberFound[0]);

		// get a subset of finds, ordered by COUNT column
		final int startAt = 5;
		final int totalCount = 26;
		SimpleObject orderObject = new SimpleObject();
		orderObject.setCount(1);
		numberFound[0] = 0;
		numberFound[1] = startAt;
		pm.getObjects(SimpleObject.class, new SearchListener<SimpleObject>()
		{
			@Override
			public void objectFound(SimpleObject object)
			{
				// check that we're going ascending order
				assertEquals(numberFound[1] + 1, object.getCount());
				numberFound[0]++;
				numberFound[1] = object.getCount();
			}
		}, new All(), new Order(totalCount, startAt, new Ascending(orderObject)));
		assertEquals(totalCount, numberFound[0]);
		
		//make sure we get the all objects when we search by superclass
		numberFound[0] = 0;
		numberFound[1] = 0;
		pm.getObjects(Object.class, new SearchListener<Object>()
		{
			@Override
			public void objectFound(Object o)
			{
				if (o instanceof SimpleObject)
				{
					SimpleObject object = (SimpleObject) o;
					if (numberFound[0] > 0)
					{
						// check that we're going descending order
						assertEquals(numberFound[1] - 1, object.getCount());
					}
					numberFound[0]++;
					numberFound[1] = object.getCount();
				}
			}
		}, new All());
		assertEquals(testCount, numberFound[0]);

		pm.close();
		

		//make sure we get the all objects when we search by superclass, even from a new PM
		pm = new PersistenceManager(driver, database, login, password);
		numberFound[0] = 0;
		numberFound[1] = 0;
		pm.getObjects(Object.class, new SearchListener<Object>()
		{
			@Override
			public void objectFound(Object o)
			{
				if (o instanceof SimpleObject)
				{
					SimpleObject object = (SimpleObject) o;
					if (numberFound[0] > 0)
					{
						// check that we're going descending order
						assertEquals(numberFound[1] - 1, object.getCount());
					}
					numberFound[0]++;
					numberFound[1] = object.getCount();
				}
			}
		}, new All());
		assertEquals(testCount, numberFound[0]);

		pm.close();
	}

	/**
	 * Try adding two objects that implement a sortable interface and sorting on
	 * the interface.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testSortingInterface() throws Exception
	{
		PersistenceManager pm = null;
		if(driver != null)
		{
			pm = new PersistenceManager(driver, database, login, password);
		}
		else
		{
			//we're dealing with a 4.0 or better driver, omit driver specification.
			pm = new PersistenceManager( database, login, password);
		}
		// drop all tables
		pm.dropTable(Object.class);

		// create test data
		for (int x = 0; x < 10; x++)
		{
			// alternate FooSortable and BarSortable
			Sortable s = null;
			if (x % 2 == 0)
			{
				s = new FooSortable();
			} else
			{
				s = new BarSortable();
			}
			s.setFoo(x);
			pm.saveObject(s);
		}

		// retrieve the test data, sorted ascending
		FooSortable sortObject = new FooSortable();
		sortObject.setFoo(1);
		List<Sortable> sortable = pm.getObjects(Sortable.class, new All(), new Ascending(sortObject, Sortable.class));
		assertEquals(10, sortable.size());

		// make sure everything is in the right order
		int lastValue = 0;
		for (Sortable s : sortable)
		{
			assertEquals(lastValue, s.getFoo().intValue());
			lastValue++;
		}
		

		// retrieve the test data, sorted descending
		sortable = pm.getObjects(Sortable.class, new All(), new Descending(sortObject, Sortable.class));
		assertEquals(10, sortable.size());

		// make sure everything is in the right order
		for (Sortable s : sortable)
		{
			lastValue--;
			assertEquals(lastValue, s.getFoo().intValue());
		}

		pm.close();
	}
	
	/**
	 * Test whether null values are preserved on save and load.
	 * 
	 */
	@Test
	public void testNullValuePreservation() throws Exception
	{
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		// drop all tables
		pm.dropTable(Object.class);
		SimpleObject so = new SimpleObject();
		pm.saveObject(so);
		pm.close();
		
		pm = new PersistenceManager(driver, database, login, password);
		List<SimpleObject> sos = pm.getObjects(SimpleObject.class, new All());
		assertEquals(1,sos.size());
		SimpleObject first = sos.get(0);
		assertNull(first.getName());
		assertNull(first.getScale());
		assertNull(first.getAge());
		pm.close();
	}
	
	/**
	 * Test if all C__HAS_A entries are deleted after deleting objects.
	 */
	@Test
	public void testProtectionEntryDeletion() throws Exception
	{

		// create the database connection
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		persist.dropTable(Object.class);

		ArrayContainingObject aco = new ArrayContainingObject();
		aco.setDataarray(new double[] { 1.1, 2.2, 3.3, 4.4 });
		// saving the object gives it an outside reference
		persist.saveObject(aco);
		persist.saveObject(aco);

		//delete the ComplexObject
		persist.deleteObjects(ArrayContainingObject.class, new All());
		
		
		//make sure the C__HAS_A table is empty
		ConnectionWrapper cw = persist.getConnectionWrapper();
		Connection c = cw.getConnection();
		PreparedStatement ps = c.prepareStatement("SELECT COUNT(*) FROM " + Defaults.HAS_A_TABLENAME);
		ResultSet rs = ps.executeQuery();
		if(rs.next())
		{
			long count = rs.getLong(1);
			assertEquals(0,count);
		}
		else
		{
			fail("Could not get count from "+Defaults.HAS_A_TABLENAME);
		}
		

		persist.close();
	}
	
	/**
	 * Test some prohibited column names.
	 * 
	 */
	@Test
	public void testBadColumnNames() throws Exception
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		BadColumnNames bad=new BadColumnNames();
		bad.setCount(1234);
		bad.setKey("the key");
		persist.saveObject(bad);
		persist.close();
		
		//get a new connection
		persist = new PersistenceManager(driver, database, login, password);
		BadColumnNames bcn = persist.getObjects(BadColumnNames.class, new All()).get(0);
		assertEquals(bad.getCount(),bcn.getCount());
		assertEquals(bad.getKey(),bcn.getKey());
		persist.close();
		
		
	}
	
	/**
	 * Test searches on interfaces with one implementing object as example.
	 * 
	 */
	@Test
	public void testInterfaceSearches() throws Exception
	{
		PersistenceManager persist = new PersistenceManager(driver, database, login, password);
		//create some test data
		for(int x =0;x<100;x++)
		{
			MyFooContainer mfc = new MyFooContainer();
			mfc.setFoo(Integer.toString(x));
			persist.saveObject(mfc);
		}
		persist.saveObject(new MyFooContainer());
		persist.close();
		
		persist = new PersistenceManager(driver, database, login, password);
		List<FooContainer> values = null;
		MyFooContainer obj = new MyFooContainer();
		obj.setFoo("42");
		//get all FooContainer where foo is null
		values = persist.getObjects(FooContainer.class, new Null(obj,FooContainer.class) );
		assertEquals(1,values.size());
		//get all FooContainer where foo is not null
		values = persist.getObjects(FooContainer.class, new NotNull(obj,FooContainer.class) );
		assertEquals(100,values.size());
		//get all FooContainer where foo is different from "42"
		values = persist.getObjects(FooContainer.class, new Different(obj,FooContainer.class) );
		assertEquals(99,values.size());
		//get all FooContainer where foo is greater than "42"
		values = persist.getObjects(FooContainer.class, new Greater(obj,FooContainer.class) );
		assertEquals(62,values.size());
		//get all FooContainer where foo is greater than or equal to "42"
		values = persist.getObjects(FooContainer.class, new GreaterOrEqual(obj,FooContainer.class) );
		assertEquals(63,values.size());
		//get all FooContainer where foo is less than "42"
		values = persist.getObjects(FooContainer.class, new Less(obj,FooContainer.class) );
		assertEquals(37,values.size());
		//get all FooContainer where foo is less than or equal to "42"
		values = persist.getObjects(FooContainer.class, new LessOrEqual(obj,FooContainer.class) );
		assertEquals(38,values.size());
		
		//get all FooContainer where foo is null
		values = persist.getObjects(FooContainer.class, new Null(obj) );
		assertEquals(1,values.size());
		//get all FooContainer where foo is not null
		values = persist.getObjects(FooContainer.class, new NotNull(obj) );
		assertEquals(100,values.size());
		//get all FooContainer where foo is different from "42"
		values = persist.getObjects(FooContainer.class, new Different(obj) );
		assertEquals(99,values.size());
		//get all FooContainer where foo is greater than "42"
		values = persist.getObjects(FooContainer.class, new Greater(obj) );
		assertEquals(62,values.size());
		//get all FooContainer where foo is greater than or equal to "42"
		values = persist.getObjects(FooContainer.class, new GreaterOrEqual(obj) );
		assertEquals(63,values.size());
		//get all FooContainer where foo is less than "42"
		values = persist.getObjects(FooContainer.class, new Less(obj) );
		assertEquals(37,values.size());
		//get all FooContainer where foo is less than or equal to "42"
		values = persist.getObjects(FooContainer.class, new LessOrEqual(obj) );
		assertEquals(38,values.size());

		List<MyFooContainer>values2=null;
		//get all FooContainer where foo is null
		values2 = persist.getObjects(MyFooContainer.class, new Null(obj,MyFooContainer.class) );
		assertEquals(1,values2.size());
		//get all FooContainer where foo is not null
		values2 = persist.getObjects(MyFooContainer.class, new NotNull(obj,MyFooContainer.class) );
		assertEquals(100,values2.size());
		//get all FooContainer where foo is different from "42"
		values2 = persist.getObjects(MyFooContainer.class, new Different(obj,MyFooContainer.class) );
		assertEquals(99,values2.size());
		//get all FooContainer where foo is greater than "42"
		values2 = persist.getObjects(MyFooContainer.class, new Greater(obj,MyFooContainer.class) );
		assertEquals(62,values2.size());
		//get all FooContainer where foo is greater than or equal to "42"
		values2 = persist.getObjects(MyFooContainer.class, new GreaterOrEqual(obj,MyFooContainer.class) );
		assertEquals(63,values2.size());
		//get all FooContainer where foo is less than "42"
		values2 = persist.getObjects(MyFooContainer.class, new Less(obj,MyFooContainer.class) );
		assertEquals(37,values2.size());
		//get all FooContainer where foo is less than or equal to "42"
		values2 = persist.getObjects(MyFooContainer.class, new LessOrEqual(obj,MyFooContainer.class) );
		assertEquals(38,values2.size());

		//get all FooContainer where foo is null
		values2 = persist.getObjects(MyFooContainer.class, new Null(obj) );
		assertEquals(1,values2.size());
		//get all FooContainer where foo is not null
		values2 = persist.getObjects(MyFooContainer.class, new NotNull(obj) );
		assertEquals(100,values2.size());
		//get all FooContainer where foo is different from "42"
		values2 = persist.getObjects(MyFooContainer.class, new Different(obj) );
		assertEquals(99,values2.size());
		//get all FooContainer where foo is greater than "42"
		values2 = persist.getObjects(MyFooContainer.class, new Greater(obj) );
		assertEquals(62,values2.size());
		//get all FooContainer where foo is greater than or equal to "42"
		values2 = persist.getObjects(MyFooContainer.class, new GreaterOrEqual(obj) );
		assertEquals(63,values2.size());
		//get all FooContainer where foo is less than "42"
		values2 = persist.getObjects(MyFooContainer.class, new Less(obj) );
		assertEquals(37,values2.size());
		//get all FooContainer where foo is less than or equal to "42"
		values2 = persist.getObjects(MyFooContainer.class, new LessOrEqual(obj) );
		assertEquals(38,values2.size());
		
		

		//get all FooContainer where foo is null
		values = persist.getObjects(FooContainer.class, new Null(obj,FooContainer.class,false) );
		assertEquals(1,values.size());
		//get all FooContainer where foo is not null
		values = persist.getObjects(FooContainer.class, new NotNull(obj,FooContainer.class,false) );
		assertEquals(100,values.size());
		//get all FooContainer where foo is different from "42"
		values = persist.getObjects(FooContainer.class, new Different(obj,FooContainer.class,false) );
		assertEquals(99,values.size());
		//get all FooContainer where foo is greater than "42"
		values = persist.getObjects(FooContainer.class, new Greater(obj,FooContainer.class,false) );
		assertEquals(62,values.size());
		//get all FooContainer where foo is greater than or equal to "42"
		values = persist.getObjects(FooContainer.class, new GreaterOrEqual(obj,FooContainer.class,false) );
		assertEquals(63,values.size());
		//get all FooContainer where foo is less than "42"
		values = persist.getObjects(FooContainer.class, new Less(obj,FooContainer.class,false) );
		assertEquals(37,values.size());
		//get all FooContainer where foo is less than or equal to "42"
		values = persist.getObjects(FooContainer.class, new LessOrEqual(obj,FooContainer.class,false) );
		assertEquals(38,values.size());
		
		//get all FooContainer where foo is null
		values = persist.getObjects(FooContainer.class, new Null(obj,false) );
		assertEquals(1,values.size());
		//get all FooContainer where foo is not null
		values = persist.getObjects(FooContainer.class, new NotNull(obj,false) );
		assertEquals(100,values.size());
		//get all FooContainer where foo is different from "42"
		values = persist.getObjects(FooContainer.class, new Different(obj,false) );
		assertEquals(99,values.size());
		//get all FooContainer where foo is greater than "42"
		values = persist.getObjects(FooContainer.class, new Greater(obj,false) );
		assertEquals(62,values.size());
		//get all FooContainer where foo is greater than or equal to "42"
		values = persist.getObjects(FooContainer.class, new GreaterOrEqual(obj,false) );
		assertEquals(63,values.size());
		//get all FooContainer where foo is less than "42"
		values = persist.getObjects(FooContainer.class, new Less(obj,false) );
		assertEquals(37,values.size());
		//get all FooContainer where foo is less than or equal to "42"
		values = persist.getObjects(FooContainer.class, new LessOrEqual(obj,false) );
		assertEquals(38,values.size());

		//get all FooContainer where foo is null
		values2 = persist.getObjects(MyFooContainer.class, new Null(obj,MyFooContainer.class,false) );
		assertEquals(1,values2.size());
		//get all FooContainer where foo is not null
		values2 = persist.getObjects(MyFooContainer.class, new NotNull(obj,MyFooContainer.class,false) );
		assertEquals(100,values2.size());
		//get all FooContainer where foo is different from "42"
		values2 = persist.getObjects(MyFooContainer.class, new Different(obj,MyFooContainer.class,false) );
		assertEquals(99,values2.size());
		//get all FooContainer where foo is greater than "42"
		values2 = persist.getObjects(MyFooContainer.class, new Greater(obj,MyFooContainer.class,false) );
		assertEquals(62,values2.size());
		//get all FooContainer where foo is greater than or equal to "42"
		values2 = persist.getObjects(MyFooContainer.class, new GreaterOrEqual(obj,MyFooContainer.class,false) );
		assertEquals(63,values2.size());
		//get all FooContainer where foo is less than "42"
		values2 = persist.getObjects(MyFooContainer.class, new Less(obj,MyFooContainer.class,false) );
		assertEquals(37,values2.size());
		//get all FooContainer where foo is less than or equal to "42"
		values2 = persist.getObjects(MyFooContainer.class, new LessOrEqual(obj,MyFooContainer.class,false) );
		assertEquals(38,values2.size());

		//get all FooContainer where foo is null
		values2 = persist.getObjects(MyFooContainer.class, new Null(obj,false) );
		assertEquals(1,values2.size());
		//get all FooContainer where foo is not null
		values2 = persist.getObjects(MyFooContainer.class, new NotNull(obj,false) );
		assertEquals(100,values2.size());
		//get all FooContainer where foo is different from "42"
		values2 = persist.getObjects(MyFooContainer.class, new Different(obj,false) );
		assertEquals(99,values2.size());
		//get all FooContainer where foo is greater than "42"
		values2 = persist.getObjects(MyFooContainer.class, new Greater(obj,false) );
		assertEquals(62,values2.size());
		//get all FooContainer where foo is greater than or equal to "42"
		values2 = persist.getObjects(MyFooContainer.class, new GreaterOrEqual(obj,false) );
		assertEquals(63,values2.size());
		//get all FooContainer where foo is less than "42"
		values2 = persist.getObjects(MyFooContainer.class, new Less(obj,false) );
		assertEquals(37,values2.size());
		//get all FooContainer where foo is less than or equal to "42"
		values2 = persist.getObjects(MyFooContainer.class, new LessOrEqual(obj,false) );
		assertEquals(38,values2.size());
		

		persist.close();
	}
	
	/**
	 * Test basic database connection functions.
	 */
	@Test 
	public void testDatabasePool() throws Exception
	{
		//test normal operation
		DataConnectionPool dcp = new DataConnectionPool(1, driver, database, login, password,new Properties());
		dcp.getConnectionWrapper().commitAndDiscard();
		dcp.getConnectionWrapper().rollbackAndDiscard();
		//get several connection wrappers
		ConnectionWrapper cw1 = dcp.getConnectionWrapper();
		ConnectionWrapper cw2 = dcp.getConnectionWrapper();
		ConnectionWrapper cw3 = dcp.getConnectionWrapper();
		assertNotNull(cw1);
		assertNotNull(cw2);
		assertNotNull(cw3);
		assertTrue(cw1.isTaken());
		cw1.discard();
		assertFalse(cw1.isTaken());
		dcp.cleanUp();
		assertTrue(cw1.isTaken());
		
		//test exceptions
		boolean thrown = false;
		try
		{
			dcp = new DataConnectionPool(1,driver,null,login,password,new Properties());
			dcp.cleanUp();
		}
		catch(SQLException e)
		{
			thrown = true;
		}
		assertTrue("No exception thrown on null database.",thrown);
		thrown = false;
		try
		{
			dcp = new DataConnectionPool(1,"completely fake driver",database,login,password,new Properties());
			dcp.cleanUp();
		}
		catch(SQLException e)
		{
			thrown = true;
		}
		assertTrue("No exception thrown on fake database.",thrown);
	}
	
	/**
	 * Test if rollback actually works.
	 */
	@Test
	public void testRollback() throws Exception
	{
		DataConnectionPool dcp = new DataConnectionPool(1, driver, database, login, password,new Properties());
		ConnectionWrapper cw = dcp.getConnectionWrapper();
		Connection c = cw.getConnection();

		// drop fake table
		PreparedStatement ps = null;
		try
		{
			ps = c.prepareStatement("DROP TABLE TEST_TABLE ");
			ps.execute();
			ps.close();
			cw.commit();
		}
		catch (Exception e)
		{
			// we don't care if this is thrown, it just means the table does not
			// exist
			cw.rollback();
		}

		// create a fake table
		ps = c.prepareStatement("CREATE TABLE TEST_TABLE (FOO INT)");
		ps.execute();
		ps.close();
		cw.commit();

		// insert some fake data
		ps = c.prepareStatement("INSERT INTO TEST_TABLE (FOO) VALUES(25)");
		ps.execute();
		ps.close();
		// roll back operation
		cw.rollback();

		// make sure there is no fake data
		ps = c.prepareStatement("SELECT COUNT(*) FROM TEST_TABLE");
		ResultSet rs = ps.executeQuery();
		assertTrue(rs.next());
		assertEquals(0, rs.getInt(1));
		ps.close();

		// drop fake table
		ps = c.prepareStatement("DROP TABLE TEST_TABLE ");
		ps.execute();
		ps.close();

		// we're done with the connection
		cw.commitAndDiscard();

		dcp.cleanUp();

	}
	
	/**
	 * Test exception is thrown if we try to update a schema when we're not allowed.
	 * 
	 */
	@Test
	public void testSchemaPermissionException() throws Exception
	{
		boolean thrown = false;
		PersistenceManager persist = null;
		
		//create a normal PersistManager
		persist = new PersistenceManager(driver, database, login, password);
		assertTrue(persist.getPersist().isCreateSchema());
		persist.getPersist().setCreateSchema(false);
		assertFalse(persist.getPersist().isCreateSchema());
		persist.getPersist().setCreateSchema(true);
		assertTrue(persist.getPersist().isCreateSchema());
		persist.close();
		persist = new PersistenceManager(driver, database, login, password, false);
		assertFalse(persist.getPersist().isCreateSchema());
		persist.close();
		
		
		try
		{
			persist = new PersistenceManager(driver, database, login, password);
			ConnectionWrapper cw = persist.getConnectionWrapper();
			Connection c = cw.getConnection();
			c.prepareStatement("DROP TABLE " + Defaults.HAS_A_TABLENAME).execute();
			cw.commitAndDiscard();			
		}
		catch (Exception e)
		{
			// don't care, this just means the table didn't exist
		}
		finally
		{
			if (persist != null)
			{
				persist.close();
			}
		}

		try
		{
			// create a manager that doesn't allow schema updates
			persist = new PersistenceManager(driver, database, login, password, false);
		}
		catch (SQLException e)
		{
			if (e.getCause() instanceof SchemaPermissionException)
			{
				thrown = true;
			}
		}
		finally
		{
			if (persist != null)
			{
				persist.close();
			}
		}
		assertTrue("Could update schema despite not having permission.", thrown);
	}
	
	/**
	 * Test initialising from a properties file.
	 */
	@Test
	public void testPropertiesFile() throws Exception
	{

 	   	File temp = File.createTempFile("temporary_test", ".prop");
 	   	String filename = temp.getAbsolutePath();
		//create a file for the properties
		FileWriter fw = new FileWriter(filename);

		// test all values correctly set
		if (driver != null)
		{
			fw.write("com.github.conserveorm.driver=" + driver + "\n");
		}
		fw.write("com.github.conserveorm.connectionstring=" + database + "\n");
		if(login!=null)
		{
			fw.write("com.github.conserveorm.username=" + login + "\n");
		}
		if(password != null)
		{
			fw.write("com.github.conserveorm.password=" + password + "\n");
		}
		fw.close();
		
		PersistenceManager persist = new PersistenceManager(filename);
		persist.saveObject(new Object());
		assertEquals(1,persist.getCount(Object.class, new All()));
		persist.dropTable(Object.class);
		persist.close();
		
		FileInputStream fis = new FileInputStream(filename);
		persist = new PersistenceManager(fis);
		persist.saveObject(new Object());
		assertEquals(1,persist.getCount(Object.class, new All()));
		persist.dropTable(Object.class);
		persist.close();
		fis.close();
		
		Properties prop = new Properties();
		fis = new FileInputStream(filename);
		prop.load(fis);
		persist = new PersistenceManager(prop);
		persist.saveObject(new Object());
		assertEquals(1,persist.getCount(Object.class, new All()));
		persist.dropTable(Object.class);
		persist.close();
		fis.close();
		
		File f = new File(filename);
		f.delete();
		
		//check if exeptions are thrown at the right place
		boolean thrown = false;

		//no database
		fw = new FileWriter(filename);
		if (driver != null)
		{
			fw.write("com.github.conserveorm.driver=" + driver + "\n");
		}
		fw.write("com.github.conserveorm.username="+login+"\n");
		fw.write("com.github.conserveorm.password="+password+"\n");
		fw.close();
		try
		{
			persist = new PersistenceManager(filename);
		}
		catch(SQLException e)
		{
			thrown = true;
		}
		finally
		{
			if(persist != null)
			{
				persist.close();
				persist = null;
			}
		}
		assertTrue("No exception on non-existing database.",thrown);
		thrown =false;
		f = new File(filename);
		f.delete();
				
	}
	
	/**
	 * Test sum, max, min, avg for all possible data types.
	 */
	@Test
	public void testAllDataTypesAggregate() throws Exception
	{
		PersistenceManager pm = new PersistenceManager(driver,database,login,password);
		for(int x = 1;x<=100;x++)
		{
			AllPrimitives ap = new AllPrimitives();
			if(x%2==0)
			{
				ap.setBoolobject(Boolean.TRUE);
				ap.setBoolvalue(true);
			}
			else
			{
				ap.setBoolobject(Boolean.FALSE);
				ap.setBoolvalue(false);
			}
			ap.setByteobject((byte)x);
			ap.setBytevalue((byte)x);
			ap.setCharobject((char)x);
			ap.setCharvalue((char)x);
			ap.setDoubleobject((double)x);
			ap.setDoublevalue((double)x);
			ap.setFloatobject((float)x);
			ap.setFloatvalue((float)x);
			ap.setIntobject(x);
			ap.setIntvalue(x);
			ap.setLongobject((long)x);
			ap.setLongvalue(x);
			ap.setShortobject((short)x);
			ap.setShortvalue((short)x);
			
			pm.saveObject(ap);
		}
		
		//calculate all sums
		AggregateFunction [] functions = new AggregateFunction[]{
				new Sum("getByteobject"),
				new Sum("getBytevalue"),
				new Sum("getCharobject"),
				new Sum("getCharvalue"),
				new Sum("getDoubleobject"),
				new Sum("getDoublevalue"),
				new Sum("getFloatobject"),
				new Sum("getFloatvalue"),
				new Sum("getIntobject"),
				new Sum("getIntvalue"),
				new Sum("getLongobject"),
				new Sum("getLongvalue"),
				new Sum("getShortobject"),
				new Sum("getShortvalue")
		};
		Number[] sums = pm.calculateAggregate(AllPrimitives.class, functions, new All());
		for(int x = 0;x<sums.length;x++)
		{
			assertEquals(5050,sums[x].longValue());
		}
		
		//calculate all maximums
		functions = new AggregateFunction[]{
				new Maximum("getByteobject"),
				new Maximum("getBytevalue"),
				new Maximum("getCharobject"),
				new Maximum("getCharvalue"),
				new Maximum("getDoubleobject"),
				new Maximum("getDoublevalue"),
				new Maximum("getFloatobject"),
				new Maximum("getFloatvalue"),
				new Maximum("getIntobject"),
				new Maximum("getIntvalue"),
				new Maximum("getLongobject"),
				new Maximum("getLongvalue"),
				new Maximum("getShortobject"),
				new Maximum("getShortvalue")
		};
		Number[] maxs = pm.calculateAggregate(AllPrimitives.class, functions, new All());
		for(int x = 0;x<maxs.length;x++)
		{
			assertEquals(100,maxs[x].longValue());
		}
		//calculate all minimums
		functions = new AggregateFunction[]{
				new Minimum("getByteobject"),
				new Minimum("getBytevalue"),
				new Minimum("getCharobject"),
				new Minimum("getCharvalue"),
				new Minimum("getDoubleobject"),
				new Minimum("getDoublevalue"),
				new Minimum("getFloatobject"),
				new Minimum("getFloatvalue"),
				new Minimum("getIntobject"),
				new Minimum("getIntvalue"),
				new Minimum("getLongobject"),
				new Minimum("getLongvalue"),
				new Minimum("getShortobject"),
				new Minimum("getShortvalue")
		};
		Number[] mins = pm.calculateAggregate(AllPrimitives.class, functions, new All());
		for(int x = 0;x<mins.length;x++)
		{
			assertEquals(1,mins[x].longValue());
		}
		//calculate all averages
		functions = new AggregateFunction[]{
				new Average("getByteobject"),
				new Average("getBytevalue"),
				new Average("getCharobject"),
				new Average("getCharvalue"),
				new Average("getDoubleobject"),
				new Average("getDoublevalue"),
				new Average("getFloatobject"),
				new Average("getFloatvalue"),
				new Average("getIntobject"),
				new Average("getIntvalue"),
				new Average("getLongobject"),
				new Average("getLongvalue"),
				new Average("getShortobject"),
				new Average("getShortvalue")
		};
		Number[] avgs = pm.calculateAggregate(AllPrimitives.class, functions, new All());
		for(int x = 0;x<avgs.length;x++)
		{
			assertEquals(50.5,avgs[x].doubleValue(),0.0001);
		}
		
		//try using the ConnectionWrapper method
		ConnectionWrapper cw = pm.getConnectionWrapper();
		double avg = (double) pm.calculateAggregate(cw, AllPrimitives.class, new Average("getDoubleobject"), new All());
		assertEquals(50.5,avg,0.0001);
		
		//delete all AllPrimitives
		pm.dropTable(cw,AllPrimitives.class);

		//test all integer types return a zero long value
		functions = new AggregateFunction[]{
				new Sum("getByteobject"),
				new Sum("getBytevalue"),
				new Sum("getCharobject"),
				new Sum("getCharvalue"),
				new Sum("getIntobject"),
				new Sum("getIntvalue"),
				new Sum("getLongobject"),
				new Sum("getLongvalue"),
				new Sum("getShortobject"),
				new Sum("getShortvalue")
		};
		Number[] num = pm.calculateAggregate(cw,AllPrimitives.class, functions, new All());
		assertEquals(functions.length,num.length);
		for(int x =0;x<num.length;x++)
		{
			assertTrue(num[x] instanceof Long);
			assertEquals(0l,num[x].longValue());
		}
		
		//test all float types return a zero double value
		functions = new AggregateFunction[]{
				new Sum("getDoubleobject"),
				new Sum("getDoublevalue"),
				new Sum("getFloatobject"),
				new Sum("getFloatvalue")
		};
		num = pm.calculateAggregate(cw,AllPrimitives.class, functions, new All());
		assertEquals(functions.length,num.length);
		for(int x =0;x<num.length;x++)
		{
			assertTrue(num[x] instanceof Double);
			assertEquals(0l,num[x].doubleValue(),0.0001);
		}
		cw.commitAndDiscard();
		
		pm.close();
	}
	
	/**
	 * Test if getting an object by its database ID works.
	 */
	@Test
    public void testGetObjectById() throws Exception
    {
        PersistenceManager pm = new PersistenceManager(driver,database,login,password);
        //create some dummy objects
        for(int x = 0;x<50;x++)
        {

            SimpleObject so = new SimpleObject();
            long id = pm.saveObject(so);
            so.setAge(id);
            so.setName(Long.toString(id));
            long id2 = pm.saveObject(so);
            //make sure the id is the same
            assertEquals(id,id2);
            pm.close();

            pm = new PersistenceManager(driver,database,login,password);
            SimpleObject sos = pm.getObject(SimpleObject.class,id);
            assertEquals(so.getAge(),sos.getAge());
            assertEquals(so.getName(),sos.getName());
        }
        pm.close();
    }
	
	/**
	 * Test updating an object that contains other objects.
	 */
	@Test
	public void testUpdateComplexObject() throws Exception
	{
		PersistenceManager pm1 = new PersistenceManager(driver,database,login,password);
		PersistenceManager pm2 = new PersistenceManager(driver,database,login,password);
		ComplexObject orig = new ComplexObject();
		SimplestObject simpleOrig= new SimplestObject();
		simpleOrig.setFoo(42.0);
		orig.setSimplestObject(simpleOrig);
		pm1.saveObject(orig);
		List<ComplexObject> list = pm2.getObjects(ComplexObject.class, new All());
		assertEquals(1,list.size());
		ComplexObject copy  = list.get(0);
		
		assertEquals(orig.getSimplestObject().getFoo(),copy.getSimplestObject().getFoo());
		
		//drop the enclosed object
		orig.setSimplestObject(null);
		pm1.saveObject(orig);
		
		//refresh the copy
		assertTrue(pm2.hasChanged(copy));
		pm2.refresh(copy);
		assertNull(copy.getSimplestObject());
		
		//add a new simplestobject to the original
		SimplestObject nuSimple = new SimplestObject();
		nuSimple.setFoo(9000.1);
		orig.setSimplestObject(nuSimple);
		pm1.saveObject(orig);
		
		//make sure the change propagates
		assertTrue(pm2.hasChanged(copy));
		pm2.refresh(copy);
		assertNotNull(copy.getSimplestObject());
		assertEquals(orig.getSimplestObject().getFoo().doubleValue(),copy.getSimplestObject().getFoo().doubleValue(),0.01);
		
		
		//change from non-null to another non-null property
		nuSimple = new SimplestObject();
		nuSimple.setFoo(11.0);
		orig.setSimplestObject(nuSimple);
		pm1.saveObject(orig);
		
		//make sure the change propagates
		assertTrue(pm2.hasChanged(copy));
		pm2.refresh(copy);
		assertNotNull(copy.getSimplestObject());
		assertEquals(orig.getSimplestObject().getFoo(),copy.getSimplestObject().getFoo());
		
		//make sure we haven't inadvertently created new objects
		assertEquals(1,pm1.getCount(SimplestObject.class, new All()));
		assertEquals(1,pm1.getCount(ComplexObject.class, new All()));
		assertEquals(2,pm1.getCount(Object.class, new All()));
		
		pm1.close();
		pm2.close();
	}
	
	/**
	 * Test using queries with Order sub-clauses.
	 */
	@Test
	public void testSubClauses() throws Exception
	{
		PersistenceManager pm = new PersistenceManager(driver,database,login,password);
		ConnectionWrapper cw = pm.getConnectionWrapper();
		//add data with two sortable fields
		for(int x = 0;x<100;x++)
		{
			long dec = x/10;
			double rem = x%10;
			SimpleObject so = new SimpleObject();
			so.setAge(dec);
			so.setScale(rem);
			pm.saveObject(cw,so);
		}
		cw.commitAndDiscard();
		SimpleObject sortAge=new SimpleObject();
		sortAge.setAge(1l);
		SimpleObject sortScale=new SimpleObject();
		sortScale.setScale(1.0);
		List<SimpleObject> list = pm.getObjects(SimpleObject.class,new All(),new Order(new Ascending(sortAge),new Descending(sortScale)));

		for(int x = 0;x<list.size()-1;x++)
		{
			SimpleObject curr = list.get(x);
			SimpleObject next = list.get(x+1);
			if(curr.getAge().longValue()==next.getAge().longValue())
			{
				//we sorted scale descending
				assertTrue(curr.getScale()>next.getScale());
			}
			else
			{
				//we sorted age ascending
				assertEquals((long)curr.getAge()+1,(long)next.getAge());
				assertEquals((double)curr.getScale()+9,(double)next.getScale(),0.00001);
			}
		}
		
		list = pm.getObjects(SimpleObject.class,new All(),new Order(new Ascending(sortAge),new Ascending(sortScale)));

		for(int x = 0;x<list.size()-1;x++)
		{
			SimpleObject curr = list.get(x);
			SimpleObject next = list.get(x+1);
			if(curr.getAge().longValue()==next.getAge().longValue())
			{
				//we sorted scale descending
				assertTrue(curr.getScale()<next.getScale());
			}
			else
			{
				//we sorted age ascending
				assertEquals((long)curr.getAge()+1,(long)next.getAge());
				assertEquals((double)curr.getScale()-9,(double)next.getScale(),0.00001);
			}
		}
	
		pm.close();
	}
	
	/**
	 * Test if naming a column via annotation works.
	 * 
	 */
	@Test
	public void testColumnNameAnnotation() throws Exception
	{
		PersistenceManager pm = new PersistenceManager(driver,database,login,password);
		//save an object with a known value
		ColumnNameObject cno = new ColumnNameObject();
		cno.setName("foo_bar");
		pm.saveObject(cno);
		//'manually' check if the value is there
		ConnectionWrapper cw = pm.getConnectionWrapper();
		String query = "SELECT COUNT(*) FROM ";
		query += NameGenerator.getTableName(cno.getClass(), pm.getPersist().getAdapter());
		query += " WHERE ALTERNATIVENAME = ?";
		PreparedStatement ps = cw.prepareStatement(query);
		ps.setString(1, cno.getName());
		ResultSet rs = ps.executeQuery();
		assertTrue(rs.next());
		assertEquals(1,rs.getLong(1));
		ps.close();
		
		cw.commitAndDiscard();
		
		pm.close();
	}
	
	/**
	 * Test save/load/update string arrays
	 */
	@Test
	public void testStringArrays() throws Exception
	{
		PersistenceManager pm1 = new PersistenceManager(driver,database,login,password);
		StringArrayContainer sac = new StringArrayContainer();
		sac.setValues(new String[]{"Foo","bar","BAZ"});
		pm1.saveObject(sac);
		
		//get the object from another PersistenceManager.
		PersistenceManager pm2 = new PersistenceManager(driver,database,login,password);
		List<StringArrayContainer> list = pm2.getObjects(StringArrayContainer.class,new All());
		assertEquals(1,list.size());
		StringArrayContainer copy = list.get(0);
		assertEquals(sac.getValues().length,copy.getValues().length);
		for(int x = 0;x<sac.getValues().length;x++)
		{
			assertEquals(sac.getValues()[x],copy.getValues()[x]);
		}
		
		//change the original object's array contents
		sac.getValues()[0]="Nonsense";
		pm1.saveObject(sac);
		//check that the change propagates
		assertTrue(pm2.hasChanged(copy));
		//get the refreshed copy
		pm2.refresh(copy);
		assertEquals(sac.getValues().length,copy.getValues().length);
		for(int x = 0;x<sac.getValues().length;x++)
		{
			assertEquals(sac.getValues()[x],copy.getValues()[x]);
		}
		
		//change the original object's length
		sac.setValues(new String[]{"one","two","three","four"});
		pm1.saveObject(sac);
		//check that the change propagates
		assertTrue(pm2.hasChanged(copy));
		//get the refreshed copy
		pm2.refresh(copy);
		assertEquals(sac.getValues().length,copy.getValues().length);
		for(int x = 0;x<sac.getValues().length;x++)
		{
			assertEquals(sac.getValues()[x],copy.getValues()[x]);
		}
		
		pm2.close();
		pm1.close();
	}
	
	/**
	 * Test saving/loading enums.
	 */
	@Test
	public void testEnums() throws Exception
	{
		PersistenceManager pm1 = new PersistenceManager(driver,database,login,password);
		EnumContainer ec = new EnumContainer();
		ec.setState(MyEnum.BAB);
		pm1.saveObject(ec);
		
		//get the object from another PersistenceManager.
		PersistenceManager pm2 = new PersistenceManager(driver,database,login,password);
		List<EnumContainer> list = pm2.getObjects(EnumContainer.class, new All());
		assertEquals(1,list.size());
		EnumContainer copy = list.get(0);
		assertEquals(ec.getState(),copy.getState());
		
		ec.setState(MyEnum.BOO);
		pm1.saveObject(ec);
		assertTrue(pm2.hasChanged(copy));
		pm2.refresh(copy);
		assertEquals(ec.getState(),copy.getState());
		
		ec.setState(null);
		pm1.saveObject(ec);
		assertTrue(pm2.hasChanged(copy));
		pm2.refresh(copy);
		assertNull(copy.getState());
		
		ec.setState(MyEnum.BOO);
		pm1.saveObject(ec);
		assertTrue(pm2.hasChanged(copy));
		pm2.refresh(copy);
		assertEquals(ec.getState(),copy.getState());
		
		pm1.close();
		pm2.close();
		
	}
	
	/**
	 * Test resizing columns.
	 */
	@Test
	public void testResizeColumn() throws Exception
	{
		PersistenceManager pm = new PersistenceManager(driver,database,login,password);
		
		//try widening
		NarrowColumnObject sco = new NarrowColumnObject();
		sco.setValue("foo");
		pm.saveObject(sco);
		TestTools tt = new TestTools(pm.getPersist());
		tt.changeName(NarrowColumnObject.class, WideColumnObject.class);
		pm.updateSchema(WideColumnObject.class);
		pm.close();
		pm = new PersistenceManager(driver,database,login,password);
		List<WideColumnObject> wlist = pm.getObjects(WideColumnObject.class,new All());
		assertEquals(1,wlist.size());
		WideColumnObject wnco = wlist.get(0);
		assertEquals(sco.getValue(),wnco.getValue());
		
		//narrowing is not supported by all databases. 
		
		pm.close();
		
	}
	
	/**
	 * Test adding and removing columns from indices.
	 */
	@Test
	public void testAddRemoveIndices() throws Exception
	{

		PersistenceManager pm = new PersistenceManager(driver,database,login,password);
		
		//save an object with a reduced index
		IndicesReduced red = new IndicesReduced("foo");
		pm.saveObject(red);

		TestTools tt = new TestTools(pm.getPersist());
		tt.changeName(IndicesReduced.class, IndicesFull.class);
		pm.updateSchema(IndicesFull.class);
		pm.close();
		
		//load the object, make sure it still exists
		pm = new PersistenceManager(driver,database,login,password);
		assertEquals(0,pm.getCount(IndicesReduced.class, new All()));
		List<IndicesFull> list = pm.getObjects(IndicesFull.class, new All());
		assertEquals(1,list.size());
		IndicesFull ref = list.get(0);
		assertEquals(red.getFoo(),ref.getFoo());
		
		//delete the object
		pm.deleteObject(ref);
		//create a new object, store it
		ref = new IndicesFull();
		ref.setFoo("bar");
		pm.saveObject(ref);
		tt = new TestTools(pm.getPersist());
		tt.changeName(IndicesFull.class, IndicesReduced.class);
		pm.updateSchema(IndicesReduced.class);
		pm.close();
		
		//load the object, make sure it still exists
		pm = new PersistenceManager(driver,database,login,password);
		assertEquals(0,pm.getCount(IndicesFull.class, new All()));
		List<IndicesReduced> listf = pm.getObjects(IndicesReduced.class, new All());
		assertEquals(1,listf.size());
		IndicesReduced copy = listf.get(0);
		assertEquals(copy.getFoo(),ref.getFoo());
		
		pm.close();
	}
	
	/**
	 * Test repeatedly inserting and deleting objects containing arrays.
	 * 
	 */
	@Test
	public void testInsertAndDeleteOfArrays() throws Exception
	{

		PersistenceManager pm = new PersistenceManager(driver,database,login,password);
		for(int x = 0;x<10;x++)
		{
			//insert 10 objects
			for(int t = 0;t<10;t++)
			{
				ObjectArrayContainingObject aco = new ObjectArrayContainingObject();
				aco.setData(new Object[]{x*10.0+t,2.0,3.0});
				pm.saveObject(aco);
			}
			assertEquals(x*5+10,pm.getCount(ObjectArrayContainingObject.class, new All()));
			//delete 5 objects
			for(int t = 0;t<5;t++)
			{
				ObjectArrayContainingObject match = new ObjectArrayContainingObject();
				match.setData(new Object[]{x*10.0+t});
				pm.deleteObjects(ObjectArrayContainingObject.class, new Equal(match));
			}
			assertEquals((x+1)*5,pm.getCount(ObjectArrayContainingObject.class, new All()));
		}
		//delete all objects
		pm.deleteObjects(ObjectArrayContainingObject.class, new All());
		assertEquals(0, pm.getCount(ObjectArrayContainingObject.class, new All()));
		pm.close();
	}
	
	/**
	 * Test if the capacity value appears sane.
	 * @throws Exception
	 */
	@Test
	public void testCapacityValue() throws Exception
	{
		PersistenceManager pm = new PersistenceManager(driver,database,login,password);
		//drop everything
		pm.dropTable(Object.class);
		ConnectionWrapper cw = pm.getConnectionWrapper();
		assertEquals(0.0,pm.getUsedCapacity(cw),Math.ulp(0.0));
		cw.discard();
		
		int x = 0;
		//insert a whole bunch of things
		for(;x<1000;x++)
		{
			SimpleObject so = new SimpleObject();
			so.setName(Integer.toString(x));
			pm.saveObject(so);
		}
		//make sure we've used some capacity
		cw = pm.getConnectionWrapper();
		double cap = pm.getUsedCapacity(cw);
		assertTrue(cap>0);
		assertTrue(cap<1);
		cw.discard();
		
		//insert even more stuff
		for(;x<2000;x++)
		{
			SimpleObject so = new SimpleObject();
			so.setName(Integer.toString(x));
			pm.saveObject(so);
		}
		//make sure we've used even more capacity
		cw = pm.getConnectionWrapper();
		double cap2 = pm.getUsedCapacity(cw);
		assertTrue(cap2>0);
		assertTrue(cap2<1);
		assertTrue(cap<cap2);
		cw.discard();
		
		//make sure deleting large numbers of objects works
		pm.deleteObjects(SimpleObject.class, new All());
		String query = "SELECT COUNT(*) FROM JAVA_LANG_OBJECT";
		cw = pm.getConnectionWrapper();
		ResultSet rs = cw.prepareStatement(query).executeQuery();
		rs.next();
		assertEquals(0,rs.getLong(1));
		rs.close();
		
		pm.close();
	}
	
	/**
	 * Class properties are handled specially, test this here.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testObjectsWithClassProperties() throws Exception
	{
		PersistenceManager pm = new PersistenceManager(driver,database,login,password);
		ClassContainingObject one = new ClassContainingObject();
		ClassContainingObject two = new ClassContainingObject();
		one.setMyValue(1f);
		one.setMyClass(Map.class);
		two.setMyValue(2f);
		two.setMyClass(URI.class);
		pm.saveObject(one);
		pm.saveObject(two);
		pm.close();
		
		//test retrieving the objects
		pm = new PersistenceManager(driver,database,login,password);
		ClassContainingObject query = new ClassContainingObject();
		query.setMyClass(Map.class);
		List<ClassContainingObject> res = pm.getObjects(query);
		assertEquals(1,res.size());
		assertEquals(1f,res.get(0).getMyValue(),0.0001f);
		query.setMyClass(URI.class);
		res = pm.getObjects(query);
		assertEquals(1,res.size());
		assertEquals(2f,res.get(0).getMyValue(),0.0001f);
		
		//update an ojbect
		ClassContainingObject toUpdate = res.get(0);
		toUpdate.setMyValue(3f);
		pm.saveObject(toUpdate);
		pm.close();
		pm = new PersistenceManager(driver,database,login,password);
		//query object still correctly initialised from above...
		res = pm.getObjects(query);
		assertEquals(1,res.size());
		assertEquals(3f,res.get(0).getMyValue(),0.0001f);
		toUpdate = res.get(0);
		toUpdate.setMyClass(Object.class);
		pm.saveObject(toUpdate);
		pm.close();
		pm = new PersistenceManager(driver,database,login,password);
		query.setMyClass(Object.class);
		res = pm.getObjects(query);
		assertEquals(1,res.size());
		assertEquals(3f,res.get(0).getMyValue(),0.0001f);
		
		
		//make sure deleting also works
		pm.deleteObjects(ClassContainingObject.class, new All());
		assertEquals(0,pm.getCount(ClassContainingObject.class, new All()));
		pm.close();
	}
	
	/**
	 * Test saving an object with an array, then updating the array.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testUpdateObjectArray() throws Exception
	{
		String textToRemove = "Foo";
		//save an object that contains a non-primitive array
		ComplexArrayObject aco = new ComplexArrayObject();
		ComplexObject[] data = new ComplexObject[2];
		data[0] = new ComplexObject();
		data[1] = new ComplexObject();
		data[0].setObject(textToRemove);
		data[1].setObject("Bar");
		aco.setData(data);
		PersistenceManager pm = new PersistenceManager(driver,database,login,password);
		pm.saveObject(aco);
		
		//change one of the objects of the array
		data[0] = new ComplexObject();
		data[0].setObject("Baz");
		
		//re-save the complex object to ensure the array is updated
		pm.saveObject(aco);
		
		//make sure the overwritten object in the array is now gone
		assertEquals(2,pm.getCount(ComplexObject.class, new All()));
		List<ComplexObject>list = pm.getObjects(ComplexObject.class, new All());
		assertEquals(2,list.size());
		for(ComplexObject co:list)
		{
			assertFalse(textToRemove.equals((String)co.getObject()));
		}
		pm.close();
	}
	
	/**
	 * Test queries using the LIKE operator.
	 */
	@Test
	public void testLikeOperator() throws Exception
	{
		PersistenceManager pm = new PersistenceManager(driver,database,login,password);
		Book so = new Book();
		so.setTitle("foobar");
		pm.saveObject(so);
		so = new Book();
		so.setTitle("foo");
		pm.saveObject(so);
		so = new Book();
		so.setTitle("foob");
		pm.saveObject(so);
		so = new Book();
		so.setTitle("fooba");
		pm.saveObject(so);
		so = new Book();
		so.setTitle("oobar");
		pm.saveObject(so);
		so = new Book();
		so.setTitle("foar");
		pm.saveObject(so); 
		
		Book query = new Book();
		// get the objects matching the foo* pattern (4)
		query.setTitle("foo%");
		assertEquals(4,pm.getCount(Book.class,new Like(query)));
		// get objects matching the *bar pattern (2)
		query.setTitle("%bar");
		assertEquals(2,pm.getCount(Book.class,new Like(query)));
		// get objects matching the *ooba* pattern (3)
		query.setTitle("%ooba%");
		assertEquals(3,pm.getCount(Book.class,new Like(query)));
		// get object matching the fo*ar pattern (2)
		query.setTitle("fo%ar");
		assertEquals(2,pm.getCount(Book.class,new Like(query)));
		//get all 6 objects
		query.setTitle("fo%");
		Book query2 = new Book();
		query2.setTitle("%ar");
		assertEquals(6, pm.getCount(Book.class, new Or(new Like(query),new Like(query2))));
		
		pm.close();
	}
	
	/**
	 * Test the examples from the tutorial (except the author-book example, that's tested elsewhere).
	 * 
	 */
	@Test
	public void testTutorial1() throws Exception
	{
		PersistenceManager pm = new PersistenceManager(driver,database,login,password);
		//save an object
		TextObject a = new TextObject();
		a.setText("The quick brown fox");
		a.setKeyWords(a.getText().split(" "));
		pm.saveObject(a);
		
		//search for the object
		TextObject example = new TextObject();
		example.setText("The quick brown fox");
		List<TextObject>res  = pm.getObjects(TextObject.class, new Equal(example));
		System.out.println(res.get(0).getKeyWords());
		Set<String>keywords = res.get(0).getKeyWords();
		assertTrue(keywords.contains("The"));
		assertTrue(keywords.contains("quick"));
		assertTrue(keywords.contains("brown"));
		assertTrue(keywords.contains("fox"));
		assertEquals(4,keywords.size());
		
		//find by unordered set
		example.setText(null);
		example.setKeyWords(new String[]{"brown"});
		res  = pm.getObjects(TextObject.class, new Equal(example));
		System.out.println(res.get(0).getText());
		keywords = res.get(0).getKeyWords();
		assertTrue(keywords.contains("The"));
		assertTrue(keywords.contains("quick"));
		assertTrue(keywords.contains("brown"));
		assertTrue(keywords.contains("fox"));
		assertEquals(4,keywords.size());
		
		//find not matching
		res = pm.getObjects(TextObject.class, new Different(example));
		if(res.size()==0)
		{
		    System.out.println("No matches found.");
		}
		
		//delete all TextObjects
		pm.deleteObjects(TextObject.class,new All());
		
		//insert some new ones
		a = new TextObject();
		a.setText("The quick brown fox");
		a.setKeyWords(a.getText().split(" ")); 
		pm.saveObject(a);
		FooTextObject foo = new FooTextObject();
		foo.setKeyWords(new String[] { "The", "quick", "brown", "dog" });
		pm.saveObject(foo);
		BarTextObject bar = new BarTextObject();
		bar.setKeyWords(new String[] { "The", "quick", "brown", "rabbit" });
		pm.saveObject(bar);
		
		//do some searches,verify size
		example = new TextObject();
		example.setKeyWords(new String[] { "brown" });
		List<TextObject> list = 
		           pm.getObjects(TextObject.class, new Equal(example));
		assertEquals(3, list.size());
		List<FooTextObject> fooList = 
		           pm.getObjects(FooTextObject.class, new Equal(example));
		assertEquals(1,fooList.size());

		example.setKeyWords(new String[] { "rabbit" });
		List<TextObject> rabbitList = 
		           pm.getObjects(TextObject.class, new Equal(example));
		assertEquals(1,rabbitList.size());
		
		pm.close();
		
	}
	
	/**
	 * Test the sorting and limits part of the tutorial
	 */
	@Test
	public void testTutorial2() throws Exception
	{

		PersistenceManager pm = new PersistenceManager(driver,database,login,password);
		
		//insert some test data
		pm.saveObject(new Person("Tim","Smith"));
		pm.saveObject(new Person("Vijay","Shankar"));
		pm.saveObject(new Person("Ash","Walsh"));
		pm.saveObject(new Person("Random","Hacker"));
		pm.saveObject(new Person("Isaac","Asimov"));
		pm.saveObject(new Person("Elvis","Presley"));
		
		// create the sorting object
		Person lastNameSort = new Person();

		// the value of LastName does not matter, only that it is not null
		lastNameSort.setLastName("foo");

		// sort descending by last name
		List<Person> res = 
		      pm.getObjects(Person.class, new All(), new Descending(lastNameSort));

		//make sure the first name is the first
		assertEquals(6,res.size());
		assertEquals("Walsh",res.get(0).getLastName());
		assertEquals("Asimov",res.get(res.size()-1).getLastName());
		
		//create the sorting objects
		lastNameSort = new Person();
		Person firstNameSort = new Person();

		//the values of LastName and FirstName do not matter, 
		//only that they are not null
		lastNameSort.setLastName("foo");
		firstNameSort.setFirstName("bar");

		//sort descending by last name, then ascending by first name
		res = pm.getObjects(Person.class, new All(), 
		    new Order(new Descending(lastNameSort),new Ascending(firstNameSort)));
		assertEquals(6,res.size());
		assertEquals("Walsh",res.get(0).getLastName());
		assertEquals("Asimov",res.get(res.size()-1).getLastName());
		
		//sort ascending on firstname, then ascending on lastname, limit to first three results
		res = pm.getObjects(Person.class, new All(), 
		    new Order( 3, new Ascending(firstNameSort), 
		                  new Ascending(lastNameSort)));
		assertEquals(3,res.size());
		assertEquals("Walsh",res.get(0).getLastName());
		assertEquals("Asimov",res.get(res.size()-1).getLastName());
		
		
		//same as above, but skip first three and return 2
		res = pm.getObjects(Person.class, new All(), 
		    new Order(2, 3, new Ascending(firstNameSort), 
		                    new Ascending(lastNameSort)));
		assertEquals(2,res.size());
		assertEquals("Hacker",res.get(0).getLastName());
		assertEquals("Smith",res.get(res.size()-1).getLastName());
		
		int toDelete = 20;
		for(int x=0;x<toDelete;x++)
		{
			pm.saveObject(new TextObject());
		}
		int deleted = pm.deleteObjects(TextObject.class, new All());
		assertEquals(toDelete,deleted);
		
		
		pm.close();
	}
	
	/**
	 * Test that we can safely delete an array, even though it shares some components with another array.
	 * @throws Exception
	 */
	@Test
	public void testCrossingArrays() throws Exception
	{
		
		SimplestObject so1 = new SimplestObject();
		so1.setFoo(1.0);
		SimplestObject so2 = new SimplestObject();
		so2.setFoo(2.0);
		ObjectArrayContainingObject oaco1 = new ObjectArrayContainingObject();
		oaco1.setData(new Object[]{so1,so2});
		ObjectArrayContainingObject oaco2 = new ObjectArrayContainingObject();
		oaco2.setData(new Object[]{so1});
		

		PersistenceManager pm = new PersistenceManager(driver,database,login,password);
		pm.saveObject(oaco1);
		pm.saveObject(oaco2);
		pm.deleteObject(oaco2);
		List<ObjectArrayContainingObject> list = pm.getObjects(ObjectArrayContainingObject.class, new All());
		assertEquals(1,list.size());
		ObjectArrayContainingObject res = list.get(0);
		assertEquals(2,res.getData().length);
		assertNotNull(res.getData()[0]);
		assertNotNull(res.getData()[1]);
		
		SimplestObject so1b = (SimplestObject) res.getData()[0];
		SimplestObject so2b = (SimplestObject) res.getData()[1];
		
		assertEquals(1.0,so1b.getFoo(),0.001);
		assertEquals(2.0,so2b.getFoo(),0.001); 
		
		//delete the objects in the array
		pm.deleteObject(so1b);
		pm.deleteObject(so2b);
		
		list = pm.getObjects(ObjectArrayContainingObject.class, new All());
		assertEquals(1,list.size());
		res = list.get(0);
		assertEquals(2,res.getData().length);
		assertNotNull(res.getData()[0]);
		assertNotNull(res.getData()[1]);
		
		so1b = (SimplestObject) res.getData()[0];
		so2b = (SimplestObject) res.getData()[1];
		
		assertEquals(1.0,so1b.getFoo(),0.001);
		assertEquals(2.0,so2b.getFoo(),0.001); 
		
		pm.close();
	}
	
	/**
	 * Test that objects containing collections recursively are handled correctly.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testSubCategories() throws Exception
	{
		PersistenceManager pm = new PersistenceManager(driver,database,login,password);

		InfiniteRepititionCollection a = new InfiniteRepititionCollection("a");
		InfiniteRepititionCollection b = new InfiniteRepititionCollection("b");
		InfiniteRepititionCollection c = new InfiniteRepititionCollection("c");
		InfiniteRepititionCollection d = new InfiniteRepititionCollection("d");
		InfiniteRepititionCollection e = new InfiniteRepititionCollection("e");
		
		a.addSubCollection(b);
		b.addSubCollection(c);
		b.addSubCollection(d);
		c.addSubCollection(d);
		d.addSubCollection(e);
		pm.saveObject(b);
		pm.saveObject(a);
		pm.close();
		
		pm = new PersistenceManager(driver,database,login,password);
		InfiniteRepititionCollection query = new InfiniteRepititionCollection();
		query.setName("a");
		List<InfiniteRepititionCollection> res = pm.getObjects(InfiniteRepititionCollection.class, new Equal(query));
		assertEquals(1,res.size());
		InfiniteRepititionCollection A = res.get(0);
		pm.deleteObject(A);
		
		query.setName("b");
		res = pm.getObjects(InfiniteRepititionCollection.class, new Equal(query));
		assertEquals(1,res.size());
		InfiniteRepititionCollection B = res.get(0);
		assertEquals("b",B.getName());
		assertEquals(2,B.getSubcategories().size());
		InfiniteRepititionCollection tmp0 = B.getSubcategories().get(0);
		InfiniteRepititionCollection tmp1 = B.getSubcategories().get(1);
		assertEquals(1,tmp0.getSubcategories().size());
		assertEquals(1,tmp1.getSubcategories().size());
		if(tmp0.getName().equals("c"))
		{
			assertEquals("d",tmp1.getName());
			assertEquals("d",tmp0.getSubcategories().get(0).getName());
			assertEquals("e",tmp1.getSubcategories().get(0).getName());
		}
		else
		{
			assertEquals("d",tmp0.getName());
			assertEquals("c",tmp1.getName());
			assertEquals("e",tmp0.getSubcategories().get(0).getName());
			assertEquals("d",tmp1.getSubcategories().get(0).getName());
		}
		pm.close();
	}
	
	@Test
	public void testCircularArrayContainers() throws Exception
	{
		
		// create some authors;
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

		// save everything
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		pm.saveObject(asimov);
		pm.close();
		
		
		pm = new PersistenceManager(driver,database,login,password);
		pm.deleteObjects(Object.class, new All());
		pm.close();
		
		
		pm = new PersistenceManager(driver,database,login,password);
		String arrayCount = "SELECT COUNT(*) FROM " + Defaults.HAS_A_TABLENAME;
		ConnectionWrapper cw = pm.getConnectionWrapper();
		ResultSet executeQuery = cw.prepareStatement(arrayCount).executeQuery();
		assertTrue(executeQuery.next());
		assertEquals(0,executeQuery.getInt(1));
		cw.commitAndDiscard();
		pm.close();
		
	}
	
	/**
	 * Try adding and updating objects of classes with the Id annotation.
	 * @throws Exception
	 */
	@Test
	public void testIdColumns() throws Exception
	{

		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		pm.deleteObjects(WithIdLong.class, new All());
		pm.deleteObjects(WithIdString.class, new All());
		pm.deleteObjects(WithIdStringAndLong.class, new All());

		//test with long id
		WithIdLong one = new WithIdLong();
		one.setTheLongId(1L);
		one.setValue(0.2f);
		one.setSomeOtherString("one");
		pm.saveObject(one);
		pm.close();
		
		pm = new PersistenceManager(driver, database, login, password);
		one.setSomeOtherString("two");
		pm.saveObject(one);
		List<WithIdLong> list = pm.getObjects(WithIdLong.class, new All());
		assertEquals(1, list.size());
		assertEquals("two",list.get(0).getSomeOtherString());
		
		//test with string id
		List<WithIdString> list2 = pm.getObjects(WithIdString.class, new All());
		assertEquals(0, list2.size());
		WithIdString two = new WithIdString();
		two.setTheStringId("two");
		two.setSomeOtherString("three");
		two.setValue(0.3f);
		pm.saveObject(two);
		pm.close();

		pm = new PersistenceManager(driver, database, login, password);
		list2 = pm.getObjects(WithIdString.class, new All());
		assertEquals(1,list2.size());
		assertEquals("three",list2.get(0).getSomeOtherString());
		assertNotNull(list2.get(0).getValue());
		two.setSomeOtherString("four");
		two.setValue(null);
		pm.saveObject(two);
		list2 = pm.getObjects(WithIdString.class, new All());
		assertEquals(1,list2.size());
		assertEquals("four",list2.get(0).getSomeOtherString());
		assertEquals(0.3f,list2.get(0).getValue(),0.0001f);
		
		//make sure null values are not erased
		WithIdString update = new WithIdString();
		update.setTheStringId("two");
		update.setValue(42f);
		pm.saveObject(update);
		list2 = pm.getObjects(WithIdString.class, new All());
		assertEquals(1,list2.size());
		assertEquals("four",list2.get(0).getSomeOtherString());
		assertEquals(42f,list2.get(0).getValue(),0.001f);
		
		
		//test with string and long id
		List<WithIdStringAndLong>list3 = pm.getObjects(WithIdStringAndLong.class, new All());
		assertTrue(list3.isEmpty());
		WithIdStringAndLong three = new WithIdStringAndLong();
		three.setTheLongId(34L);
		three.setTheStringId("thirtyfour");
		three.setSomeOtherString("foo");
		three.setValue(3.0f);
		pm.saveObject(three);
		pm.close();

		pm = new PersistenceManager(driver, database, login, password);
		three.setSomeOtherString("bar");
		pm.saveObject(three);
		pm.close();
		
		pm = new PersistenceManager(driver, database, login, password);
		list3 = pm.getObjects(WithIdStringAndLong.class, new All());
		assertEquals(1,list3.size());
		assertEquals("bar",list3.get(0).getSomeOtherString());
		pm.close();
		
		pm = new PersistenceManager(driver, database, login, password);
		three.setTheStringId("foo");
		pm.saveObject(three);
		list3 = pm.getObjects(WithIdStringAndLong.class, new All());
		assertEquals(2,list3.size());
		three.setTheLongId(30L);
		pm.saveObject(three);
		list3 = pm.getObjects(WithIdStringAndLong.class, new All());
		assertEquals(2,list3.size());
		pm.close();		
		

		pm = new PersistenceManager(driver, database, login, password);
		IdContainer container = new IdContainer();
		container.setIdObject(one);
		pm.saveObject(container);
		list = pm.getObjects(WithIdLong.class, new All());
		assertEquals(1, list.size());
		assertEquals("two",list.get(0).getSomeOtherString());
		pm.close();
		
		one.setValue(1234f);
		pm = new PersistenceManager(driver, database, login, password);
		pm.saveObject(container);
		list = pm.getObjects(WithIdLong.class, new All());
		assertEquals(1, list.size());
		assertEquals(1234f,list.get(0).getValue(),0.1f);
		pm.close();
	}	
	
	/**
	 * Test object with static members and complex types.
	 */
	@Test
	public void testStaticMembers() throws Exception
	{
		PersistenceManager pm = new PersistenceManager(driver, database, login, password);
		DataParser dp = new DataParser();
		dp.setString("2020-11-22T10:19:41Z\n"
				+ "\n"
				+ "RUNWAY IN USE: 19ESGP SIE 201122 AUTO\n"
				+ "RWY 19 MET REPORT 220950Z\n"
				+ "WIND 260/19KT MAX30 MNM13\n"
				+ "\n"
				+ "VIS 10KM\n"
				+ "\n"
				+ "\n"
				+ "\n"
				+ "CLD FEW /// 2500FT SCT /// 3300FT BKN /// 4500FT\n"
				+ "T09 DP04\n"
				+ "QNH 1 0 0 6 HPA TRL 65\n"
				+ "\n"
				+ "\n"
				+ "\n"
				+ "\n"
				+ "\n"
				+ "\n"
				+ "\n"
				+ "\n"
				+ "\n"
				+ "RWY 01 M 19\n"
				+ "MEAN02 260/20 KT 260/20 KT\n"
				+ "VRB 250-290 250-290\n"
				+ "MIN/MAX 12/27 12/27\n"
				+ "COMP +04/L20 -04/R20\n"
				+ "RVR >2000N >2000N\n"
				+ "VIS 17 KM 18 KM\n"
				+ "PRW\n"
				+ "CLD1 FEW 4100 FT\n"
				+ "CLD2\n"
				+ "CLD3\n"
				+ "QFETHR 1004.5 1004.8\n"
				+ "\n"
				+ "QNH 1006.6 TRL 65 QFE 1004.4\n"
				+ "T 9.0 DP 3.1 RH 67METAR ESGP 220950Z AUTO 26019G30KT 9999 FEW025/// SCT033/// BKN045/// 09/04 Q1006");
		pm.saveObject(dp);
		pm.close();
	}
}
