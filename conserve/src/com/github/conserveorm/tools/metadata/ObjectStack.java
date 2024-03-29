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
package com.github.conserveorm.tools.metadata;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import com.github.conserveorm.adapter.AdapterBase;
import com.github.conserveorm.connection.ConnectionWrapper;
import com.github.conserveorm.select.discriminators.Equal;
import com.github.conserveorm.tools.Defaults;
import com.github.conserveorm.tools.DelayedInsertionBuffer;
import com.github.conserveorm.tools.ObjectTools;
import com.github.conserveorm.tools.Tools;
import com.github.conserveorm.tools.generators.NameGenerator;

/**
 * Wrapper that encapsulates all ObjectRepresentations for an object.
 * 
 * 
 * @author Erik Berglund
 * 
 */
public class ObjectStack
{
	private RepresentationTree representations = new RepresentationTree();
	private AdapterBase adapter;
	private int size = 0;// the height from the lowest to the highest level
	private int heightOfObject; // the height of the tree node that contains
								// java.lang.Object.

	/**
	 * Load the object stack from the database.
	 * 
	 * @param cw
	 *            the connection to the database.
	 * @param adapter
	 * @param klass
	 *            the class to load a stack for.
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public ObjectStack(ConnectionWrapper cw, AdapterBase adapter,
			Class<?> klass) throws SQLException, ClassNotFoundException
	{
		this.adapter = adapter;
		// fill in the root node
		ObjectRepresentation dor = new DatabaseObjectRepresentation(adapter,
				klass, cw);
		representations.setActual(dor);
		loadRecursively(cw, representations.root);
		// calculate lowest level
		calculateLevels();
	}

	public ObjectStack(AdapterBase adapter, Class<?> c)
	{
		this(adapter, c, null);
	}

	public ObjectStack(AdapterBase adapter, Class<?> c, Object o)
	{
		this(adapter, c, o, null);
	}

	/**
	 * Create a stack from an extant class and, optionally, object.
	 * 
	 * @param adapter
	 * @param c
	 *            the class to create a stack for
	 * @param o
	 *            optional object of class c, used to fill in values.
	 * @param delayBuffer
	 */
	public ObjectStack(AdapterBase adapter, Class<?> c, Object o,
			DelayedInsertionBuffer delayBuffer)
	{
		this.adapter = adapter;
		ObjectRepresentation or = new ConcreteObjectRepresentation(adapter, c,
				o, delayBuffer);
		representations.setActual(or);
		// recursively load the rest of the representations
		loadRecursively(representations.root, o, delayBuffer);

		// calculate lowest level
		calculateLevels();
		// remove duplicate values
		removeDuplicatedProperties();
		// clean up the tree
		removeDuplicatedNodes(representations.root, new ArrayList<Class<?>>(),
				new ArrayList<Node>());

		// check for implementations of Collection
		if (Collection.class.isAssignableFrom(c))
		{
			// switch to bug-fix implementation of Collection
			for (ObjectRepresentation rep : this.getAllRepresentations())
			{
				if (rep.getRepresentedClass().equals(Collection.class))
				{
					rep.implementCollection();
				}
			}
		}
		// do the same for Map as for Collection
		if (Map.class.isAssignableFrom(c))
		{
			// switch to bug-fix implementation of Map
			for (ObjectRepresentation rep : this.getAllRepresentations())
			{
				if (rep.getRepresentedClass().equals(Map.class))
				{
					rep.implementMap();
				}
			}
		}
	}

	private void removeDuplicatedNodes(Node node, List<Class<?>> seenClasses,
			List<Node> nodes)
	{
		List<Node> supers = node.getSupers();
		for (int x = 0; x < supers.size(); x++)
		{
			Node s = supers.get(x);
			Class<?> c = s.getRepresentation().getRepresentedClass();
			if (!seenClasses.contains(c))
			{
				seenClasses.add(c);
				nodes.add(s);
				removeDuplicatedNodes(s, seenClasses, nodes);
			}
			else
			{
				// merge the two sub-trees
				int seenIndex = seenClasses.indexOf(c);
				Node realNode = nodes.get(seenIndex);
				node.replaceSuper(x, realNode);
			}
		}
	}

	/**
	 * Recursively remove properties that exists in superclasses.
	 * 
	 * @param n
	 */
	private void removeDuplicatedProperties()
	{
		Queue<Node> queue = new LinkedList<Node>();
		queue.offer(representations.root);

		while (!queue.isEmpty())
		{
			Node n = queue.poll();
			// remove items that exist in the superclass
			ObjectRepresentation rep = n.getRepresentation();
			List<Node> supers = n.getSupers();
			// iterate over all the superclasses
			for (Node s : supers)
			{
				// iterate over all properties
				for (int p = 0; p < rep.getPropertyCount(); p++)
				{
					String name = rep.getPropertyName(p);
					// check if any of the superclasses has a similarly-named
					// property
					if (hasPropertyRecursive(s, name))
					{
						rep.removeProperty(name);
						break;
					}
				}
				// add the supernodes
				queue.offer(s);
			}
		}
	}

	/**
	 * Check if Node s or any of its super-nodes has a property with the given
	 * name.
	 * 
	 * @param s
	 * @param name
	 * @return
	 */
	private boolean hasPropertyRecursive(Node s, String name)
	{
		if (s.getRepresentation().hasProperty(name))
		{
			return true;
		}
		List<Node> supers = s.getSupers();
		for (Node superNode : supers)
		{
			if (hasPropertyRecursive(superNode, name))
			{
				return true;
			}
		}
		return false;
	}

	/**
	 * Fill in all nodes from database until none are left.
	 * 
	 * @param n
	 * @throws SQLException
	 */
	private void loadRecursively(ConnectionWrapper cw, Node n)
			throws SQLException, ClassNotFoundException
	{
		if (n.getRepresentation().getRepresentedClass().equals(Object.class))
		{
			heightOfObject = n.getHeight();
		}
		List<Class<?>> supers = getSuperClassesFromDatabase(
				n.getRepresentation().getRepresentedClass(), cw);
		for (Class<?> s : supers)
		{
			ObjectRepresentation dor = new DatabaseObjectRepresentation(adapter,
					s, cw);
			Node nu = n.addSuper(dor);
			// recurse
			loadRecursively(cw, nu);
		}
	}

	/**
	 * Iteratively load the stack based on an extant class.
	 * 
	 * @param n
	 *            the parent node containing the subclass.
	 * @param delayBuffer
	 * @param o
	 */
	private void loadRecursively(Node n, Object o,
			DelayedInsertionBuffer delayBuffer)
	{

		if (n.getRepresentation().getRepresentedClass().equals(Object.class))
		{
			heightOfObject = n.getHeight();
		}
		Class<?> c = n.getRepresentation().getRepresentedClass();
		List<Class<?>> supers = new ArrayList<Class<?>>();

		// save the superclass
		Class<?> superClass = c.getSuperclass();
		if (superClass != null)
		{
			supers.add(superClass);
		}
		// save all interfaces
		for (Class<?> i : c.getInterfaces())
		{
			if (superClass == null || !i.isAssignableFrom(superClass))
			{
				supers.add(i);
			}
		}
		// recursively go up the tree
		for (Class<?> s : supers)
		{
			ObjectRepresentation or = new ConcreteObjectRepresentation(adapter,
					s, o, delayBuffer);
			Node nu = n.addSuper(or);
			// recurse
			loadRecursively(nu, o, delayBuffer);
		}
	}

	/**
	 * Calculate the lowest level by iterating over the levels and tracking the
	 * lowest one.
	 */
	private void calculateLevels()
	{
		size = representations.getMaxHeight() + 1;
	}

	/**
	 * Get a list of all superclasses and interfaces implemented directly by
	 * this class, according to the database.
	 * 
	 * @param subClass
	 * @param cw
	 * @return
	 * @throws SQLException
	 */
	private List<Class<?>> getSuperClassesFromDatabase(Class<?> subClass,
			ConnectionWrapper cw) throws SQLException
	{
		List<Class<?>> res = new ArrayList<Class<?>>();
		try
		{
			StringBuilder query = new StringBuilder("SELECT SUPERCLASS FROM ");
			query.append(Defaults.IS_A_TABLENAME);
			query.append(" WHERE SUBCLASS = ?");
			PreparedStatement ps = cw.prepareStatement(query.toString());
			ps.setString(1, NameGenerator.getSystemicName(subClass));
			Tools.logFine(ps);
			ResultSet rs = ps.executeQuery();
			while (rs.next())
			{
				String name = rs.getString(1);
				Class<?> c = ObjectTools.lookUpClass(name, adapter);
				res.add(c);
			}
			ps.close();
		}
		catch (ClassNotFoundException cnfe)
		{
			throw new SQLException(cnfe);
		}

		return res;
	}

	/**
	 * Get a list of all superclasses we know the given class has.
	 * 
	 * @param subClass
	 */
	public List<Class<?>> getSuperClasses(Class<?> subClass)
	{
		List<Class<?>> res = new ArrayList<Class<?>>();
		Node node = null;
		for (Node n : representations.allNodes())
		{
			if (n.getRepresentation().getRepresentedClass().equals(subClass))
			{
				node = n;
				break;
			}
		}
		if (node != null)
		{
			List<Node> supers = node.getSupers();
			for (Node s : supers)
			{
				res.add(s.getRepresentation().getRepresentedClass());
			}
		}
		return res;
	}

	/**
	 * Get a list of all super-representation nodes of the given representation
	 * node. A super-representation is a direct superclass or directly
	 * implemented interface.
	 * 
	 * @param subRep
	 */
	public List<Node> getSupers(Node subRep)
	{
		List<Node> res = new ArrayList<>();

		if (subRep != null)
		{
			List<Node> supers = subRep.getSupers();
			res.addAll(supers);
		}
		return res;
	}

	/**
	 * Get the number of representation layers. Each object has a stack that has
	 * size = N+1, where N is the size of the stack of the super-class.
	 * java.lang.Object has size = 1.
	 * 
	 * @return the number of inheritance levels in this object stack.
	 */
	public int getSize()
	{
		return size;
	}

	/**
	 * Convert height (specific-to-general) to level (general-to-specific).
	 * 
	 * @param height
	 *            the number of levels from the actual representation
	 * @return the number of levels from java.lang.Object.
	 */
	private int heightToLevel(int height)
	{
		return heightOfObject - height;
	}

	/**
	 * Get the most general representation that has a given property name. If
	 * there is no such representation, return null.
	 * 
	 * @param propertyName
	 */
	public ObjectRepresentation getRepresentation(String propertyName)
	{
		ObjectRepresentation res = null;
		int maxLevel = -1;
		for (Node n : representations.allNodes())
		{
			if (n.getRepresentation().hasProperty(propertyName)
					&& n.getHeight() > maxLevel)
			{
				maxLevel = n.getHeight();
				res = n.getRepresentation();
			}
		}
		return res;
	}

	/**
	 * Get all property names in this object stack.
	 * 
	 */
	List<String> getAllPropertyNames()
	{
		List<String> res = new ArrayList<>();
		for (Node n : representations.allNodes())
		{
			ObjectRepresentation rep = n.getRepresentation();
			for (int x = 0; x < rep.getPropertyCount(); x++)
			{
				res.add(rep.getPropertyName(x));
			}
		}
		return res;
	}

	/**
	 * Get the representation of a given class, if it exists in this stack.
	 * 
	 * @param clazz
	 *            the class to get the representation for.
	 * @return the reperesentation of this object at the given class level.
	 */
	public ObjectRepresentation getRepresentation(Class<?> clazz)
	{
		Node n = getNode(clazz);
		if (n != null)
		{
			return n.getRepresentation();
		}
		return null;
	}

	/**
	 * Get the node that contains the representation of a given class.
	 * 
	 * @param clazz
	 */
	public Node getNode(Class<?> clazz)
	{
		for (Node n : representations.allNodes())
		{
			if (n.getRepresentation().getRepresentedClass().equals(clazz))
			{
				return n;
			}
		}
		return null;
	}

	/**
	 * Check if a given subnode has a given node among its supers.
	 * 
	 * @param sub
	 *            the subnode.
	 * @param possibleSuper
	 *            the node that we want to check among the supers.
	 * 
	 * @return false if possibleSuper is not a direct super of sub, or if sub is
	 *         not in this ObjectStack.
	 */
	public boolean hasSuper(Node sub, Node possibleSuper)
	{
		Node localSub = getNode(sub.getRepresentation().getRepresentedClass());
		if (localSub != null)
		{
			return localSub.hasSuper(possibleSuper);
		}
		return false;
	}

	/**
	 * Save the object represented by this ObjectStack, and all referenced
	 * objects, as necessary. Create cache copy.
	 * 
	 * @param cw
	 * 
	 * @throws SQLException @throws
	 */
	public void save(ConnectionWrapper cw) throws SQLException
	{
		if (hasId())
		{
			// check if a matching object already exists
			Object searchObject = createSearchObject();
			List<?> searchRes = this.adapter.getPersist().getObjects(cw,
					searchObject.getClass(), new Equal(searchObject));
			if (searchRes.isEmpty())
			{
				// not found, insert a new row as if there were no Id columns.
			}
			else if (searchRes.size() == 1)
			{
				// one object found, update it
				Object res = searchRes.get(0);
				try
				{
					copyValues(res);
					long id = adapter.getPersist().saveObjectUnprotected(cw, res, this.getActualRepresentation().delayBuffer);
					this.setDatabaseId(id);
				}
				catch (Exception e)
				{
					throw new SQLException(e);
				}
				return;
			}
			else
			{
				// more than one object found, Id is not unique
				throw new SQLException("Id " + getDatabaseId() + " for object of class "
						+ searchObject.getClass().getCanonicalName()
						+ " is not unique.");
			}
		}
		saveNoCache(cw, null);
		// store the object in the object-row map
		ObjectRepresentation rep = this.getActualRepresentation();
		adapter.getPersist().saveToCache(rep.getTableName(), rep.getObject(),
				rep.getId());
	}
	
	private void setDatabaseId(long id)
	{
		for(Node rep:this.representations.allNodes())
		{
			rep.getRepresentation().setId(id);
		}
	}
	
	private Long getDatabaseId()
	{
		return this.representations.root.getRepresentation().id;
	}

	/**
	 * Copy all the values of this ObjectStack into the result object.
	 * 
	 * @param res
	 * @throws InvocationTargetException 
	 * @throws IllegalArgumentException 
	 * @throws IllegalAccessException 
	 */
	private void copyValues(Object res) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException
	{
		for (Node n : representations.allNodes())
		{
			ObjectRepresentation rep = n.getRepresentation();
			for (int prop = 0; prop < rep.getPropertyCount(); prop++)
			{
				Object value = rep.getPropertyValue(prop);
				if (value != null)
				{
					Method mutator = rep.getMutator(prop);
					if (mutator != null)
					{
						boolean oldAccess = mutator.isAccessible();
						mutator.setAccessible(true);
						mutator.invoke(res, value);
						mutator.setAccessible(oldAccess);
					}
				}
			}
		}
	}

	/**
	 * Get an object of the same class as this ObjectStack, with the values of
	 * the Id fields copied into it.
	 */
	private Object createSearchObject()
	{
		Object res = null;
		try
		{
			Constructor<?> constr = this.getActualRepresentation().clazz
					.getConstructor();
			boolean oldAccess = constr.isAccessible();
			constr.setAccessible(true);
			res = constr.newInstance();
			constr.setAccessible(oldAccess);
			for (Node n : representations.allNodes())
			{
				ObjectRepresentation rep = n.getRepresentation();
				for(int prop = 0;prop<rep.getPropertyCount();prop++)
				{
					Method mutator = rep.getMutator(prop);
					if (mutator != null)
					{
						String propName = rep.getPropertyName(prop);
						Object value = null;
						if (rep.getIdentityColumns().contains(propName))
						{
							value = rep.getPropertyValue(prop);
						}
						oldAccess = mutator.isAccessible();
						mutator.setAccessible(true);
						if (value == null && mutator.getParameterTypes()[0].isPrimitive())
						{
							// can't set a primitive to null
						}
						else
						{
							mutator.invoke(res, value);
						}
						mutator.setAccessible(oldAccess);
					}
				}
			}
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return res;
	}

	/**
	 * Return true if any of this class' accessors are annotated with the
	 * {@link com.github.conserveorm.annotations.Id} annotation, false
	 * otherwise.
	 * 
	 */
	public boolean hasId()
	{
		for (Node n : representations.allNodes())
		{
			if (!n.getRepresentation().getIdentityColumns().isEmpty())
			{
				return true;
			}
		}
		return false;
	}

	/**
	 * Save the object represented by this ObjectStack, and all referenced
	 * objects, as necessary. No cache copy is created.
	 * 
	 * @param cw
	 * @param minLevel
	 *            the at which to stop the inserts.
	 * 
	 * @throws SQLException
	 */
	public void saveNoCache(ConnectionWrapper cw, Integer minLevel)
			throws SQLException
	{
		// recursively store the object
		saveNoCache(cw, representations.root, minLevel, null, null);
	}

	/**
	 * Recursively store the object in the database without creating a cache
	 * reference.
	 * 
	 * @param cw
	 * @param root
	 * @param minLevel
	 * @param subClassName
	 * @param id
	 * @throws SQLException
	 */
	private void saveNoCache(ConnectionWrapper cw, Node root, Integer minLevel,
			String subClassName, Long id) throws SQLException
	{
		if (id == null)
		{
			// Find the non-interface objects between root and java.lang.Object.
			// Save the java.lang.Object first, then its children down to root
			// Then save all superclasses of root that have not been saved
			// already (that is, all interfaces)
			Node realSuper = root;
			// the direct subclass of the real super class
			Node subSuper = root;
			while (!realSuper.getRepresentation().getRepresentedClass()
					.equals(Object.class))
			{
				List<Node> supers = realSuper.getSupers();
				for (Node n : supers)
				{
					if (!n.getRepresentation().isInterface())
					{
						subSuper = realSuper;
						realSuper = n;
						break;
					}
				}
			}
			// store the real super-class, the Object
			ConcreteObjectRepresentation objeRep = (ConcreteObjectRepresentation) realSuper
					.getRepresentation();
			adapter.getPersist().getTableManager().ensureTableExists(objeRep,
					cw);
			String subName = null;
			if (!realSuper.equals(subSuper))
			{
				subName = subSuper.getRepresentation().getSystemicName();
				if (subSuper.getRepresentation().isArray())
				{
					subName = NameGenerator.getArrayTablename(adapter);
				}
			}
			objeRep.save(cw, subName, null);
			id = objeRep.getId();
			realSuper.setSaved(true);
		}

		if (!root.isSaved())
		{
			root.setSaved(true);

			// save the object
			ConcreteObjectRepresentation crep = (ConcreteObjectRepresentation) root
					.getRepresentation();
			adapter.getPersist().getTableManager().ensureTableExists(crep, cw);
			crep.save(cw, subClassName, id);

			// get the current class name
			String className = root.getRepresentation().getSystemicName();
			if (root.getRepresentation().isArray())
			{
				className = NameGenerator.getArrayTablename(adapter);
			}

			// get the superclasses
			List<Node> supers = root.getSupers();
			for (int x = 0; x < supers.size(); x++)
			{
				Node s = supers.get(x);
				// recurse
				// interfaces are saved all the way to the top,
				// minLevel is ignored if null
				// otherwise go up to minlevel
				if (s.getRepresentation().isInterface() || minLevel == null
						|| heightToLevel(s.getHeight()) >= minLevel)
				{
					saveNoCache(cw, s, minLevel, className, crep.getId());
				}
			}
		}
	}

	/**
	 * Get the actual object representation node, or the root node.
	 * 
	 */
	public Node getActual()
	{
		return this.representations.root;
	}

	/**
	 * Get a list of all table names in this stack.
	 * 
	 */
	public List<String> getAllTableNames()
	{
		List<String> res = new ArrayList<>();
		for (Node n : representations.allNodes())
		{
			res.add(n.getRepresentation().getTableName());
		}
		return res;
	}

	/**
	 * Return the largest level-number any class in the stack has.
	 * java.lang.Object has level = 0.
	 * 
	 */
	public int getMaxLevel()
	{
		return heightToLevel(0);
	}

	/**
	 * Return the smallest level-number any class in the stack has.
	 * java.lang.Object has level = 0.
	 * 
	 */
	public int getMinLevel()
	{
		return getMaxLevel() - getSize() + 1;
	}

	/**
	 * Get the representation at the bottom of the stack, i.e. the
	 * representation that corresponds to the actual class of the object.
	 * 
	 * @return the representation of the object at the bottom of the inheritance
	 *         tree.
	 */
	public ObjectRepresentation getActualRepresentation()
	{
		return this.representations.getActual();
	}

	public List<ObjectRepresentation> getAllRepresentations()
	{
		return this.representations.getAllRepresentations();
	}

	/**
	 * Check if this object stack contains a certain class.
	 * 
	 * @param clazz
	 */
	public boolean containsClass(Class<?> clazz)
	{
		for (ObjectRepresentation rep : getAllRepresentations())
		{
			if (rep.getRepresentedClass().equals(clazz))
			{
				return true;
			}
		}
		return false;
	}

	public boolean contains(Node node)
	{
		return containsClass(node.getRepresentation().getRepresentedClass());
	}

	public class Node
	{
		private List<Node> superclasses = new ArrayList<Node>();
		private ObjectRepresentation representation;
		private int height;
		private boolean saved;

		public Node(ObjectRepresentation rep, int height)
		{
			this.representation = rep;
			this.height = height;
		}

		public void replaceSuper(int index, Node realNode)
		{
			superclasses.remove(index);
			superclasses.add(index, realNode);
		}

		public void setSaved(boolean b)
		{
			saved = b;
		}

		public boolean isSaved()
		{
			return saved;
		}

		public Node addSuper(ObjectRepresentation rep)
		{
			Node n = new Node(rep, height + 1);
			superclasses.add(n);
			return n;
		}

		public int getHeight()
		{
			return height;
		}

		/**
		 * Get the direct superclasses and implemented interface of the class
		 * contained in this node.
		 */
		public List<Node> getSupers()
		{
			return superclasses;
		}

		public boolean hasSuper(Node n)
		{
			return getSupers().contains(n);
		}

		public ObjectRepresentation getRepresentation()
		{
			return representation;
		}

		/**
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString()
		{
			return representation.toString();
		}

		/**
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj)
		{
			boolean res = false;
			if (obj instanceof Node)
			{
				Node n = (Node) obj;
				if (n.getRepresentation().equals(getRepresentation()))
				{
					res = true;
				}
			}
			return res;
		}
	}

	/**
	 * Keep track of all subclass-superclass relations this object is part of.
	 * Since any class in an object can have more than one superclass (we are
	 * here counting interfaces as superclasses), but only one subclass, this is
	 * an inverted tree with the root in the actual ObjectRepresentation that
	 * represents the object, and the ObjectRepresentation that represents
	 * java.lang.Object is one of the leaf nodes. The other leaf nodes are
	 * interfaces that this object or any of its superclasses implements.
	 * 
	 * @author Erik Berglund
	 * 
	 */
	private class RepresentationTree
	{
		Node root;

		public RepresentationTree()
		{
		}

		public List<ObjectRepresentation> getAllRepresentations()
		{
			List<ObjectRepresentation> res = new ArrayList<ObjectRepresentation>();
			List<Node> allNodes = allNodes();
			for (Node n : allNodes)
			{
				if (!res.contains(n.getRepresentation()))
				{
					res.add(n.getRepresentation());
				}
			}
			return res;

		}

		public void setActual(ObjectRepresentation actual)
		{
			this.root = new Node(actual, 0);
		}

		public ObjectRepresentation getActual()
		{
			return root.getRepresentation();
		}

		/**
		 * Calculate the maximum height from root to leaf
		 * 
		 */
		public int getMaxHeight()
		{
			return getMaxHeight(root);
		}

		/**
		 * Get the maximum height of node n.
		 * 
		 * @param n
		 */
		private int getMaxHeight(Node n)
		{
			int height = n.getHeight();
			for (Node t : n.getSupers())
			{
				height = Math.max(height, getMaxHeight(t));
			}
			return height;
		}

		/**
		 * Get a list of all the nodes in no particular order.
		 * 
		 */
		public List<Node> allNodes()
		{
			List<Node> res = new ArrayList<Node>();
			addNode(res, root);

			return res;
		}

		/**
		 * Recursively add nodes to a list.
		 * 
		 * @param res
		 * @param n
		 */
		private void addNode(List<Node> res, Node n)
		{
			res.add(n);
			for (Node t : n.getSupers())
			{
				addNode(res, t);
			}
		}
	}

	/**
	 * Get a reference to the adapter used by this stack.
	 * 
	 */
	public AdapterBase getAdapter()
	{
		return adapter;
	}

	/**
	 * Get the one node that has sup as a super.
	 * 
	 * @param sup
	 */
	public Node getSubNode(Node sup)
	{
		Node res = null;
		List<Node> all = representations.allNodes();
		for (Node a : all)
		{
			if (hasSuper(a, sup))
			{
				res = a;
				break;
			}
		}
		return res;
	}

	/**
	 * Check if it is possible that any object of the represented class has a
	 * self-reference. The method is recursive.
	 * 
	 */
	public boolean canContainCircularReferences()
	{
		// get the class that we want to check for circular references
		Class<?> real = this.getActual().getRepresentation()
				.getRepresentedClass();
		List<Class<?>> checkedClasses = new ArrayList<>();
		return canContainCircularReferences(real, checkedClasses);
	}

	private boolean canContainCircularReferences(Class<?> realClass,
			List<Class<?>> checkedClasses)
	{

		for (String property : this.getAllPropertyNames())
		{
			// get the representation that holds the given property
			ObjectRepresentation rep = getRepresentation(property);
			// check if the property is non-primitive
			if (!rep.isPrimitive(property))
			{
				// get the property class
				Class<?> propClass = rep.getReturnType(property);
				if (propClass.isArray())
				{
					propClass = propClass.getComponentType();
				}
				if (!checkedClasses.contains(propClass))
				{
					if (propClass.isAssignableFrom(realClass))
					{
						return true;
					}
					checkedClasses.add(propClass);
					ObjectStack subStack = new ObjectStack(adapter, propClass);
					if (subStack.canContainCircularReferences(realClass,
							checkedClasses))
					{
						return true;
					}
				}
			}
		}
		return false;
	}

	/**
	 * Returns true if any of this stack's properties is not a database
	 * primitive.
	 * 
	 */
	public boolean hasNonPrimitiveProperty()
	{
		List<ObjectRepresentation> allRepresentations = this
				.getAllRepresentations();
		for (ObjectRepresentation rep : allRepresentations)
		{
			if (rep.hasNonPrimitiveProperty())
			{
				return true;
			}
		}
		return false;
	}

}
