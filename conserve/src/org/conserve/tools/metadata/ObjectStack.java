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
package org.conserve.tools.metadata;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.conserve.adapter.AdapterBase;
import org.conserve.connection.ConnectionWrapper;
import org.conserve.tools.Defaults;
import org.conserve.tools.DelayedInsertionBuffer;
import org.conserve.tools.NameGenerator;
import org.conserve.tools.ObjectTools;
import org.conserve.tools.Tools;

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
	public ObjectStack(ConnectionWrapper cw, AdapterBase adapter, Class<?> klass) throws SQLException, ClassNotFoundException
	{
		this.adapter = adapter;
		// fill in the root node
		ObjectRepresentation dor = new DatabaseObjectRepresentation(adapter, klass, cw);
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
	public ObjectStack(AdapterBase adapter, Class<?> c, Object o, DelayedInsertionBuffer delayBuffer)
	{
		this.adapter = adapter;
		ObjectRepresentation or = new ConcreteObjectRepresentation(adapter, c, o, delayBuffer);
		representations.setActual(or);
		// recursively load the rest of the representations
		loadRecursively(representations.root, o, delayBuffer);

		// calculate lowest level
		calculateLevels();
		// remove duplicate values
		removeDuplicatedProperties();
		// clean up the tree
		removeDuplicatedNodes(representations.root, new ArrayList<Class<?>>(), new ArrayList<Node>());

		// check for implementations of Collection
		if (this.getActualRepresentation().isImplementation(Collection.class))
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
		if (this.getActualRepresentation().isImplementation(Map.class))
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

	private void removeDuplicatedNodes(Node node, List<Class<?>> seenClasses, List<Node> nodes)
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
	private void loadRecursively(ConnectionWrapper cw, Node n) throws SQLException, ClassNotFoundException
	{
		if (n.getRepresentation().getRepresentedClass().equals(Object.class))
		{
			heightOfObject = n.getHeight();
		}
		List<Class<?>> supers = getSuperClassesFromDatabase(n.getRepresentation().getRepresentedClass(), cw);
		for (Class<?> s : supers)
		{
			ObjectRepresentation dor = new DatabaseObjectRepresentation(adapter, s, cw);
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
	private void loadRecursively(Node n, Object o, DelayedInsertionBuffer delayBuffer)
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
			if (!ObjectTools.implementsInterfaceIncludingSuper(superClass, i))
			{
				supers.add(i);
			}
		}
		// recursively go up the tree
		for (Class<?> s : supers)
		{
			ObjectRepresentation or = new ConcreteObjectRepresentation(adapter, s, o, delayBuffer);
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
	private List<Class<?>> getSuperClassesFromDatabase(Class<?> subClass, ConnectionWrapper cw) throws SQLException
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
			if (ps.execute())
			{
				ResultSet rs = ps.getResultSet();
				while (rs.next())
				{
					String name = rs.getString(1);
					Class<?> c = ObjectTools.lookUpClass(name, adapter);
					res.add(c);
				}
				rs.close();
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
	 * @return
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
	 * @return
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
	 * @return
	 */
	public ObjectRepresentation getRepresentation(String propertyName)
	{
		ObjectRepresentation res = null;
		int maxLevel = -1;
		for (Node n : representations.allNodes())
		{
			if (n.getRepresentation().hasProperty(propertyName) && n.getHeight() > maxLevel)
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
	List<String>getAllPropertyNames()
	{
		List<String>res = new ArrayList<>();
		for(Node n:representations.allNodes())
		{
			ObjectRepresentation rep = n.getRepresentation();
			for(int x = 0;x<rep.getPropertyCount();x++)
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
	 * @return
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
	 * Save the object represented by this ObjectStack, and all referenced
	 * objects, as necessary. Create cache copy.
	 * 
	 * @param cw
	 * 
	 * @throws SQLException
	 */
	public void save(ConnectionWrapper cw) throws SQLException
	{
		saveNoCache(cw, null);
		// store the object in the object-row map
		ObjectRepresentation rep = this.getActualRepresentation();
		adapter.getPersist().saveToCache(rep.getTableName(), rep.getObject(), rep.getId());
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
	public void saveNoCache(ConnectionWrapper cw, Integer minLevel) throws SQLException
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
	private void saveNoCache(ConnectionWrapper cw, Node root, Integer minLevel, String subClassName, Long id) throws SQLException
	{
		if (id == null)
		{
			// Find the non-interface objects between root and java.lang.Object.
			// Save the java.lang.Object first, then its children down to root
			// Then save all superclasses of root that have not been saved
			// already (that is, all interfaces)
			Node realSuper = root;
			//the direct subclass of the real super class
			Node subSuper = root;
			while (!realSuper.getRepresentation().getRepresentedClass().equals(Object.class))
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
			ConcreteObjectRepresentation objeRep = (ConcreteObjectRepresentation) realSuper.getRepresentation();
			adapter.getPersist().getTableManager().ensureTableExists(objeRep, cw);
			String subName = null;
			if(!realSuper.equals(subSuper))
			{
				subName = subSuper.getRepresentation().getSystemicName(); 
				if(subSuper.getRepresentation().isArray())
				{
					subName = NameGenerator.getArrayTablename(adapter);
				}
			}
			objeRep.save(cw, subName , null);
			id= objeRep.getId();
			realSuper.setSaved(true);
		}

		if (!root.isSaved())
		{
			root.setSaved(true);

			// save the object
			ConcreteObjectRepresentation crep = (ConcreteObjectRepresentation) root.getRepresentation();
			adapter.getPersist().getTableManager().ensureTableExists(crep, cw);
			crep.save(cw, subClassName, id);

			// get the current class name
			String className = root.getRepresentation().getSystemicName();
			if (root.getRepresentation().isArray())
			{
				className = NameGenerator.getArrayTablename(adapter);
			}

			// get the superclasses
			// this is done out-of-order to match up to search order, see
			// TreePathListener.java.
			List<Node> supers = root.getSupers();
			if (supers.size() > 0)
			{
				for (int x = 1; x < supers.size(); x++)
				{
					Node s = supers.get(x);
					// recurse
					// interfaces are saved all the way to the top,
					// minLevel is ignored if null
					// otherwise go up to minlevel
					if (s.getRepresentation().isInterface() || minLevel == null || heightToLevel(s.getHeight()) >= minLevel)
					{
						saveNoCache(cw, s, minLevel, className, crep.getId());
					}
				}
				Node s = supers.get(0);
				if (s.getRepresentation().isInterface() || minLevel == null || heightToLevel(s.getHeight()) >= minLevel)
				{
					saveNoCache(cw, s, minLevel, className, crep.getId());
				}
			}
		}
	}

	/**
	 * Get the actual object representation node, or the root node.
	 * 
	 * @return
	 */
	public Node getActual()
	{
		return this.representations.root;
	}

	/**
	 * Get a list of all table names in this stack.
	 * 
	 * @return
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
	 * @return
	 */
	public int getMaxLevel()
	{
		return heightToLevel(0);
	}

	/**
	 * Return the smallest level-number any class in the stack has.
	 * java.lang.Object has level = 0.
	 * 
	 * @return
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
	 * @return
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
		 * 
		 * @return
		 */
		public List<Node> getSupers()
		{
			return superclasses;
		}

		public ObjectRepresentation getRepresentation()
		{
			return representation;
		}

		/**
		 * Remove a super-class.
		 * 
		 * @param node
		 */
		public void removeSuper(Node node)
		{
			superclasses.remove(node);
		}
		
		/**
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString()
		{
			return representation.toString();
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

		/**
		 * @return
		 * 
		 */
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
		 * @return
		 */
		public int getMaxHeight()
		{
			return getMaxHeight(root);
		}

		/**
		 * @param n
		 * @return
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
		 * @return
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

		/**
		 * Remove a node from the tree.
		 * 
		 * @param node
		 */
		public void removeNode(Node node)
		{
			List<Node> all = allNodes();
			for (Node n : all)
			{
				n.removeSuper(node);
			}
		}

	}

	/**
	 * Remove nodes that fulfil these criteria:
	 * 
	 * 1. No superclasses
	 * 
	 * 2. No properties
	 */
	public void pruneEmptyClasses()
	{
		List<Node> nodes = this.representations.allNodes();
		boolean found = true;
		while (found)
		{
			found = false;
			for (int x = 0; x < nodes.size(); x++)
			{
				Node node = nodes.get(x);
				if (node.getSupers().isEmpty() && node.getRepresentation().getPropertyCount() == 0)
				{
					found = true;
					representations.removeNode(node);
					nodes.remove(x);
					x--;
				}
			}
		}
	}

	/**
	 * Get a reference to the adapter used by this stack.
	 * 
	 * @return
	 */
	public AdapterBase getAdapter()
	{
		return adapter;
	}
	

}
