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
package org.conserve.tools.metadata;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.conserve.adapter.AdapterBase;
import org.conserve.connection.ConnectionWrapper;
import org.conserve.tools.Defaults;
import org.conserve.tools.DelayedInsertionBuffer;
import org.conserve.tools.NameGenerator;
import org.conserve.tools.ObjectTools;

/**
 * Wrapper that encapsulates all ObjectRepresentations for an object.
 * 
 * 
 * @author Erik Berglund
 * 
 */
public class ObjectStack
{
	//private ArrayList<ObjectRepresentation> representations = new ArrayList<ObjectRepresentation>();
	private RepresentationTree representations = new RepresentationTree();
	private AdapterBase adapter;

	public ObjectStack(AdapterBase adapter, Class<?> c)
	{
		this(adapter, c, null);
	}

	public ObjectStack(AdapterBase adapter, Class<?> c, Object o)
	{
		this(adapter, c, o, null);
	}

	public ObjectStack(AdapterBase adapter, Class<?> c, Object o, DelayedInsertionBuffer delayBuffer)
	{
		this.adapter = adapter;
		Node tmp = null;
		while (c != null)
		{
			ObjectRepresentation rep = new ConcreteObjectRepresentation(adapter, c, o, delayBuffer);
			if(tmp == null)
			{
				representations.setActual(rep);
				tmp = representations.root;
			}
			else
			{
				tmp = tmp.addSuper(rep);
			}
			c = c.getSuperclass();
		}
		// remove duplicate values
		for (int x = getSize() - 1; x >= 0; x--)
		{
			// get the representation at this level
			ObjectRepresentation rep = getRepresentation(x);
			// find the properties at this level
			for (int p = 0; p < rep.getPropertyCount(); p++)
			{
				String name = rep.getPropertyName(p);
				// check if any of the superclasses has a similarly-named
				// property
				for (int y = x - 1; y >= 0; y--)
				{
					ObjectRepresentation superRep = getRepresentation(y);
					if (superRep.hasProperty(name))
					{
						rep.removeProperty(p);
						p--;
						break;
					}
				}
			}
		}
		// check for implementations of Collection
		if (this.getActualRepresentation().isImplementation(Collection.class))
		{
			// find the lowest level that still implements Collection
			ObjectRepresentation lastKnownImplementor = getActualRepresentation();
			// we know that the bottom implements collection, skip it
			for (int x = this.getSize() - 2; x >= 0; x--)
			{
				if (this.getRepresentation(x).isImplementation(Collection.class))
				{
					lastKnownImplementor = this.getRepresentation(x);
				}
				else
				{
					break;
				}
			}
			// switch on the virtual properties in this level
			lastKnownImplementor.implementCollection();
		}
		// do the same for Map as for Collection
		if (this.getActualRepresentation().isImplementation(Map.class))
		{
			// find the lowest level that still implements Collection
			ObjectRepresentation lastKnownImplementor = getActualRepresentation();
			// we know that the bottom implements collection, skip it
			for (int x = this.getSize() - 2; x >= 0; x--)
			{
				if (this.getRepresentation(x).isImplementation(Map.class))
				{
					lastKnownImplementor = this.getRepresentation(x);
				}
				else
				{
					break;
				}
			}
			// switch on the virtual properties in this level
			lastKnownImplementor.implementMap();
		}
	}

	/**
	 * @param adapter
	 * @param list
	 */
	public ObjectStack(AdapterBase adapter, List<ObjectRepresentation> list)
	{
		this.adapter = adapter;
		Node tmp = null;
		for (ObjectRepresentation k : list)
		{
			if(tmp == null)
			{
				representations.setActual(k);
				tmp = representations.root;
			}
			else
			{
				tmp = tmp.addSuper(k);
			}
		}
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
		return representations.getMaxHeight()+1;
	}

	/**
	 * Get the representation at a given level.
	 * 
	 * @param level
	 * @return the reperesentation of this object at the given class level.
	 */
	public ObjectRepresentation getRepresentation(int level)
	{
		//TODO: Handle multiple supers
		int size = getSize();
		for(Node n:representations.allNodes())
		{
			int l = size-n.getHeight()-1;
			if(l == level)
			{
				return n.getRepresentation();
			}
		}
		return null;
	}

	/**
	 * Get the representation that has a given property name. If there is no
	 * such representation, return null.
	 * 
	 * @param propertyName
	 * @return
	 */
	public ObjectRepresentation getRepresentation(String propertyName)
	{
		ObjectRepresentation res = null;
		int maxLevel = -1;
		for(Node n:representations.allNodes())
		{
			if(n.getRepresentation().hasProperty(propertyName) && n.getHeight()>maxLevel)
			{
				maxLevel = n.getHeight();
				res = n.getRepresentation();
			}
		}
		return res;
	}

	/**
	 * Get the level at which the given property is represented. If there is no
	 * such representation, return negative.
	 * 
	 * @param propertyName
	 * @return
	 */
	public int getRepresentationLevel(String propertyName)
	{
		int maxLevel = -1;
		for(Node n:representations.allNodes())
		{
			if(n.getRepresentation().hasProperty(propertyName) && n.getHeight()>maxLevel)
			{
				maxLevel = n.getHeight();
			}
		}
		if(maxLevel>=0)
		{
			return representations.getMaxHeight()-maxLevel;
		}
		else
		{
			return -1;
		}
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
		int level = getLevel(clazz);
		if (level >= 0)
		{
			return getRepresentation(level);
		}
		else
		{
			return null;
		}
	}

	/**
	 * 
	 * Get the representational level of the given class. Returns -1 if there is
	 * no such class in the stack.
	 * 
	 * @param c
	 * @return the level (where 0 equals Object.class) of the class c within
	 *         this stack.
	 */
	public int getLevel(Class<?> c)
	{
		if (c.isInterface())
		{
			// find the highest level that implements the interface
			for (int x = getSize() - 1; x >= 0; x--)
			{
				Class<?> candidate = getRepresentation(x).getRepresentedClass();
				if (ObjectTools.implementsInterfaceIncludingSuper(candidate, c))
				{
					return x;
				}
			}

		}
		else
		{
			for (int x = 0; x < getSize(); x++)
			{
				if (getRepresentation(x).getRepresentedClass().equals(c))
				{
					return x;
				}
			}
		}
		return -1;
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
		saveNoCache(cw, 0);
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
	public void saveNoCache(ConnectionWrapper cw, int minLevel) throws SQLException
	{

		// storage for the subclass name and table id
		String className = null;
		Long id = null;
		// iterate up from the lowest level towards java.lang.Object
		for (int x = getSize() - 1; x >= minLevel; x--)
		{
			// only ConcreteObjectRepresentations can be saved
			ConcreteObjectRepresentation rep = (ConcreteObjectRepresentation) getRepresentation(x);
			adapter.getPersist().getTableManager().ensureTableExists(rep, cw);
			rep.save(cw, className, id);
			if (rep.isArray())
			{
				className = Defaults.ARRAY_TABLENAME;
			}
			else
			{
				className = NameGenerator.getSystemicName(rep.getRepresentedClass());
			}
			id = rep.getId();
		}
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
		return getRepresentation(getSize() - 1);
	}

	private class Node
	{
		private List<Node>superclasses = new ArrayList<Node>();
		private ObjectRepresentation representation;
		private int height;
		
		public Node(ObjectRepresentation rep,int height)
		{
			this.representation = rep;
			this.height = height;
		}
		
		public Node addSuper(ObjectRepresentation rep)
		{
			Node n = new Node(rep,height+1);
			superclasses.add(n);
			return n;
		}
		
		public int getHeight()
		{
			return height;
		}
		
		public List<Node>getSupers()
		{
			return superclasses;
		}
		public List<ObjectRepresentation>getSuperReps()
		{
			List<ObjectRepresentation>res = new ArrayList<ObjectRepresentation>();
			for(Node n:superclasses)
			{
				res.add(n.getRepresentation());
			}
			return res;
		}
		public ObjectRepresentation getRepresentation()
		{
			return representation;
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
		
		public void setActual(ObjectRepresentation actual)
		{
			this.root = new Node(actual,0);
		}
		
		public ObjectRepresentation getActual()
		{
			return root.getRepresentation();
		}
		
		/**
		 * Calculate the maximum height from root to leaf
		 * @return
		 */
		public int getMaxHeight()
		{
			return getMaxHeight(root);
		}
		
		/**
		 * @param root2
		 * @return
		 */
		private int getMaxHeight(Node n)
		{
			int height = n.getHeight();
			for(Node t:n.getSupers())
			{
				height = Math.max(height, getMaxHeight(t));
			}
			return height;
		}

		/**
		 * Get a list of all the nodes in no particular order.
		 * @return
		 */
		public List<Node> allNodes()
		{
			List<Node>res = new ArrayList<Node>();
			addNode(res,root);
			
			return res;
		}

		/**
		 * Recursivley add nodes to a list.
		 * 
		 * @param res
		 * @param n
		 */
		private void addNode(List<Node> res, Node n)
		{
			res.add(n);
			for(Node t:n.getSupers())
			{
				addNode(res,t);
			}
		}


	}

}
