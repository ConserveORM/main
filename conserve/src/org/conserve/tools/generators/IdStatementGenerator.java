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
package org.conserve.tools.generators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.conserve.adapter.AdapterBase;
import org.conserve.select.Clause;
import org.conserve.tools.Defaults;
import org.conserve.tools.metadata.ObjectRepresentation;
import org.conserve.tools.metadata.ObjectStack;
import org.conserve.tools.metadata.ObjectStack.Node;

/**
 * Generates a statements that does an inner join on all the tables in a stack.
 * 
 * @author Erik Berglund
 * 
 */
public class IdStatementGenerator
{
	private AsStatementGenerator asGenerator;
	private boolean addJoins;

	private List<String> joinTables = new ArrayList<>();
	private List<String> joinTableIds = new ArrayList<>();
	private List<String> joinPropertyTables = new ArrayList<>();
	private List<String> joinPropertyTableIds = new ArrayList<>();
	private List<ObjectRepresentation> joinRepresentations = new ArrayList<>();

	private HashMap<String, ArrayList<String>> joinedTables = new HashMap<String, ArrayList<String>>();
	private List<RelationDescriptor> relationDescriptors = new ArrayList<RelationDescriptor>();
	private AdapterBase adapter;

	/**
	 * @param adapter
	 */
	public IdStatementGenerator(AdapterBase adapter, ObjectStack oStack,
			Clause[] clauses, boolean addJoins)
	{
		this.adapter = adapter;
		this.addJoins = addJoins;
		this.asGenerator = new AsStatementGenerator(this.adapter);
		this.addTablesToJoin(oStack);
		if (addJoins)
		{
			Node linker = oStack.getNode(Object.class);
			if (linker == null)
			{
				linker = oStack.getActual();
			}
			// recursively add join tables
			addLinks(oStack, oStack.getActual(), linker);
		}
	}



	/**
	 * Recursively add link statements between each class and its superclass and
	 * interfaces.
	 * 
	 * @param oStack
	 * @param rep
	 *            the class to add links for.
	 */
	private void addLinks(ObjectStack oStack, Node rep, Node linker)
	{
		if(!rep.equals(linker))
		{
			addLinkStatement(rep.getRepresentation(), linker.getRepresentation());
		}
		List<Node> supers = oStack.getSupers(rep);
		for (Node sup : supers)
		{
			// recurse
			addLinks(oStack, sup,linker);
		}
	}

	/**
	 * Generate an id statement.
	 * 
	 * @return the identity statement as a string.
	 */
	public String generate()
	{
		StringBuilder tmp = new StringBuilder(100);
		// generate the id statement
		for (RelationDescriptor rdesc : getRelationDescriptors())
		{
			if (tmp.length() > 0)
			{
				tmp.append(" AND ");
			}
			tmp.append(rdesc.toString());
		}
		return tmp.toString();
	}


	public void addRelationDescriptor(RelationDescriptor desc)
	{
		this.relationDescriptors.add(desc);
	}

	/**
	 * Get a reference to the relationship descriptors in this object.
	 * Changing the returned value will change this object.
	 * 
	 * @return a reference to the list of relationship descriptors in this object.
	 */
	public List<RelationDescriptor> getRelationDescriptors()
	{
		return this.relationDescriptors;
	}


	/**
	 * Generate a statement that lists all query tables and their short name, separated by commas.
	 * @return a piece of SQL that describes the short names of tables.
	 */
	public String generateAsStatement()
	{
		List<String> tables = new ArrayList<String>();
		tables.addAll(joinTables);
		tables.addAll(joinPropertyTables);
		List<String> ids = new ArrayList<String>();
		ids.addAll(joinTableIds);
		ids.addAll(joinPropertyTableIds);
		return asGenerator.generate(tables, ids);
	}

	public void addPropertyTableToJoin(String tableName, String asName)
	{
		if (!joinTableIds.contains(asName)
				&& !joinPropertyTableIds.contains(asName))
		{
			joinPropertyTables.add(tableName);
			joinPropertyTableIds.add(asName);
		}
	}

	public void addTableToJoin(String tableName, String asName)
	{
		if (!joinTableIds.contains(asName)
				&& !joinPropertyTableIds.contains(asName))
		{
			joinTables.add(tableName);
			joinTableIds.add(asName);
		}
	}
	
	public void addLeftJoin(JoinDescriptor join)
	{
		asGenerator.addJoin(join);
	}

	/**
	 * Set a and b as joined by this statement.
	 * 
	 * @param a a table shortname.
	 * @param b a table shortname.
	 */
	private void setJoined(String a, String b)
	{
		// insert both the a->b and the b->a combination, as this makes queries
		// faster.
		// a->b combo
		ArrayList<String> aList = joinedTables.get(a);
		if (aList == null)
		{
			aList = new ArrayList<String>();
			joinedTables.put(a, aList);
		}
		if (!aList.contains(b))
		{
			aList.add(b);
		}
		// b->a combo
		ArrayList<String> bList = joinedTables.get(b);
		if (bList == null)
		{
			bList = new ArrayList<String>();
			joinedTables.put(b, bList);
		}
		if (!bList.contains(a))
		{
			bList.add(a);
		}
	}

	/**
	 * Indicate that superRep and objRep are joined.
	 * 
	 * @param superRep
	 * @param objRep
	 */
	public void addLinkStatement(ObjectRepresentation superRep,
			ObjectRepresentation objRep)
	{
		// check if the representations have already been joined
		if (!isJoined(superRep.getAsName(), objRep.getAsName()))
		{
			// if not, add a statement to join IDs
			FieldDescriptor obj = new FieldDescriptor(objRep.getTableName(),
					objRep.getAsName(), Defaults.ID_COL);
			FieldDescriptor sup = new FieldDescriptor(superRep.getTableName(),
					superRep.getAsName(), Defaults.ID_COL);
			RelationDescriptor relDesc = new RelationDescriptor(obj, sup);
			addRelationDescriptor(relDesc);

			// indicate that the representations are linked
			setJoined(superRep.getAsName(), objRep.getAsName());
		}
	}

	/**
	 * Add all tables in propertyStack.
	 * 
	 * @param propertyStack the stack to join.
	 */
	public void addPropertyTablesToJoin(ObjectStack propertyStack)
	{
		boolean propertiesFound = false;
		List<ObjectRepresentation> allReps = propertyStack
				.getAllRepresentations();
		for (ObjectRepresentation rep : allReps)
		{
			if (!propertiesFound)
			{
				if (rep.getNonNullPropertyCount() > 0 || rep.isArray())
				{
					propertiesFound = true;
				}
			}
			if (propertiesFound)
			{
				if (rep.isArray())
				{
					addPropertyTableToJoin(NameGenerator.getArrayTablename(adapter),
							rep.getAsName());
				}
				else
				{
					addPropertyTableToJoin(rep.getTableName(), rep.getAsName());
				}
			}
		}
	}

	/**
	 * Add all tables in stack, up to and including the table for propertyClass
	 * 
	 * @param stack the stack to join, may not be null.
	 */
	public void addTablesToJoin(ObjectStack stack)
	{
		// add the actual representation
		ObjectRepresentation actual = stack.getActualRepresentation();
		this.joinRepresentations.add(actual);
		if (actual.isArray())
		{
			addTableToJoin(NameGenerator.getArrayTablename(adapter), actual.getAsName());
		}
		else
		{
			addTableToJoin(actual.getTableName(), actual.getAsName());
		}
		if (addJoins)
		{
			// if we should, add all others
			List<ObjectRepresentation> allReps = stack.getAllRepresentations();
			for (ObjectRepresentation rep : allReps)
			{
				if (!rep.equals(actual))
				{
					this.joinRepresentations.add(rep);
					if (rep.isArray())
					{
						addTableToJoin(NameGenerator.getArrayTablename(adapter),
								rep.getAsName());
					}
					else
					{
						addTableToJoin(rep.getTableName(), rep.getAsName());
					}
				}
			}
		}
	}

	/**
	 * Check if a and b have already been joined by this statement prototype.
	 * 
	 * @param a
	 * @param b
	 * @return true if a and b are already joined, false otherwise.
	 */
	public boolean isJoined(String a, String b)
	{
		ArrayList<String> aList = joinedTables.get(a);
		if (aList != null)
		{
			if (aList.contains(b))
			{
				return true;
			}
		}
		return false;
	}

	/**
	 * @return the joinTables
	 */
	public List<String> getJoinTables()
	{
		return joinTables;
	}

	/**
	 * @return the joinTableIds
	 */
	public List<String> getJoinTableIds()
	{
		return joinTableIds;
	}

	/**
	 * @return the joinRepresentations
	 */
	public List<ObjectRepresentation> getJoinRepresentations()
	{
		return joinRepresentations;
	}



	/**
	 * Get values to be inserted in '?' places in the query.
	 */
	public List<Object> getValues()
	{
		return asGenerator.getValues();
	}

}
