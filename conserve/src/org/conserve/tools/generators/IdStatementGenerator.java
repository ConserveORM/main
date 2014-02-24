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
package org.conserve.tools.generators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.conserve.adapter.AdapterBase;
import org.conserve.tools.Defaults;
import org.conserve.tools.NameGenerator;
import org.conserve.tools.metadata.ObjectRepresentation;
import org.conserve.tools.metadata.ObjectStack;
import org.conserve.tools.metadata.ObjectStack.Node;

/**
 * @author Erik Berglund
 * 
 */
public class IdStatementGenerator
{
	private AsStatementGenerator asGenerator;
	private boolean addJoins;

	private List<String> joinTables = new ArrayList<String>();
	private List<String> joinTableIds = new ArrayList<String>();
	private List<String> joinPropertyTables = new ArrayList<String>();
	private List<String> joinPropertyTableIds = new ArrayList<String>();
	private List<ObjectRepresentation> joinRepresentations = new ArrayList<ObjectRepresentation>();

	private HashMap<String, ArrayList<String>> joinedTables = new HashMap<String, ArrayList<String>>();
	private List<RelationDescriptor> relationDescriptors = new ArrayList<RelationDescriptor>();

	/**
	 * @param adapter
	 */
	public IdStatementGenerator(AdapterBase adapter, ObjectStack oStack,
			boolean addJoins)
	{
		this.addJoins = addJoins;
		this.asGenerator = new AsStatementGenerator(adapter);
		this.addTablesToJoin(oStack);
		if (addJoins)
		{
			// recursively add join tables
			addLinks(oStack, oStack.getActual());
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
	private void addLinks(ObjectStack oStack, Node rep)
	{
		List<Node> supers = oStack.getSupers(rep);
		for (Node sup : supers)
		{
			addLinkStatement(sup.getRepresentation(), rep.getRepresentation());
			// recurse
			addLinks(oStack, sup);
		}
	}

	/**
	 * Generate an id statement.
	 * 
	 * @param oStack
	 * @param minLevel
	 * @return
	 */
	public String generate(int minLevel)
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

	/**
	 * Generate a linking statement that uses nested inner joins.
	 * 
	 * @param tablesToShorten
	 *            optional list of tables to replace with their respective short
	 *            name.
	 * @return
	 */
	public String generateInnerJoins(String[] tablesToShorten)
	{
		List<String> shortenTables = new ArrayList<String>();
		if (tablesToShorten != null)
		{
			for (String s : tablesToShorten)
			{
				shortenTables.add(s);
			}
		}
		StringBuilder tmp = new StringBuilder(100);
		// generate the id statement
		if (addJoins)
		{
			for (RelationDescriptor rdesc : getRelationDescriptors())
			{

				if (tmp.length() > 0)
				{
					tmp.append(" AND ");
				}
				FieldDescriptor first = rdesc.getFirst();
				FieldDescriptor second = rdesc.getSecond();
				if (shortenTables.contains(first.getTableName()))
				{
					tmp.append(first.toShortString());
				}
				else
				{
					tmp.append(first.toFullString());
				}
				tmp.append(" = ");
				if (rdesc.isRequiresvalue())
				{
					tmp.append("?");
				}
				else
				{
					if (shortenTables.contains(second.getTableName()))
					{
						tmp.append(second.toShortString());
					}
					else
					{
						tmp.append(second.toFullString());
					}
				}
			}
		}
		return tmp.toString();
	}

	public void addRelationDescriptor(RelationDescriptor desc)
	{
		this.relationDescriptors.add(desc);
	}

	public List<RelationDescriptor> getRelationDescriptors()
	{
		return this.relationDescriptors;
	}

	public String generateAsStatement()
	{
		return generateAsStatement(new String[0]);
	}

	public String generateAsStatement(String[] omitTables)
	{
		List<String> tables = new ArrayList<String>();
		tables.addAll(joinTables);
		tables.addAll(joinPropertyTables);
		List<String> ids = new ArrayList<String>();
		ids.addAll(joinTableIds);
		ids.addAll(joinPropertyTableIds);
		return asGenerator.generate(tables, ids, relationDescriptors,
				omitTables);
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

	/**
	 * Set a and b as joined by this statement.
	 * 
	 * @param a
	 * @param b
	 */
	public void setJoined(String a, String b)
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
					superRep.getAsName(), Defaults.REAL_ID_COL);
			RelationDescriptor relDesc = new RelationDescriptor(obj, sup);
			addRelationDescriptor(relDesc);

			//add the real class name
			obj = new FieldDescriptor(superRep.getTableName(),
					superRep.getAsName(), Defaults.REAL_CLASS_COL);
			if (objRep.isArray())
			{
				// arrays have a different tablename
				addRelationDescriptor(new RelationDescriptor(obj,
						Defaults.ARRAY_TABLENAME));
			}
			else
			{
				addRelationDescriptor(new RelationDescriptor(obj,
						NameGenerator.getSystemicName(objRep
								.getRepresentedClass())));
			}
			// indicate that the representations are linked
			setJoined(superRep.getAsName(), objRep.getAsName());
		}
	}

	/**
	 * Add all tables in propertyStack.
	 * 
	 * @param propertyStack
	 */
	public void addPropertyTablesToJoin(ObjectStack propertyStack)
	{
		// TODO: Must this recurse from specific to general?
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
					addPropertyTableToJoin(Defaults.ARRAY_TABLENAME,
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
	 * @param stack
	 *            may be null.
	 */
	public void addTablesToJoin(ObjectStack stack)
	{
		// add the actual representation
		ObjectRepresentation actual = stack.getActualRepresentation();
		this.joinRepresentations.add(actual);
		if(actual.isArray())
		{
			addTableToJoin(Defaults.ARRAY_TABLENAME, actual.getAsName());
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
						addTableToJoin(Defaults.ARRAY_TABLENAME,
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

}
