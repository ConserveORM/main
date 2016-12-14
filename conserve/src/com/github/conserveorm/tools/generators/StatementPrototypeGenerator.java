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
package com.github.conserveorm.tools.generators;

import java.lang.reflect.Array;
import java.lang.reflect.Modifier;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;

import com.github.conserveorm.adapter.AdapterBase;
import com.github.conserveorm.select.Clause;
import com.github.conserveorm.select.ConditionalClause;
import com.github.conserveorm.select.discriminators.Selector;
import com.github.conserveorm.sort.Order;
import com.github.conserveorm.sort.Sorter;
import com.github.conserveorm.tools.Defaults;
import com.github.conserveorm.tools.DelayedInsertionBuffer;
import com.github.conserveorm.tools.ObjectTools;
import com.github.conserveorm.tools.StatementPrototype;
import com.github.conserveorm.tools.metadata.ObjectRepresentation;
import com.github.conserveorm.tools.metadata.ObjectStack;
import com.github.conserveorm.tools.metadata.ObjectStack.Node;
import com.github.conserveorm.tools.uniqueid.UniqueIdGenerator;
import com.github.conserveorm.tools.uniqueid.UniqueIdTree;

/**
 * Generates SQL "WHERE..." sub-clauses based on a given Where object and the
 * AdapterBase.
 * 
 * @author Erik Berglund
 * 
 */
public class StatementPrototypeGenerator
{
	private AdapterBase adapter;
	private UniqueIdGenerator uidGenerator;
	private UniqueIdTree typeIds;
	private HashMap<String, UniqueIdTree> parameterTypeIds;
	private ObjectStack typeStack;
	private Clause[] clauses;

	public StatementPrototypeGenerator(AdapterBase adapter)
	{
		this.adapter = adapter;
		this.uidGenerator = new UniqueIdGenerator();
		parameterTypeIds = new HashMap<String, UniqueIdTree>();
	}

	/**
	 * Generate a statement prototype that looks in Class klass for objects. If
	 * addJoins is true, it will also satisfy all superclasses.
	 * 
	 * @param klass
	 *            the class to generate a statment prototype for.
	 * @param addJoins
	 *            whether satisfy superclass constraints.
	 * @return a prototype that can be used to generate PreparedStatements.
	 * @throws SQLException
	 */
	public StatementPrototype generate(Class<?> klass, boolean addJoins) throws SQLException
	{
		typeStack = new ObjectStack(adapter, klass);
		typeIds = new UniqueIdTree(uidGenerator);
		typeIds.nameStack(typeStack);

		StatementPrototype res = new StatementPrototype(adapter, typeStack, typeStack.getActualRepresentation().getRepresentedClass(), clauses,
				addJoins);

		generateRecursively(res, null, clauses);

		return res;
	}

	private void generateRecursively(StatementPrototype sp, Boolean sorted, Clause... clauses) throws SQLException
	{
		if (clauses != null)
		{
			for (Clause clause : clauses)
			{
				if (clause != null)
				{
					Clause[] subClauses = clause.getSubclauses();
					// handle sorting statements
					if (clause instanceof Sorter)
					{
						generateOrder((Sorter) clause, sp);
					}
					// handle LIMIT and OFFSET clauses
					else if (clause instanceof Order)
					{
						Order order = (Order) clause;
						if (order.getLimit() != null)
						{
							if (sp.getLimit() == null)
							{
								sp.setLimit(order.getLimit());
								if (order.getOffset() != null)
								{
									if (sp.getOffset() == null)
									{
										sp.setOffset(order.getOffset());
									}
									else
									{
										throw new IllegalArgumentException("Multiple offsets defined.");
									}
								}
							}
							else
							{
								throw new IllegalArgumentException("Multiple limits defined.");
							}
						}
						if (subClauses != null)
						{
							for (int x = 0; x < subClauses.length; x++)
							{
								Sorter sorter = (Sorter) subClauses[x];
								generateOrder(sorter, sp);
							}
						}
					}
					// instance of Equals, Greater, etc.
					else if (clause instanceof Selector)
					{
						Selector sel = (Selector) clause;

						ObjectStack oStack = new ObjectStack(adapter, sel.getSelectionClass(), sel.getSelectionObject(),
								new DelayedInsertionBuffer(adapter.getPersist()));
						typeIds.nameStack(oStack);
						generateClause(oStack, sel, sp, sorted);
					}
					// instance of AND, OR...
					else if (clause instanceof ConditionalClause)
					{
						boolean pushed = false;
						if (clause.getKeyWord() != null)
						{
							pushed = true;
							sp.push(clause.getKeyWord());
						}
						for (int x = 0; x < subClauses.length; x++)
						{
							if (subClauses[x] instanceof Selector)
							{
								Selector sel = (Selector) subClauses[x];

								ObjectStack oStack = new ObjectStack(adapter, sel.getSelectionClass(), sel.getSelectionObject(),
										new DelayedInsertionBuffer(adapter.getPersist()));
								typeIds.nameStack(oStack);
								generateClause(oStack, sel, sp, sorted);
							}
							else
							{
								generateRecursively(sp, sorted, subClauses[x]);
							}
						}
						if (pushed)
						{
							sp.pop();
						}
					}
				}
			}
		}
	}

	/**
	 * Create a statement to order results.
	 * 
	 * @param order
	 * @param mainStatement
	 */
	private void generateOrder(Sorter sorter, StatementPrototype mainStatement)
	{
		// add order statements for all non-null values
		ObjectStack oStack = new ObjectStack(adapter, sorter.getSortClass(), sorter.getSortObject());
		this.typeIds.nameStack(oStack);
		mainStatement.getIdStatementGenerator().addPropertyTablesToJoin(oStack);
		for (ObjectRepresentation rep : oStack.getAllRepresentations())
		{
			for (Integer index : rep)
			{
				StringBuilder statement = new StringBuilder();
				statement.append(rep.getAsName());
				statement.append(".");
				statement.append(rep.getPropertyName(index));
				statement.append(" ");
				statement.append(sorter.getKeyWord());
				mainStatement.addSortStatement(statement.toString());
			}
		}
		if (sorter.getSortObject() == null)
		{
			// sort on the main class' C__ID column
			StringBuilder statement = new StringBuilder();
			statement.append(oStack.getActualRepresentation().getAsName());
			statement.append(".");
			statement.append(Defaults.ID_COL);
			statement.append(" ");
			statement.append(sorter.getKeyWord());
			mainStatement.addSortStatement(statement.toString());
		}
	}

	private void generateClause(ObjectStack oStack, Selector sel, StatementPrototype sp, Boolean sorted) throws SQLException
	{
		if (oStack.getActualRepresentation().isArray())
		{
			if (sorted == null)
			{
				sorted = true;
			}
			generateArrayQuery(sel, sp, oStack, sorted);
		}
		else
		{
			generateNonArrayQuery(sel, sp, oStack, sorted);
		}
	}

	/**
	 * Generate query for an object that is not an array (it may contain
	 * arrays).
	 * 
	 * @param sel
	 *            the selection query to generate
	 * @param sp
	 *            the statement prototype to put the query in
	 * @param oStack
	 *            the abstract representation of the object
	 * @param sorted
	 *            true if this query is part of a sorted query
	 * @throws SQLException
	 */
	private void generateNonArrayQuery(Selector sel, StatementPrototype sp, ObjectStack oStack, Boolean sorted) throws SQLException
	{
		if (sorted == null || isCollectionsObject(sel.getSelectionObject().getClass()))
		{
			sorted = isSorted(sel.getSelectionObject().getClass());
		}
		
		ObjectRepresentation baseRep = oStack.getRepresentation(sel.getSelectionClass());
		
		// queries will always include these three tables
		baseRep.setForceInclude(true);
		oStack.getActualRepresentation().setForceInclude(true);
		ObjectRepresentation objectRep = oStack.getRepresentation(Object.class);
		if (objectRep != null)
		{
			oStack.getRepresentation(Object.class).setForceInclude(true);
		}

		// get all possible paths from a superclass to the actual implementing
		// class
		TreePathLister tpl = new TreePathLister();
		List<List<ObjectRepresentation>> allPaths = tpl.generateLists(oStack);
		tpl.prunePaths(allPaths);
		if (!sel.isStrictInheritance())
		{
			tpl.pruneInheritance(allPaths, sel.getSelectionClass());
		}

		// go through the list of paths and add all relevant query parts
		for (List<ObjectRepresentation> path : allPaths)
		{
			for (int t = 0; t < path.size(); t++)
			{
				ObjectRepresentation rep = path.get(t);
				if(!rep.equals(baseRep) )
				{
					if(rep.belongsInJoin())
					{
						sp.getIdStatementGenerator().addLinkStatement(rep,baseRep);
					}
				}
				// make sure the class is in the link table
				if(rep.belongsInJoin())
				{
					sp.getIdStatementGenerator().addPropertyTableToJoin(rep.getTableName(), rep.getAsName());
				}
				
				// store parameters:
				// iterate over the non-null values
				for (Integer x : rep)
				{
					Object property = rep.getPropertyValue(x);
					String propertyName = rep.getPropertyName(x);
					StringBuilder conditional = new StringBuilder(rep.getAsName());
					conditional.append(".");
					conditional.append(propertyName);
					if (rep.isPrimitive(x))
					{
						// the value is a primitive, so insert it directly
						conditional.append(sel.getRelationalRepresentation());
						if (sel.takesPlaceholder())
						{
							conditional.append("?");
							sp.addConditionalStatement(conditional.toString(), property);
						}
						else
						{
							sp.addConditionalStatement(conditional.toString());
						}
					}
					else
					{
						// the value is complex, insert a reference
						Long id = (long) System.identityHashCode(property);
						if (rep.getDelayedInsertionBuffer().isKnown(id, property))
						{
							break;
						}
						rep.getDelayedInsertionBuffer().addId(id, property);
						ObjectStack propertyStack = new ObjectStack(adapter, property.getClass(), property, rep.getDelayedInsertionBuffer());
						nameStack(rep.getAsName() + "." + propertyName, propertyStack);
						Class<?> propertyClass = rep.getReturnType(x);

						ObjectRepresentation propertyRep = propertyStack.getRepresentation(propertyClass);

						conditional.append(" = ");
						conditional.append(propertyRep.getAsName());
						conditional.append(".");
						conditional.append(Defaults.ID_COL);
						sp.addConditionalStatement(conditional.toString());
						// is this query using strict inheritance?
						// is the property and the property class different?
						if (!propertyClass.equals(property.getClass()) 
								&& (sel.isStrictInheritance() 
								|| (!propertyClass.isInterface() && !Modifier.isAbstract(propertyClass.getModifiers()))))
						{
							// Then add linking statement
							addLinkStatement(sp, propertyStack, propertyClass);
						}
						// create a new selection on the property
						Selector nuSel = sel.duplicate(property, propertyClass);
						// generate the conditional
						generateClause(propertyStack, nuSel, sp, sorted);
					}
				}
			}
		}
	}

	/**
	 * Add statements linking all properties in the stack up to and including
	 * propertyClass.
	 * 
	 * @param sp
	 * @param propertyStack
	 * @param propertyClass
	 */
	private void addLinkStatement(StatementPrototype sp, ObjectStack propertyStack, Class<?> propertyClass)
	{
		Node objRep = propertyStack.getNode(propertyClass);
		for(ObjectRepresentation rep:propertyStack.getAllRepresentations())
		{
			if(!rep.equals(objRep.getRepresentation()) 
					&& propertyClass.isAssignableFrom(rep.getRepresentedClass())
					&& rep.belongsInJoin())
			{
				sp.getIdStatementGenerator().addLinkStatement(objRep.getRepresentation(),rep);
			}
		}
		
		if (objRep.getRepresentation().isArray())
		{
			sp.getIdStatementGenerator().addPropertyTableToJoin(NameGenerator.getArrayTablename(adapter), objRep.getRepresentation().getAsName());
		}
		else
		{
			sp.getIdStatementGenerator().addPropertyTableToJoin(objRep.getRepresentation().getTableName(), objRep.getRepresentation().getAsName());
		}
	}

	/**
	 * Name the stack associated with a given property.
	 * 
	 * @param propertyName
	 *            the name of the property.
	 * @param stack
	 */
	private void nameStack(String propertyName, ObjectStack stack)
	{

		UniqueIdTree propertyTree = parameterTypeIds.get(propertyName);
		if (propertyTree == null)
		{
			// if this property has not been part of the query previously, add a
			// new propertyTree for it.
			propertyTree = new UniqueIdTree(uidGenerator);
			parameterTypeIds.put(propertyName, propertyTree);
		}
		propertyTree.nameStack(stack);
	}

	/**
	 * Check if this class is a class that requires sorting (almost all classes,
	 * even though most do not implement it). Classes that require and implement
	 * sorting are TreeMap, LinkedHashMap, Vector, ArrayList, etc.
	 * 
	 * If c is not a collection-type class or an array, return null.
	 * 
	 * @param c
	 * @return
	 */
	Boolean isSorted(Class<?> c)
	{
		Boolean res = null;
		if (c.isArray())
		{
			res = true;
		}
		else
		{
			// check if c is a Map
			if (Map.class.isAssignableFrom(c))
			{
				res = false;
				// sorted maps are sorted
				if (SortedMap.class.isAssignableFrom(c))
				{
					res = true;
				}
				// check if c is one of HashMap's sorted subclasses.
				if (LinkedHashMap.class.isAssignableFrom(c))
				{
					res = true;
				}
				if (EnumMap.class.isAssignableFrom(c))
				{
					res = true;
				}
			}
			else if (Collection.class.isAssignableFrom(c))
			{
				// by default, collections are not sorted
				res = false;
				// check if the c is a Set
				if (Set.class.isAssignableFrom(c))
				{
					// check if c implements any of Sets sorted sub-interfaces
					if (SortedSet.class.isAssignableFrom(c))
					{
						res = true;
					}
					// check if c is one of HashSet's sorted implementations
					if (LinkedHashSet.class.isAssignableFrom(c))
					{
						res = true;
					}
				}
				// check if c is a Queue
				if (Queue.class.isAssignableFrom(c))
				{
					res = true;
				}

				// check if c is a List
				if (List.class.isAssignableFrom(c))
				{
					res = true;
				}

			}
		}
		return res;
	}

	/**
	 * Returns true if c is a class that requires a decision on
	 * sorting/non-sorting.
	 * 
	 * @param c
	 * @return
	 */
	Boolean isCollectionsObject(Class<?> c)
	{

		Boolean res = false;
		if (c.isArray())
		{
			res = true;
		}
		else
		{
			// check if c is a Map
			if (Map.class.isAssignableFrom(c))
			{
				res = true;
			}
			else if (Collection.class.isAssignableFrom(c))
			{
				res = true;
			}
		}
		return res;
	}

	/**
	 * @param sel
	 *            the selector used for the query.
	 * @param sp
	 *            the statement prototype to co-generate
	 * @param rep
	 *            the representation of the array object
	 * @param sorted
	 *            true if outcome depends on ordering of elements.
	 * @throws SQLException
	 */
	private void generateArrayQuery(Selector sel, StatementPrototype sp, ObjectStack oStack, Boolean sorted) throws SQLException
	{
		ObjectRepresentation rep = oStack.getActualRepresentation();
		sp.getIdStatementGenerator().addPropertyTableToJoin(NameGenerator.getArrayTablename(adapter), rep.getAsName());
		// iterate over all non-null array entries
		// satisfy all the non-null members of the array
		int length = Array.getLength(sel.getSelectionObject());
		// first, count the number of non-null entries
		int numEntries = 0;
		for (int x = 0; x < length; x++)
		{
			Object o = Array.get(sel.getSelectionObject(), x);
			if (o != null)
			{
				numEntries++;
			}
		}
		// only add relation statements if the array has entries
		if (numEntries > 0)
		{
			// add array relation
			for (int x = 0; x < length; x++)
			{
				Object o = Array.get(sel.getSelectionObject(), x);
				if (o != null)
				{
					Class<?> propertyClass = sel.getSelectionObject().getClass().getComponentType();
					ArrayList<Object> conditionalValues = new ArrayList<Object>();
					StringBuilder sb = new StringBuilder(rep.getAsName());
					sb.append(".");
					sb.append(Defaults.ID_COL);
					sb.append(" = ");
					// add an as statement for the relation
					String relationTable = rep.getTableName();
					String relationAsName = this.uidGenerator.next();
					sp.getIdStatementGenerator().addPropertyTableToJoin(relationTable, relationAsName);

					sb.append(relationAsName);
					sb.append(".");
					sb.append(Defaults.ARRAY_MEMBER_ID);
					sb.append(" AND ");

					if (sorted)
					{
						// position is only valid for queries with sorted types
						sb.append(relationAsName);
						sb.append(".");
						sb.append(Defaults.ARRAY_POSITION);
						sb.append(" = ? AND ");
						conditionalValues.add(Integer.valueOf(x));

					}
					sb.append(relationAsName);
					sb.append(".");
					sb.append(Defaults.VALUE_COL);
					sb.append(" ");
					if (ObjectTools.isDatabasePrimitive(o.getClass()) && ObjectTools.isDatabasePrimitive(propertyClass))
					{
						sb.append(sel.getRelationalRepresentation());
						if (sel.takesPlaceholder())
						{
							sb.append("?");
							conditionalValues.add(o);
						}
						// put the stuff in the query
						sp.addConditionalStatement(sb.toString());
						sp.addConditionalValues(conditionalValues);
					}
					else
					{
						// check if we have queried this property before
						Long id = (long) System.identityHashCode(o);
						if (rep.getDelayedInsertionBuffer().isKnown(id, o))
						{
							break;
						}
						rep.getDelayedInsertionBuffer().addId(id, o);
						// name the property
						ObjectStack propertyStack = new ObjectStack(adapter, o.getClass(), o, rep.getDelayedInsertionBuffer());
						nameStack(relationAsName + "." + Defaults.VALUE_COL, propertyStack);
						ObjectRepresentation propertyRep = propertyStack.getRepresentation(propertyClass);
						sb.append(" = ");
						sb.append(propertyRep.getAsName());
						sb.append(".");
						sb.append(Defaults.ID_COL);
						// save to query
						sp.addConditionalStatement(sb.toString());
						sp.addConditionalValues(conditionalValues);
						// is the property and the property class different?
						if (!propertyClass.equals(o.getClass()))
						{
							propertyRep.setForceInclude(true);
							propertyStack.getActualRepresentation().setForceInclude(true);
							ObjectRepresentation objectRep = propertyStack.getRepresentation(Object.class);
							if(objectRep != null)
							{
								objectRep.setForceInclude(true);
							}
							// Then add linking statement
							addLinkStatement(sp, propertyStack, propertyClass);
						}
						// recursively generate more of the query
						Selector nuSel = sel.duplicate(o, propertyClass);
						// generate the conditional
						generateClause(propertyStack, nuSel, sp, sorted);

					}
				}
			}
		}
	}

	/**
	 * @return the typeStack
	 */
	public ObjectStack getTypeStack()
	{
		return typeStack;
	}

	public void setClauses(Clause... clause)
	{
		this.clauses = clause;
	}

}
