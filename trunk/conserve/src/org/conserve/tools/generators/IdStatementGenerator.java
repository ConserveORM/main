package org.conserve.tools.generators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.conserve.adapter.AdapterBase;
import org.conserve.tools.Defaults;
import org.conserve.tools.ObjectRepresentation;
import org.conserve.tools.ObjectStack;
import org.conserve.tools.ObjectTools;

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
	public IdStatementGenerator(AdapterBase adapter, ObjectStack oStack, boolean addJoins)
	{
		this.addJoins = addJoins;
		this.asGenerator = new AsStatementGenerator(adapter);
		this.addTablesToJoin(oStack);
		// add join tables
		if (addJoins)
		{
			for (int x = oStack.getSize() - 1; x > 0; x--)
			{
				ObjectRepresentation rep = oStack.getRepresentation(x);
				addLinkStatement(oStack.getRepresentation(x - 1), rep);
			}
		}
	}

	/**
	 * @param oStack
	 * @param minLevel
	 * @return
	 */
	public String generate(int minLevel)
	{
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
				tmp.append(rdesc.toString());
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
		return asGenerator.generate(tables, ids, relationDescriptors, omitTables);
	}

	public void addPropertyTableToJoin(String tableName, String asName)
	{
		if (!joinTableIds.contains(asName) && !joinPropertyTableIds.contains(asName))
		{
			joinPropertyTables.add(tableName);
			joinPropertyTableIds.add(asName);
		}
	}

	public void addTableToJoin(String tableName, String asName)
	{
		if (!joinTableIds.contains(asName) && !joinPropertyTableIds.contains(asName))
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

	public void addLinkStatement(ObjectRepresentation superRep, ObjectRepresentation objRep)
	{
		// check if the representations have already been joined
		if (!isJoined(superRep.getAsName(), objRep.getAsName()))
		{
			// if not, add a statement to join them
			FieldDescriptor from = new FieldDescriptor(objRep.getTableName(), objRep.getAsName(), Defaults.ID_COL);
			FieldDescriptor to = new FieldDescriptor(superRep.getTableName(), superRep.getAsName(),
					Defaults.REAL_ID_COL);
			RelationDescriptor relDesc = new RelationDescriptor(from, to);
			this.addRelationDescriptor(relDesc);

			from = new FieldDescriptor(superRep.getTableName(), superRep.getAsName(), Defaults.REAL_CLASS_COL);
			if (objRep.isArray())
			{
				// arrays have a different tablename
				addRelationDescriptor(new RelationDescriptor(from, Defaults.ARRAY_TABLE_NAME));
			}
			else
			{
				addRelationDescriptor(new RelationDescriptor(from, ObjectTools.getSystemicName(objRep
						.getRepresentedClass())));
			}
			// indicate that the representations are linked
			setJoined(superRep.getAsName(), objRep.getAsName());
		}
	}

	/**
	 * Add all tables in propertyStack, up to and including the table for
	 * propertyClass
	 * 
	 * @param propertyStack
	 * @param propertyClass
	 *            may be null.
	 */
	public void addPropertyTablesToJoin(ObjectStack propertyStack, Class<?> propertyClass)
	{
		boolean propertiesFound = false;
		for (int x = propertyStack.getSize() - 1; x >= 0; x--)
		{
			ObjectRepresentation rep = propertyStack.getRepresentation(x);
			if (!propertiesFound)
			{
				if (rep.getNonNullPropertyCount() > 0 || rep.isArray())
				{
					propertiesFound = true;
				}
				else if (propertyClass != null && rep.getRepresentedClass().equals(propertyClass))
				{
					propertiesFound = true;
				}
			}
			if (propertiesFound)
			{
				if (rep.isArray())
				{
					addPropertyTableToJoin(Defaults.ARRAY_TABLE_NAME, rep.getAsName());
				}
				else
				{
					addPropertyTableToJoin(rep.getTableName(), rep.getAsName());
				}
			}
			if (propertyClass != null && rep.getRepresentedClass().equals(propertyClass))
			{
				// end once we reach the desired class
				break;
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
		for (int x = stack.getSize() - 1; x >= 0; x--)
		{
			ObjectRepresentation rep = stack.getRepresentation(x);
			this.joinRepresentations.add(rep);
			if (rep.isArray())
			{
				addTableToJoin(Defaults.ARRAY_TABLE_NAME, rep.getAsName());
			}
			else
			{
				addTableToJoin(rep.getTableName(), rep.getAsName());
			}
			if (!addJoins)
			{
				break;
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
