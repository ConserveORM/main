/*******************************************************************************
 *  
 * Copyright (c) 2009, 2017 Erik Berglund.
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
package com.github.conserveorm.tools;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.github.conserveorm.adapter.AdapterBase;
import com.github.conserveorm.connection.ConnectionWrapper;
import com.github.conserveorm.select.Clause;
import com.github.conserveorm.tools.generators.IdStatementGenerator;
import com.github.conserveorm.tools.generators.JoinDescriptor;
import com.github.conserveorm.tools.metadata.ObjectRepresentation;
import com.github.conserveorm.tools.metadata.ObjectStack;

/**
 * A prototype to a PreparedStatement, that can convert itself to a
 * PreparedStatement.
 * 
 * @author Erik Berglund
 * 
 */
public class StatementPrototype
{
	private AdapterBase adapter;
	private String prePend = "";
	private String append = "";
	private Long offset;
	private Long limit;

	private List<String> sortStatements = new ArrayList<String>();
	private List<Object> conditionalValues = new ArrayList<Object>();
	private StatementContainer statementStackPointer;
	private StatementContainer statementStack;

	private IdStatementGenerator idGen;

	public StatementPrototype(AdapterBase adapter, ObjectStack oStack, Class<?> resultClass, Clause[]clauses, boolean addJoins)
	{
		this.adapter = adapter;

		idGen = new IdStatementGenerator(adapter,oStack,clauses,addJoins);
		// initialise the statement stack
		push("AND");
		statementStack = statementStackPointer;
	}




	public PreparedStatement toPreparedStatement(ConnectionWrapper cw, String prePend) throws SQLException
	{
		this.setPrepend(prePend);
		return this.toPreparedStatement(cw);
	}

	private PreparedStatement toPreparedStatement(ConnectionWrapper cw) throws SQLException
	{
		PreparedStatement ps = cw.prepareStatement(createString());
		// set the values
		int index = 0;
		for (Object o : conditionalValues)
		{
			index++;
			if(o.getClass().isEnum())
			{
				addValue(ps,index,((Enum<?>)o).name());
			}
			else if(o.getClass().equals(Class.class))
			{
				addValue(ps,index,((Class<?>)o).getName());
			}
			else if (ObjectTools.isDatabasePrimitive(o.getClass()))
			{
				addValue(ps, index, o);
			}
		}
		return ps;
	}

	/**
	 * Add an object to the PreparedStatement. Which of the setXXX objects are
	 * called depends on the type of o.
	 * 
	 * @param ps
	 * @param index
	 * @param o
	 * @throws SQLException
	 */
	private void addValue(PreparedStatement ps, int index, Object o) throws SQLException
	{
		Tools.setParameter(ps, o.getClass(), index,o,adapter);
	}

	/**
	 * Get a string representation of the query.
	 */
	private String createString()
	{

		// create the statement
		StringBuilder sb = new StringBuilder(prePend);
		String idStatement = idGen.generate();
		sb.append(idGen.generateAsStatement());
		conditionalValues.addAll(0, idGen.getValues());

		boolean whereAdded = false;

		if (idStatement.length() > 0)
		{
			whereAdded = true;
			sb.append(" WHERE ");
			// add the statement that joins the inheritance levels together
			sb.append(idStatement);
		}
		if (statementStack.getSize() > 0)
		{
			if (!whereAdded)
			{
				whereAdded = true;
				sb.append(" WHERE ");
			}
			else
			{
				sb.append(" AND ");
			}
			// depth-first traversal of the statement tree
			StatementContainer pointer = this.statementStack;
			addStatements(sb, pointer);

		}
		if (this.sortStatements.size() > 0)
		{
			sb.append(" ORDER BY ");
			for (int x = 0; x < sortStatements.size(); x++)
			{
				sb.append(sortStatements.get(x));
				if (x < sortStatements.size() - 1)
				{
					sb.append(",");
				}
			}
		}
		if (adapter.isSupportsLimitOffsetKeywords() && (limit != null || offset != null))
		{
			StringBuilder limitOffsetBuffer = new StringBuilder();
			StringBuilder limitBuffer = new StringBuilder(" ");
			if (limit != null)
			{
				String limitString = adapter.getLimitString();
				limitString = limitString.replaceAll("\\?", limit.toString());
				limitBuffer.append(limitString);
			}
			StringBuilder offsetBuffer = new StringBuilder(" ");
			if (offset != null)
			{
				String offsetString = adapter.getOffsetString();
				offsetString = offsetString.replaceAll("\\?", offset.toString());
				offsetBuffer.append(offsetString);
			}
			if (adapter.isPutLimitBeforeOffset())
			{
				limitOffsetBuffer.append(limitBuffer);
				limitOffsetBuffer.append(offsetBuffer);
			}
			else
			{
				limitOffsetBuffer.append(offsetBuffer);
				limitOffsetBuffer.append(limitBuffer);
			}
			if (adapter.isPutLimitOffsetBeforeColumns())
			{
				// insert the limit/offset string after "select"
				sb.insert(6, limitOffsetBuffer);
			}
			else
			{
				sb.append(limitOffsetBuffer);
			}
		}
		sb.append(append);
		String queryString = sb.toString();
		return queryString;
	}
	
	/**
	 * Set a string that will be added to the end of the statement.
	 * It is the caller's responsibility to ensure the toAppend value starts with an appropriate spacing character.
	 * 
	 * @param toAppend
	 */
	public void setAppend(String toAppend)
	{
		this.append = toAppend;
	}

	private void addStatements(StringBuilder sb, StatementContainer container)
	{

		for (int x = 0; x < container.getSize(); x++)
		{
			StatementContainer statement = container.get(x);
			if (statement.isSql())
			{
				sb.append(statement.getKeyWord());
			}
			else
			{
				sb.append("(");
				addStatements(sb, statement);
				sb.append(")");
			}
			if (x < container.getSize() - 1)
			{
				sb.append(" ");
				sb.append(container.getKeyWord());
				sb.append(" ");
			}
		}
	}

	public void addEqualsClause(String col, Object value)
	{
		col += " = ?";
		this.addConditionalStatement(col, value);
	}


	/**
	 * Set a string that will be prepended to the start of the SQL statement.
	 * 
	 * @param string
	 */
	public void setPrepend(String string)
	{
		this.prePend = string;
	}

	public void setOffset(Long offset)
	{
		this.offset = offset;
	}

	public void setLimit(Long limit)
	{
		this.limit = limit;
	}

	public Long getLimit()
	{
		return limit;
	}

	public Long getOffset()
	{
		return offset;
	}

	/**
	 * Add a statement indicating the sort order of the results.
	 * 
	 * @param subStatement
	 */
	public void addSortStatement(String subStatement)
	{
		sortStatements.add(subStatement);
	}

	public void addConditionalStatement(String conditional, Object value)
	{
		addConditionalStatement(conditional);
		this.conditionalValues.add(value);
	}

	public void addConditionalValues(ArrayList<Object> values)
	{
		this.conditionalValues.addAll(values);
	}

	/**
	 * Generate the start of the SELECT * FROM type query. * will be replaced
	 * with A.*, B.* or whatever is appropriate from the join tables.
	 * 
	 * @return a String representing the the start of a selection query.
	 */
	public String getSelectStartQuery()
	{
		String firstAsName = idGen.getJoinRepresentations().get(0).getAsName();
		StringBuilder statement = new StringBuilder(200);
		statement.append("SELECT ");
		if (adapter.handlesDistinctWithClobsAndBlobsCorrectly())
		{
			statement.append("DISTINCT(");
		}
		statement.append(firstAsName);
		statement.append(".");
		statement.append(Defaults.ID_COL);
		if (adapter.handlesDistinctWithClobsAndBlobsCorrectly())
		{
			statement.append(")");
		}
		if (!idGen.getJoinRepresentations().get(0).isArray())
		{
			statement.append(",");
			statement.append(firstAsName);
			statement.append(".");
			statement.append(Defaults.REAL_CLASS_COL);
		}
		for (int x = 0; x < idGen.getJoinTables().size(); x++)
		{
			ObjectRepresentation rep = idGen.getJoinRepresentations().get(x);
			if (rep.getPropertyCount() > 0)
			{
				statement.append(",");
			}
			for (int prop = 0; prop < rep.getPropertyCount(); prop++)
			{
				statement.append(idGen.getJoinTableIds().get(x));
				statement.append(".");
				statement.append(rep.getPropertyName(prop));
				if (prop < rep.getPropertyCount() - 1)
				{
					statement.append(",");
				}
			}
		}
		statement.append(" FROM ");
		return statement.toString();
	}

	public void addConditionalStatement(String conditional)
	{
		this.statementStackPointer.add(new StatementContainer(conditional, true));
	}

	/**
	 * Push a new statement container on the stack.
	 * 
	 * @param keyWord
	 */
	public void push(String keyWord)
	{
		StatementContainer sc = new StatementContainer(keyWord);
		if (statementStackPointer != null)
		{
			statementStackPointer.add(sc);
		}
		statementStackPointer = sc;
	}

	/**
	 * Pop the top statement container of the stack.
	 */
	public void pop()
	{
		StatementContainer oldPointer = statementStackPointer;
		statementStackPointer = statementStackPointer.getParent();
		if (!oldPointer.isSql() && oldPointer.getSize() == 0)
		{
			statementStackPointer.remove(oldPointer);
		}
	}

	/**
	 * @return the generator responsible for id statements in this object.
	 */
	public IdStatementGenerator getIdStatementGenerator()
	{
		return this.idGen;
	}




	/** 
	 * Add a left join where the left part is defined by the tablen name and a shortname,
	 * and the right part is defined by a tablename, and their relationship is defined in the string onStatement.
	 * 
	 * @param leftTableName
	 * @param leftShortName
	 * @param rightTableName
	 * @param onStatement
	 */
	public void addLeftJoin(String leftTableName, String leftShortName, String rightTableName, String onStatement, Object ... values)
	{
		idGen.addLeftJoin(new JoinDescriptor(leftTableName, leftShortName, rightTableName, onStatement,values));
	}

}
