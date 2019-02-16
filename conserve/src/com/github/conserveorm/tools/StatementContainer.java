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
package com.github.conserveorm.tools;

import java.util.ArrayList;

/**
 * Contains a list of String SQL statements and/or other StatementContainers.
 * 
 * @author Erik Berglund
 *
 */
public class StatementContainer
{

	private StatementContainer parent;
	private String keyWord;
	private boolean sql;
	private ArrayList<StatementContainer>statements = new ArrayList<StatementContainer>();

	/**
	 * 
	 * @param keyWord the keyword used to combine statements in this container.
	 */
	public StatementContainer(String keyWord,boolean isSql)
	{
		this.keyWord = keyWord;
		this.sql = isSql;
	}
	/**
	 * 
	 * @param keyWord the keyword used to combine statements in this container.
	 */
	public StatementContainer(String keyWord)
	{
		this(keyWord,false);
	}

	/**
	 * Return true if this is an SQL statement, false if it is a container for SQL statements.
	 * @return the sql
	 */
	public boolean isSql()
	{
		return sql;
	}
	
	/**
	 * @return the parent
	 */
	public StatementContainer getParent()
	{
		return parent;
	}

	/**
	 * @return the keyWord
	 */
	public String getKeyWord()
	{
		return keyWord;
	}
	
	

	public void add(StatementContainer sc)
	{
		statements.add(sc);
		sc.parent = this;
	}
	
	public void remove(StatementContainer toRemove)
	{
		statements.remove(toRemove);
	}
	
	public StatementContainer get(int index)
	{
		return statements.get(index);
	}
	
	public int getSize()
	{
		return statements.size();
	}
}
