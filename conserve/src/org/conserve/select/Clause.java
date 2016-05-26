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
package org.conserve.select;


/**
 * @author Erik Berglund
 *
 */
public abstract class Clause
{
	private Clause [] subClauses;
	
	public Clause(Clause ... sel)
	{
		this.subClauses = sel;
	}
	

	protected void setSubClauses(ConditionalClause... params)
	{
		subClauses=params;
	}
	
	/**
	 * Get the components of this clause.
	 * @return an array of Where objects.
	 */
	public Clause[] getSubclauses()
	{
		return this.subClauses;
	}
	
	/**
	 * Return the SQL keyword describing to this relation.
	 * @return the SQL relational keyword.
	 */
	public String getKeyWord()
	{
		//this is the default implementation, return an empty string
		return "";
	}
	
	public void setQueryClass(Class<?>queryClass)
	{
		setQueryClassForSubClauses(queryClass);
	}
	
	protected void setQueryClassForSubClauses(Class<?>queryClass)
	{
		for(Clause clause:subClauses)
		{
			clause.setQueryClass(queryClass);
		}
	}


	/**
	 * @return
	 */
	public abstract Class<?> getQueryClass();

}
