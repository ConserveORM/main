/*******************************************************************************
 *  
 * Copyright (c) 2009, 2018 Erik Berglund.
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
package com.github.conserveorm.select;


/**
 * Where clause that combines two subclauses, either of which must be satisfied to satisfy the rule.
 * 
 * @author Erik Berglund
 *
 */
public class Or extends ConditionalClause
{
	/**
	 * Generate a where clause that is satisfied if either or both of its arguments are satisfied.
	 * @param one one of the clauses that may be satisfied to satisfy this clause.
	 * @param two the other clause that may be satisfied to satisfy this clause.
	 * @param clauses the rest of the clauses that may be satisfied to satisfy the clause.
	 */
	public Or(ConditionalClause one, ConditionalClause two, ConditionalClause...clauses)
	{
		ConditionalClause  [] params = new ConditionalClause[2+clauses.length];
		params[0]=one;
		params[1]=two;
		System.arraycopy(clauses,0,params,2,clauses.length);
		setSubClauses(params);
	}


	@Override
	public String getKeyWord()
	{
		return "OR";
	}
}
