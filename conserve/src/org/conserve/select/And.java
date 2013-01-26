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
package org.conserve.select;

/**
 * @author Erik Berglund
 *
 */
public class And extends ConditionalClause
{

	/**
	 * And clauses are satisfied if all of their sub-clauses are satisfied.
	 * 
	 * @param one the fist clause that must be satisfied in order to satisfy this clause.
	 * @param two the second clause that must be satisfied in order to satisfy this clause.
	 * @param sel optional extra clauses that must be satisfied in order to satisfy this clause.
	 */
	public And(ConditionalClause one, ConditionalClause two, ConditionalClause... sel)
	{
		ConditionalClause  [] params = new ConditionalClause[2+sel.length];
		params[0]=one;
		params[1]=two;
		System.arraycopy(sel,0,params,2,sel.length);
		setSubClauses(params);
	}


	@Override
	public String getKeyWord()
	{
		return "AND";
	}

}
