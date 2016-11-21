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

/**
 * Describes a join statement between two tables.
 * 
 * 
 * @author Erik Berglund
 *
 */
public class JoinDescriptor
{
	private String leftTable;
	private String leftShortName;
	private String rightTable;
	private String onDescription;
	private Object [] values;
	
	public JoinDescriptor(String leftTable, String leftShortName,String rightTable, String onDescription, Object ... values)
	{
		this.leftTable = leftTable;
		this.leftShortName = leftShortName;
		this.rightTable = rightTable;
		this.onDescription = onDescription;
		this.values = values;
	}
	
	/**
	 * Check if this join description matches a given table and shortname.
	 * @param table
	 * @param shortName
	 * @return
	 */
	public boolean leftMatches(String table, String shortName)
	{
		return (leftTable.equalsIgnoreCase(table) && leftShortName.equalsIgnoreCase(shortName));
	}
	
	/**
	 * Get the values to be inserted in the statement in place of '?' markers.
	 * @return
	 */
	public Object [] getValues()
	{
		return values;
	}
	
	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return leftTable+" AS "+ leftShortName + " LEFT JOIN " + rightTable +" ON " + onDescription;
	}

}
