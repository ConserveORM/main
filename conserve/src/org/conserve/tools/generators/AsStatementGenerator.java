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
import java.util.List;
import org.conserve.adapter.AdapterBase;

/**
 * Generates statements on the form "SOMETABLE AS A, SOMEOTHERTABLE AS B,[...]"
 * 
 * @author Erik Berglund
 *
 */
public class AsStatementGenerator
{

	private AdapterBase adapter;
	private List<JoinDescriptor> joins = new ArrayList<>();

	/**
	 * @param adapter
	 */
	public AsStatementGenerator(AdapterBase adapter)
	{
		this.adapter = adapter;
	}

	public void addJoin(JoinDescriptor join)
	{
		joins.add(join);
	}
	
	private JoinDescriptor getJoinDescriptorForTable(String table, String shortName)
	{
		JoinDescriptor res = null;
		for(JoinDescriptor join:joins)
		{
			if(join.leftMatches(table, shortName))
			{
				res = join;
				break;
			}
		}
		return res;
	}

	/**
	 * @param joinTableIds
	 * @param joinTables
	 * @param relationDescriptors
	 * @return
	 */
	public String generate(List<String> joinTables, List<String> joinTableIds)
	{
		StringBuilder sb = new StringBuilder();
		List<String> added = new ArrayList<String>();

		for (int x = 0; x < joinTables.size(); x++)
		{
			if (!added.contains(joinTableIds.get(x)))
			{
				added.add(joinTableIds.get(x));
				if (sb.length() > 0)
				{
					sb.append(" ");
					sb.append(adapter.getJoinKeyword());
					sb.append(" ");
				}
				JoinDescriptor jd = getJoinDescriptorForTable(joinTables.get(x), joinTableIds.get(x));
				if (jd != null)
				{
					sb.append(jd.toString());
				}
				else
				{
					sb.append(joinTables.get(x));
					sb.append(" AS ");
					sb.append(joinTableIds.get(x));
				}
			}
		}
		return sb.toString();
	}

	/**
	 * @return
	 */
	public List<Object> getValues()
	{
		List<Object>res = new ArrayList<>();
		for(JoinDescriptor join:joins)
		{
			Object [] obj = join.getValues();
			if(obj!=null)
			{
				for(Object o:obj)
				{
					res.add(o);
				}
			}
		}
		return res;
	}
}
