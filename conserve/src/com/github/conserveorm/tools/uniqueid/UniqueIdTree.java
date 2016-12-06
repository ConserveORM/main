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
package com.github.conserveorm.tools.uniqueid;

import java.util.ArrayList;
import java.util.List;

import com.github.conserveorm.tools.metadata.ObjectRepresentation;
import com.github.conserveorm.tools.metadata.ObjectStack;

/**
 * Maintains a list of ObjectStacks for a given query position.
 * 
 * @author Erik Berglund
 * 
 */
public class UniqueIdTree
{
	private List<ObjectStack> namedStacks;
	private UniqueIdGenerator generator;

	public UniqueIdTree(UniqueIdGenerator generator)
	{
		this.generator = generator;
		namedStacks = new ArrayList<ObjectStack>();
	}

	/**
	 * Assign names to all objects in the stack.
	 * 
	 * @param stack
	 */
	public void nameStack(ObjectStack stack)
	{
		// forbid the existing table names to be used as aliases
		for (String tableName : stack.getAllTableNames())
		{
			generator.addForbiddenString(tableName);
		}
		// find the existing stack that most closely match the new stack
		// as defined by the number of nodes that are common in both stacks
		ObjectStack closestStack = null;
		// the match level is the number of matching nodes
		int maxLevel = -1;
		for (ObjectStack candidate : namedStacks)
		{

			int matchCount = 0;
			for (ObjectRepresentation rep : stack.getAllRepresentations())
			{
				if (candidate.containsClass(rep.getRepresentedClass()))
				{
					matchCount++;
				}
			}

			if (matchCount > maxLevel)
			{
				//this is the candidate with the largest number of common nodes
				maxLevel = matchCount;
				closestStack = candidate;
			}
		}
		if (closestStack != null)
		{
			// copy names from existing to new stack
			for(ObjectRepresentation rep:stack.getAllRepresentations())
			{
				ObjectRepresentation other = closestStack.getRepresentation(rep.getRepresentedClass());
				if(other != null && other.getAsName()!=null)
				{
					rep.setAsName(other.getAsName());
				}
			}
		}
		//generate new names
		for(ObjectRepresentation rep:stack.getAllRepresentations())
		{
			if(rep.getAsName()==null)
			{
				rep.setAsName(generator.next());
			}
		}
		if (maxLevel < (stack.getSize() - 1))
		{
			// add new stack to list
			namedStacks.add(stack);
		}
	}
}
