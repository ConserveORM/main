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
package org.conserve.tools.uniqueid;

import java.util.ArrayList;

import org.conserve.tools.ObjectRepresentation;
import org.conserve.tools.ObjectStack;

/**
 * Maintains a list of ObjectStacks for a given query position.
 * 
 * @author Erik Berglund
 * 
 */
public class UniqueIdTree
{
	private ArrayList<ObjectStack> namedStacks;
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
		//forbid the existing table names to be used as aliases
		for(int lvl = 0;lvl<stack.getSize();lvl++)
		{
			generator.addForbiddenString(stack.getRepresentation(lvl).getTableName());
		}
		// find the existing stack that most closely match the new stack
		ObjectStack closestStack = null;
		int maxHeight = -1;
		for (ObjectStack existing : namedStacks)
		{
			int maxLevel = Math.min(stack.getSize(), existing.getSize());
			for (int x = 0; x < maxLevel; x++)
			{
				ObjectRepresentation o1 = stack.getRepresentation(x);
				ObjectRepresentation o2 = existing.getRepresentation(x);
				if (!o1.getRepresentedClass().equals(o2.getRepresentedClass()))
				{
					break;
				}
				else
				{
					if (x > maxHeight)
					{
						maxHeight = x;
						closestStack = existing;
					}
				}
			}
		}
		// copy names from existing to new stack
		for (int x = 0; x <= maxHeight; x++)
		{
			stack.getRepresentation(x).setAsName(
					closestStack.getRepresentation(x).getAsName());
		}
		// fill in remaining names from generator
		for (int x = maxHeight + 1; x < stack.getSize(); x++)
		{
			stack.getRepresentation(x).setAsName(generator.next());
		}
		if (maxHeight < (stack.getSize() - 1))
		{
			// add new stack to list
			namedStacks.add(stack);
		}
	}
}
