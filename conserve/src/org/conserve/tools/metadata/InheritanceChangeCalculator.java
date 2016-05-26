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
package org.conserve.tools.metadata;

import java.util.ArrayList;
import java.util.List;

import org.conserve.tools.metadata.ObjectStack.Node;

/**
 * Calculates the changes in inheritance model from one stack to the next.
 * 
 * The stacks must have one thing in common: Both stacks must have the same
 * actual class.
 * 
 * @author Erik Berglund
 *
 */
public class InheritanceChangeCalculator
{
	private ObjectStack fromStack;
	private ObjectStack toStack;

	public InheritanceChangeCalculator(ObjectStack fromStack, ObjectStack toStack)
	{
		this.fromStack = fromStack;
		this.toStack = toStack;
	}

	public InheritanceChangeDescription calculateDescription()
	{
		InheritanceChangeDescription res = new InheritanceChangeDescription();

		recursiveFindDeleted(res, fromStack.getActual());
		recursiveFindAdded(res, toStack.getActual());

		// check if any field has moved from one superclass to another
		List<String> fieldNames = fromStack.getAllPropertyNames();
		for (String name : fieldNames)
		{
			ObjectRepresentation oldRep = fromStack.getRepresentation(name);
			ObjectRepresentation nuRep = toStack.getRepresentation(name);
			if (nuRep != null && !oldRep.getTableName().equals(nuRep.getTableName()))
			{
				FieldChangeDescription fcDesc = new FieldChangeDescription();
				fcDesc.setFromName(name);
				fcDesc.setToName(name);
				fcDesc.setFromTable(oldRep.getTableName());
				fcDesc.setToTable(nuRep.getTableName());
				fcDesc.setToClass(nuRep.getReturnType(name));
				fcDesc.setFromClass(oldRep.getReturnType(name));

				ObjectStack os = new ObjectStack(toStack.getAdapter(), nuRep.getRepresentedClass());
				if (os.getRepresentation(name) == null)
				{
					fcDesc.setFromClass(null);
				}

				res.addMovedField(fcDesc);
			}
		}

		return res;

	}

	private void recursiveFindAdded(InheritanceChangeDescription res, Node actual)
	{
		List<List<Node>> addedList = res.getAddedSuperClasses();
		recursiveFindChanged(addedList, actual, fromStack);
	}

	private void recursiveFindDeleted(InheritanceChangeDescription res, Node actual)
	{
		List<List<Node>> deletedList = res.getRemovedSuperClasses();
		recursiveFindChanged(deletedList, actual, toStack);
	}

	/**
	 * Look for nodes that have are not in otherStack, store them in changeList
	 * 
	 * @param changedList
	 *            the list of inheritance changes
	 * @param actual
	 *            the class to start looking at
	 * @param otherStack
	 *            the stack to compare changes with.
	 */
	private void recursiveFindChanged(List<List<Node>> changedList, Node actual, ObjectStack otherStack)
	{
		// list all super-classes of actual
		List<Node> supers = actual.getSupers();
		for (Node s : supers)
		{
			List<Node> deletedNodes = new ArrayList<>();
			deletedNodes.add(actual);
			if (!otherStack.contains(s))
			{
				deletedNodes.add(s);
				recursiveFindChanged(changedList, s, otherStack, deletedNodes);
			}
			else
			{
				changedList.add(deletedNodes);
			}
		}
	}

	/**
	 * Continue looking for changed nodes among the super-nodes of already
	 * changed nodes.
	 * 
	 * 
	 * @param changedList
	 *            the list of resulting changes
	 * @param sub
	 * @param otherStack
	 *            the object stack to compare to
	 * @param deletedNodes
	 */
	private void recursiveFindChanged(List<List<Node>> changedList, Node sub, ObjectStack otherStack, List<Node> deletedNodes)
	{
		List<Node> supers = sub.getSupers();
		if (supers.size() == 0)
		{
			// no superclasses, this is the end of a chain of deleted
			// interfaces.
			List<Node> realDeleted = new ArrayList<>();
			realDeleted.addAll(deletedNodes);
			changedList.add(realDeleted);
		}
		else
		{
			for (Node s : supers)
			{
				// the chain has re-merged with the new tree
				List<Node> tmp = new ArrayList<>();
				tmp.addAll(deletedNodes);
				if (otherStack.contains(s))
				{
					changedList.add(tmp);
				}
				else
				{
					// more deleted nodes, keep on recursing
					tmp.add(s);
					recursiveFindChanged(changedList, s, otherStack, tmp);
				}
			}
		}

	}

}
