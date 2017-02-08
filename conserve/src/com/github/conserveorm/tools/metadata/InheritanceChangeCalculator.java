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
package com.github.conserveorm.tools.metadata;

import java.util.List;

import com.github.conserveorm.tools.metadata.ObjectStack.Node;

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

		findDeleted(res, fromStack.getActual());
		findAdded(res, toStack.getActual());

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

	private void findAdded(InheritanceChangeDescription res, Node actual)
	{
		List<ClassChangeList> addedList = res.getAddedSuperClasses();
		findChanged(addedList, actual, fromStack);
	}

	private void findDeleted(InheritanceChangeDescription res, Node actual)
	{
		List<ClassChangeList> deletedList = res.getRemovedSuperClasses();
		findChanged(deletedList, actual, toStack);
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
	private void findChanged(List<ClassChangeList> changedList, Node actual, ObjectStack otherStack)
	{
		// list all super-classes of actual
		List<Node> supers = actual.getSupers();
		for (Node s : supers)
		{
			//check if the two nodes are directly connected
			if (!otherStack.hasSuper(actual, s))
			{
				ClassChangeList deletedNodes = new ClassChangeList();
				deletedNodes.addNode(actual);
				deletedNodes.addNode(s);
				recursiveFindChanged(changedList, s, otherStack, deletedNodes);
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
	private void recursiveFindChanged(List<ClassChangeList> changedList, Node sub, ObjectStack otherStack, ClassChangeList deletedNodes)
	{
		List<Node> supers = sub.getSupers();
		if (supers.size() == 0)
		{
			// no superclasses, this is the end of a chain of deleted
			// interfaces or superclasses
			ClassChangeList realDeleted = new ClassChangeList();
			realDeleted.addAll(deletedNodes);
			if(otherStack.contains(sub))
			{
				realDeleted.setShared(true);
				//set the name of the class just below the shared class in the other stack
				Node otherSub = otherStack.getSubNode(sub);
				realDeleted.setSharedSub(otherSub);
			}
			changedList.add(realDeleted);
		}
		else
		{
			for (Node s : supers)
			{
				ClassChangeList tmp = new ClassChangeList();
				tmp.addAll(deletedNodes);
				if(!otherStack.hasSuper(sub, s))
				{
					// more deleted nodes, keep on recursing
					tmp.addNode(s);
					recursiveFindChanged(changedList, s, otherStack, tmp);
				}
				else
				{
					// the chain has re-merged with the new tree, stop recursing
					tmp.setShared(true);
					//set the name of the class just below the shared class in the other stack
					Node otherSub = otherStack.getSubNode(sub);
					tmp.setSharedSub(otherSub);
					changedList.add(tmp);
				}
			}
		}

	}

}
