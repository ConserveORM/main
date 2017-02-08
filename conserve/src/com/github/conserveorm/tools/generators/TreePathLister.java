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
package com.github.conserveorm.tools.generators;

import java.util.ArrayList;
import java.util.List;

import com.github.conserveorm.tools.metadata.ObjectRepresentation;
import com.github.conserveorm.tools.metadata.ObjectStack;
import com.github.conserveorm.tools.metadata.ObjectStack.Node;

/**
 * Lists all possible paths from the actual class of an ObjectStack to all
 * super-classes and interfaces.
 * 
 * @author Erik Berglund
 * 
 */
public class TreePathLister
{
	/**
	 * Generate a list of all possible paths from the root node (the actual
	 * implementing class) to leaf nodes (the superclasses and/or implemented
	 * interfaces with no superclass).
	 * 
	 * @param oStack
	 */
	public List<List<ObjectRepresentation>> generateLists(ObjectStack oStack)
	{
		List<List<ObjectRepresentation>> res = new ArrayList<>();
		// get the actual object
		Node actual = oStack.getActual();
		actual.setSaved(true);
		List<ObjectRepresentation> initialList = new ArrayList<>();
		initialList.add(actual.getRepresentation());
		res.add(initialList);
		recursiveGenerate(oStack, res, initialList, actual);

		return res;
	}

	/**
	 * @param oStack
	 * @param res
	 * @param initialList
	 * @param actual
	 */
	private void recursiveGenerate(ObjectStack oStack,
			List<List<ObjectRepresentation>> res,
			List<ObjectRepresentation> initialList, Node actual)
	{
		List<Node> supers = oStack.getSupers(actual);
		if (supers.size() > 0)
		{
			// if there are more than one superclass, duplicate the list and
			// recurse to each individual superclass
			for (int x = 1; x < supers.size(); x++)
			{
				Node superNode = supers.get(x);
				if (!superNode.isSaved())
				{
					superNode.setSaved(true);
					List<ObjectRepresentation> sideList = new ArrayList<>(
							initialList);
					res.add(sideList);
					sideList.add(superNode.getRepresentation());
					recursiveGenerate(oStack, res, sideList, superNode);
				}
			}
			// add the first superclass to the existing list
			Node superNode = supers.get(0);
			if (!superNode.isSaved())
			{
				superNode.setSaved(true);
				initialList.add(superNode.getRepresentation());
				recursiveGenerate(oStack, res, initialList, superNode);
			}
		}
	}

	/**
	 * Remove all parts of paths that are at the end of the path and does not
	 * contain any parameters. If a path contains no parameters, it will be
	 * removed.
	 * 
	 * @param allPaths
	 */
	public void prunePaths(List<List<ObjectRepresentation>> allPaths)
	{
		for (int x = 0; x < allPaths.size(); x++)
		{
			List<ObjectRepresentation> path = allPaths.get(x);
			// remove from end of path
			for (int t = path.size() - 1; t > 0; t--)
			{
				// stop if we find a concrete representation or a non-empty
				// interface
				if (path.get(t).getNonNullPropertyCount() > 0
						|| path.get(t).getRepresentedClass()
								.equals(Object.class))
				{
					break;
				}
				else
				{
					path.remove(t);
				}
			}
			if (path.size() <= 1 && allPaths.size() > 1)
			{
				allPaths.remove(path);
				x--;
			}
		}
	}

	/**
	 * Remove all references below (with a lower array index) the indicated
	 * selection class if they contain no properties.
	 * 
	 * @param allPaths
	 * @param selectionClass
	 */
	public void pruneInheritance(List<List<ObjectRepresentation>> allPaths,
			Class<?> selectionClass)
	{
		for (int x = 0; x < allPaths.size(); x++)
		{
			List<ObjectRepresentation> path = allPaths.get(x);
			// remove from start of path
			for (int t = 0; t < path.size(); t++)
			{
				// stop if we find a concrete representation or a non-empty
				// interface
				ObjectRepresentation rep = path.get(t);
				if (rep.getNonNullPropertyCount() > 0
						|| rep.getRepresentedClass().equals(selectionClass))
				{
					break;
				}
				else
				{
					path.remove(t);
					t--;
				}
			}
			if (path.size() == 0)
			{
				allPaths.remove(path);
				x--;
			}
		}
	}

}
