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

/**
 * Describes the changes in the object model where one class has been moved from
 * one superclass to another. It also contains a list of fields that have been
 * moved.
 * 
 * @author Erik Berglund
 *
 */
public class InheritanceChangeDescription
{
	private List<ClassChangeList> removedSuperClasses = new ArrayList<>();
	private List<ClassChangeList> addedSuperClasses = new ArrayList<>();
	private List<FieldChangeDescription> movedFields = new ArrayList<>();

	/**
	 * Get change descriptions for all fields that have been moved from an old
	 * superclass to a new one. Never returns null - if there are no changed
	 * fields an empty list is returned.
	 */
	public List<FieldChangeDescription> getMovedFields()
	{
		return this.movedFields;
	}

	/**
	 * Get a list of former superclasses.
	 * 
	 * @return the removed superclasses
	 */
	public List<ClassChangeList> getRemovedSuperClasses()
	{
		return removedSuperClasses;
	}

	/**
	 * Get a list of new superclasses.
	 * 
	 * @return the added superclasses 
	 */
	public List<ClassChangeList> getAddedSuperClasses()
	{
		return addedSuperClasses;
	}

	/**
	 * @param fcDesc
	 */
	public void addMovedField(FieldChangeDescription fcDesc)
	{
		this.movedFields.add(fcDesc);
	}

	/**
	 * True if the inheritance model has changed so much that checking for
	 * changes in the actual object is pointless.
	 * 
	 */
	public boolean inheritanceModelChanged()
	{
		if (size(removedSuperClasses) > 0 || size(addedSuperClasses) > 0)
		{
			return true;
		}
		return false;
	}

	/**
	 * Get the size of a list of lists
	 * 
	 * @param listoflists
	 */
	private int size(List<ClassChangeList> listoflists)
	{
		int res = 0;
		for (ClassChangeList list : listoflists)
		{
			res += list.size();
		}
		return res;
	}
}
