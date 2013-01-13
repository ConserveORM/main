/*******************************************************************************
 * Copyright (c) 2009, 2012 Erik Berglund.
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
package org.conserve.objects;

/**
 * An object that contains a reference to itself.
 * 
 * @author Erik Berglund
 *
 */
public class SelfContainingObject
{
	private SelfContainingObject self;

	/**
	 * @return the self
	 */
	public SelfContainingObject getSelf()
	{
		return self;
	}

	/**
	 * @param self the self to set
	 */
	public void setSelf(SelfContainingObject self)
	{
		this.self = self;
	}

}
