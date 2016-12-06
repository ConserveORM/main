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
package com.github.conserveorm;

/**
 * Listener that receives one single object from a search result. This can be
 * used instead of retrieving a list of objects with the getObjects(...)
 * methods.
 * 
 * This has the following advantages:
 * 
 * 1. Consumes less memory, as only one object is loaded at a time.
 * 
 * 2. Faster, as client code can start processing the results while the search
 * is still being carried out.
 * 
 * 3. Shorter locking times, as only short reads are performed. On some
 * databases this can speed up concurrent writes.
 * 
 * The client code implements this object, and passes it to an appropriate
 * getObjects(...) method along with the search parameters. The search results
 * will be passed to the objectFound(...) method, one by one.
 * 
 * 
 * @author Erik Berglund
 * 
 * @param <T>
 *            the type of object to listen for.
 * 
 */
public interface SearchListener<T>
{
	/**
	 * Callback that notifies us that an object has been found in our search.
	 * The search will not continue until this method returns, so if heavy
	 * processing is being done it's best to offload it to a separate thread and
	 * return immediately from this method.
	 * 
	 * @param object the object found and loaded from the database.
	 */
	public void objectFound(T object);
}
