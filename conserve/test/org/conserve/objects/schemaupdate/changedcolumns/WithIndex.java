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
package org.conserve.objects.schemaupdate.changedcolumns;

import org.conserve.annotations.Indexed;
import org.conserve.annotations.MaxLength;
import org.conserve.annotations.MultiIndexed;

/**
 * A class with a column with an index.
 * 
 * @author Erik Berglund
 *
 */
public class WithIndex
{
	private String value;
	private String otherValue;

	/**
	 * @return the value
	 */
	@Indexed("fooidx")
	@MaxLength(125)
	public String getValue()
	{
		return value;
	}

	/**
	 * @param value the value to set
	 */
	public void setValue(String value)
	{
		this.value = value;
	}

	/**
	 * @return the otherValue
	 */
	@MultiIndexed(values={"fooidx","baridx"})
	@MaxLength(125)
	public String getOtherValue()
	{
		return otherValue;
	}

	/**
	 * @param otherValue the otherValue to set
	 */
	public void setOtherValue(String otherValue)
	{
		this.otherValue = otherValue;
	}
}
