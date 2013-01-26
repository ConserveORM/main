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
package org.conserve.objects;

import org.conserve.annotations.AsBlob;
import org.conserve.annotations.AsClob;

/**
 * Object that tests the functionality of optionally declaring byte or char arrays to be Blobs or Clobs, respectively.
 * 
 * @author Erik Berglund
 * 
 */
public class BlobClobObject
{
	private byte[] bytes;
	private char[] chars;
	
	/**
	 * @return the bytes
	 */
	@AsBlob
	public byte[] getBytes()
	{
		return bytes;
	}
	
	/**
	 * @param bytes the bytes to set
	 */
	public void setBytes(byte[] bytes)
	{
		this.bytes = bytes;
	}
	
	/**
	 * @return the chars
	 */
	@AsClob
	public char[] getChars()
	{
		return chars;
	}
	
	/**
	 * @param chars the chars to set
	 */
	public void setChars(char[] chars)
	{
		this.chars = chars;
	}
}
