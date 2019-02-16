/*******************************************************************************
 *  
 * Copyright (c) 2009, 2019 Erik Berglund.
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
package com.github.conserveorm.objects;

import com.github.conserveorm.annotations.AsBlob;
import com.github.conserveorm.annotations.AsClob;

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
