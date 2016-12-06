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
package com.github.conserveorm.objects.demo;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * This is a test object to check that the examples in the tutorial are correct.
 * 
 * @author Erik Berglund
 *
 */
public class TextObject
{
    private String text;
    private Set<String> keyWords;
    public TextObject(){}
	public String getText()
	{
		return text;
	}
	public void setText(String text)
	{
		this.text = text;
	}
	public Set<String> getKeyWords()
	{
		return keyWords;
	}
	public void setKeyWords(Set<String> keyWords)
	{
		this.keyWords = keyWords;
	}
	public void setKeyWords(String[] keywords)
	{
		setKeyWords(new HashSet<String>(Arrays.asList(keywords)));
		
	}
    
}
