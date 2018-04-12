/*******************************************************************************
 *  
 * Copyright (c) 2009, 2018 Erik Berglund.
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

import java.util.HashSet;
import java.util.Set;

import com.github.conserveorm.annotations.MaxLength;

/**
 * @author Erik Berglund
 *
 */
public class Book
{
	private Set<Author>authors;
	private Integer publishedYear;
	private HashSet<String>keyWords;
	private String title;
	
	public Book()
	{
		
	}
	
	public Book(String title)
	{
		this.setTitle(title);
	}
	
	/**
	 * @return the authors
	 */
	public Set<Author> getAuthors()
	{
		return authors;
	}
	/**
	 * @param authors the authors to set
	 */
	public void setAuthors(Set<Author> authors)
	{
		this.authors = authors;
	}
	/**
	 * @return the publishedYear
	 */
	public Integer getPublishedYear()
	{
		return publishedYear;
	}
	/**
	 * @param publishedYear the publishedYear to set
	 */
	public void setPublishedYear(Integer publishedYear)
	{
		this.publishedYear = publishedYear;
	}
	/**
	 * @return the keyWords
	 */
	public HashSet<String> getKeyWords()
	{
		return keyWords;
	}
	/**
	 * @param keyWords the keyWords to set
	 */
	public void setKeyWords(HashSet<String> keyWords)
	{
		this.keyWords = keyWords;
	}
	/**
	 * @return the title
	 */
	@MaxLength(1024)
	public String getTitle()
	{
		return title;
	}
	/**
	 * @param title the title to set
	 */
	public void setTitle(String title)
	{
		this.title = title;
	}
	public void addAuthor(Author author)
	{
		if(authors==null)
		{
			authors = new HashSet<>();
		}
		authors.add(author);
	}
	
	public void addKeyWord(String keyWord)
	{
		if(keyWords == null)
		{
			keyWords = new HashSet<>();
		}
		this.keyWords.add(keyWord);
	}
	
}
