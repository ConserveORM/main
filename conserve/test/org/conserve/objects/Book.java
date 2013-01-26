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

import java.util.HashSet;
import java.util.Set;

import org.conserve.annotations.MaxLength;

/**
 * @author Erik Berglund
 *
 */
public class Book
{
	private Set<Author>authors=new HashSet<Author>();
	private Integer publishedYear;
	private HashSet<String>keyWords = new HashSet<String>();
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
		authors.add(author);
	}
	
	public void addKeyWord(String keyWord)
	{
		this.keyWords.add(keyWord);
	}
	
}
