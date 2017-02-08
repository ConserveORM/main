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
package com.github.conserveorm.objects;

import java.util.HashSet;

import com.github.conserveorm.annotations.MaxLength;

/**
 * @author Erik Berglund
 *
 */
public class Author
{
	private HashSet<Book> books;
	private String firstName;
	private String lastName;
	private Integer birthYear;
	
	
	public Author()
	{
	}
	
	/**
	 * @return the books by this author
	 */
	public HashSet<Book> getBooks()
	{
		return books;
	}
	/**
	 * @param books the books to set
	 */
	public void setBooks(HashSet<Book> books)
	{
		this.books = books;
	}
	/**
	 * @return the firstName
	 */
	@MaxLength(256)
	public String getFirstName()
	{
		return firstName;
	}
	/**
	 * @param firstName the firstName to set
	 */
	public void setFirstName(String firstName)
	{
		this.firstName = firstName;
	}
	/**
	 * @return the lastName
	 */
	@MaxLength(256)
	public String getLastName()
	{
		return lastName;
	}
	/**
	 * @param lastName the lastName to set
	 */
	public void setLastName(String lastName)
	{
		this.lastName = lastName;
	}
	/**
	 * @return the birthYear
	 */
	public Integer getBirthYear()
	{
		return birthYear;
	}
	/**
	 * @param birthYear the birthYear to set
	 */
	public void setBirthYear(Integer birthYear)
	{
		this.birthYear = birthYear;
	}
	
	/**
	 * Add book to this author's list of books.
	 * @param book
	 */
	public void addBook(Book book)
	{
		if(books == null)
		{
			books = new HashSet<>();
		}
		books.add(book);
		book.addAuthor(this);
	}
}
