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
package org.conserve.select.discriminators;

import java.lang.reflect.Constructor;

import org.conserve.select.ConditionalClause;

/**
 * @author Erik Berglund
 * 
 */
public abstract class Selector extends ConditionalClause implements Cloneable
{
	private Object selectBy;
	private Class<?> selectionClass;
	private boolean strictInheritance = true;

	/**
	 * Create a new selector
	 * 
	 * @param sel
	 *            the object used to discriminate in this selector
	 * 
	 * @param strictInheritance
	 *            whether to require strict inheritance matching or not.
	 */
	protected Selector(Object sel, boolean strictInheritance)
	{
		this(sel, sel.getClass(), strictInheritance);
	}

	/**
	 * @param sel
	 *            the object used to discriminiate in this selector.
	 * @param clazz
	 *            the class sel will be interpreted as belonging to, must be the
	 *            equal to sel.getClass(), one of its superclasses or
	 *            interfaces.
	 * @param strictInheritance
	 *            whether to require strict inheritance matching or not.
	 */
	protected Selector(Object sel, Class<?> clazz, boolean strictInheritance)
	{
		this.selectBy = sel;
		this.selectionClass = clazz;
		this.strictInheritance = strictInheritance;
	}

	/**
	 * Create a new selector
	 * 
	 * @param sel
	 *            the object used to discriminate in this selector
	 */
	protected Selector(Object sel)
	{
		this(sel, true);
	}

	/**
	 * @param sel
	 *            the object used to discriminiate in this selector.
	 * @param clazz
	 *            the class sel will be interpreted as belonging to, must be the
	 *            equal to sel.getClass(), one of its superclasses or
	 *            interfaces.
	 */
	protected Selector(Object sel, Class<?> clazz)
	{
		this(sel, clazz, true);
	}

	@Override
	public String getKeyWord()
	{
		return null;
	}

	/**
	 * @return the object used to discriminate in this selector
	 */
	public Object getSelectionObject()
	{
		return selectBy;
	}

	/**
	 * Return the class SelectionObject will be interpreteted as belonging to.
	 * 
	 * @return the class this selector operates on.
	 */
	public Class<?> getSelectionClass()
	{
		return selectionClass;
	}

	public void setSelectionClass(Class<?> c)
	{
		this.selectionClass = c;
	}

	/**
	 * @see org.conserve.select.Clause#setQueryClass(java.lang.Class)
	 */
	@Override
	public void setQueryClass(Class<?> queryClass)
	{
		setSelectionClass(queryClass);
		super.setQueryClass(queryClass);
	}
	
	@Override 
	public Class<?> getQueryClass()
	{
		return getSelectionClass();
	}

	/**
	 * Indicates whether this object requires a placeholder, or if the value is
	 * inherent in the query string. e.g. "IS NULL" does not require a
	 * placeholder, but ">=" does: ">= ?". The placeholder is typically a
	 * question mark or a subquery.
	 * 
	 * The default value is true.
	 * 
	 * @return true if this selector takes a placeholder.
	 */
	public boolean takesPlaceholder()
	{
		return true;
	}

	public abstract String getRelationalRepresentation();

	/**
	 * Return a selector of the same class, but with a new object as the
	 * selection object.
	 * 
	 * @param selectionObject
	 *            the new selection object.
	 * @return an exact copy of the selector, but with a new selection object.
	 */
	public Selector duplicate(Object selectionObject)
	{
		return duplicate(selectionObject,selectionObject.getClass());
	}
	

	/**
	 * Return a selector of the same class, but with a new object as the
	 * selection object.
	 * 
	 * @param selectionObject
	 *            the new selection object.
	 * @param selectionClass
	 *            class to select from.
	 * @return an exact copy of the selector, but with a new selection object.
	 */
	public Selector duplicate(Object selectionObject,
			Class<?> selectionClass)
	{
			try
			{
				Constructor<? extends Selector> constr = getClass().getConstructor(Object.class,Class.class,boolean.class);
				return constr.newInstance(selectionObject,selectionClass,this.isStrictInheritance());
			}
			catch (Exception e)
			{
				// we use a catch-all exception here, since there is no way to recover from an error.
				return null;
			}
	}

	/**
	 * @param strictInheritance
	 *            the strictInheritance to set
	 */
	public void setStrictInheritance(boolean strictInheritance)
	{
		this.strictInheritance = strictInheritance;
	}

	/**
	 * @return the strictInheritance
	 */
	public boolean isStrictInheritance()
	{
		return strictInheritance;
	}

}
