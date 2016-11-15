package org.conserve.select.discriminators;

/**
 * A Selector subclass that implements the SQL "LIKE" query.
 * @author Erik Berglund
 *
 */
public class Like extends Selector
{

	/**
	 * @param sel
	 * @param strictInheritance
	 */
	public Like(Object sel, boolean strictInheritance)
	{
		super(sel, strictInheritance);
	}

	/**
	 * @param sel
	 * @param clazz
	 * @param strictInheritance
	 */
	public Like(Object sel, Class<?> clazz, boolean strictInheritance)
	{
		super(sel, clazz, strictInheritance);
	}

	/**
	 * @param sel
	 */
	public Like(Object sel)
	{
		super(sel);
	}

	/**
	 * @param sel
	 * @param clazz
	 */
	public Like(Object sel, Class<?> clazz)
	{
		super(sel, clazz);
	}

	/**
	 * @see org.conserve.select.discriminators.Selector#getRelationalRepresentation()
	 */
	@Override
	public String getRelationalRepresentation()
	{
		return " LIKE ";
	}

}
