package org.conserve.sort;


/**
 * Special Sorter that sorts ascending according to the database ID column.
 * This is useful to iterate over searches where no Order clause has been specified.
 * 
 * @author Erik Berglund
 *
 */
public class DatabaseIDSorter extends Ascending
{
	private Class<?> sortClass;

	/**
	 * @param sortBy
	 */
	public DatabaseIDSorter(Class<?> sortBy)
	{
		super(null);
		sortClass = sortBy;
	}

	/**
	 * @see org.conserve.sort.Sorter#getSortClass()
	 */
	@Override
	public Class<?> getSortClass()
	{
		return sortClass;
	}
	
	
}
