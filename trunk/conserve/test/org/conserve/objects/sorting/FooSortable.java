package org.conserve.objects.sorting;

/**
 * @author Erik Berglund
 *
 */
public class FooSortable implements Sortable
{

	private Integer foo;

	/**
	 * @see org.conserve.objects.sorting.Sortable#getFoo()
	 */
	@Override
	public Integer getFoo()
	{
		return foo;
	}

	/**
	 * @see org.conserve.objects.sorting.Sortable#setFoo(java.lang.Integer)
	 */
	@Override
	public void setFoo(Integer foo)
	{
		this.foo = foo;
	}
}
