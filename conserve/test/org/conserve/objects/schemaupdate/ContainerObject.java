package org.conserve.objects.schemaupdate;

/**
 * 
 * An object that contains objects that will change inheritance.
 * @author Erik Berglund
 *
 */
public class ContainerObject
{
	private OriginalObject foo;
	public ContainerObject()
	{
		
	}
	/**
	 * @return the foo
	 */
	public OriginalObject getFoo()
	{
		return foo;
	}
	/**
	 * @param foo the foo to set
	 */
	public void setFoo(OriginalObject foo)
	{
		this.foo = foo;
	}
	
}
