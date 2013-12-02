package org.conserve.objects.schemaupdate;

/**
 * An object that contains objects that will change inheritance.
 * Identical to ContainerObject but has a NewName instead of an OriginalObject property.
 * 
 * @author Erik Berglund
 *
 */
public class NewNameContainer
{
	private NewName foo;
	public NewNameContainer()
	{
		
	}
	/**
	 * @return the foo
	 */
	public NewName getFoo()
	{
		return foo;
	}
	/**
	 * @param foo the foo to set
	 */
	public void setFoo(NewName foo)
	{
		this.foo = foo;
	}
	
	
}
