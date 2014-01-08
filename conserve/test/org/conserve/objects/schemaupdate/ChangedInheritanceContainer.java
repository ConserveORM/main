package org.conserve.objects.schemaupdate;

import java.io.Serializable;

/**
 * An object that contains objects that will change inheritance.
 * Identical to ContainerObject but has a Serializable instead of an OriginalObject property.
 * 
 * @author Erik Berglund
 *
 */
public class ChangedInheritanceContainer
{
	private Serializable foo;
	public ChangedInheritanceContainer()
	{	
	}
	
	/**
	 * @return the foo
	 */
	public Serializable getFoo()
	{
		return foo;
	}
	
	/**
	 * @param foo the foo to set
	 */
	public void setFoo(Serializable foo)
	{
		this.foo = foo;
	}
	
}
