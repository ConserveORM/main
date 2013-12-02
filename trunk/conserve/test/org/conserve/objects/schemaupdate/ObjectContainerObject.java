package org.conserve.objects.schemaupdate;

/**
 * Same as ContainerObject, but contains an Object instead of an OriginalObject
 * @author Erik Berglund
 *
 */
public class ObjectContainerObject
{
	private Object foo;
	public ObjectContainerObject()
	{
		
	}
	/**
	 * @return the foo
	 */
	public Object getFoo()
	{
		return foo;
	}
	/**
	 * @param foo the foo to set
	 */
	public void setFoo(Object foo)
	{
		this.foo = foo;
	}

}
