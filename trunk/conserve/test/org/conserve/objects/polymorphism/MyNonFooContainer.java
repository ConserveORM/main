package org.conserve.objects.polymorphism;

/**
 * 
 * This is the same as MyFooContainer, but does not implement the FooContainer interface.
 * 
 * @author Erik Berglund
 *
 */
public class MyNonFooContainer
{
	private String foo;
	public String getFoo()
	{
		return foo;
	}

	public void setFoo(String foo)
	{
		this.foo = foo;
	}

}
