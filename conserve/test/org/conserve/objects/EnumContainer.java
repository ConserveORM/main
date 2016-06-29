/**
 * 
 */
package org.conserve.objects;

/**
 * An object that contains a property which is an enum.
 * 
 * @author Erik Berglund
 *
 */
public class EnumContainer
{
	
	private MyEnum state;

	public MyEnum getState()
	{
		return state;
	}

	public void setState(MyEnum state)
	{
		this.state = state;
	}	
}
