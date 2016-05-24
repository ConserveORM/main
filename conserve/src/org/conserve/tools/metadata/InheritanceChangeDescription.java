package org.conserve.tools.metadata;

import java.util.ArrayList;
import java.util.List;

import org.conserve.tools.metadata.ObjectStack.Node;

/**
 * Describes the changes in the object model where one class has been moved from one superclass to another.
 * It also contains a list of fields that have been moved.
 * 
 * @author Erik Berglund
 *
 */
public class InheritanceChangeDescription
{
	private List<List<Node>> removedSuperClasses = new ArrayList<>();
	private List<List<Node>> addedSuperClasses = new ArrayList<>();
	private List<FieldChangeDescription> movedFields = new ArrayList<>();
	
	/**
	 * Get change descriptions for all fields that have been moved from an old superclass to a new one.
	 * Never returns null - if there are no changed fields an empty list is returned.
	 * 
	 * @return
	 */
	public List<FieldChangeDescription> getMovedFields()
	{
		return this.movedFields;
	}
	

	/**
	 * Get a list of former superclasses.
	 * @return the removedSuperClasses
	 */
	public List<List<Node>> getRemovedSuperClasses()
	{
		return removedSuperClasses;
	}

	/**
	 * Get a list of new superclasses.
	 * @return the addedSuperClass
	 */
	public List<List<Node>> getAddedSuperClasses()
	{
		return addedSuperClasses;
	}

	/**
	 * @param fcDesc
	 */
	public void addMovedField(FieldChangeDescription fcDesc)
	{
		this.movedFields.add(fcDesc);		
	}


	/**
	 * @param realDeleted
	 */
	public void addRemovedNodeList(List<Node> realDeleted)
	{
		removedSuperClasses.add(realDeleted);		
	}


	/**
	 * True if the inheritance model has changed so much that checking for changes in the actual object is pointless.
	 * 
	 * @return
	 */
	public boolean inheritanceModelChanged()
	{
		return !movedFields.isEmpty();
	}
}
