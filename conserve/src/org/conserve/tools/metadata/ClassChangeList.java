package org.conserve.tools.metadata;

import java.util.ArrayList;
import java.util.List;

import org.conserve.tools.metadata.ObjectStack.Node;

public class ClassChangeList
{
	// the list of nodes that have been removed/added, including the one
	// above (superclass) and the one below (subclass).
	// If the uppermost node has no superclasses, the topmost flag is set.
	private List<Node> changedNodes;
	// if this flag is set, the node is shared between a removal list and an
	// add list, meaning the entries must be rewritten instead of simply
	// added or removed
	private boolean shared;
	private Node sharedSub;
	
	public ClassChangeList()
	{
		changedNodes=new ArrayList<>();
	}
	
	/**
	 * @return
	 */
	public int size()
	{
		return changedNodes.size();
	}

	public void addNode(Node n)
	{
		changedNodes.add(n);
	}
	/**
	 * @return the shared
	 */
	public boolean isShared()
	{
		return shared;
	}
	/**
	 * @param shared the shared to set
	 */
	public void setShared(boolean shared)
	{
		this.shared = shared;
	}
	/**
	 * @return the changedNodes
	 */
	public List<Node> getChangedNodes()
	{
		return changedNodes;
	}

	/**
	 * @param other
	 */
	public void addAll(ClassChangeList other)
	{	
		for(Node o:other.getChangedNodes())
		{
			this.addNode(o);
		}
	}

	/**
	 * @param i
	 * @return
	 */
	public Node getNode(int i)
	{
		return changedNodes.get(i);
	}

	/**
	 * @param otherSub
	 */
	public void setSharedSub(Node otherSub)
	{
		sharedSub = otherSub;		
	}
	
	/**
	 * Get the sub-class of the shared class in the other (post-change) tree.
	 */
	public Node getSharedSub()
	{
		return this.sharedSub;
	}
}