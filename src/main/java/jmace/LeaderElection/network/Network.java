package jmace.LeaderElection.network;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

public class Network<T extends Comparable<T>>
{
	private final Set<T> network;
	
	public Network()
	{
		this.network = new TreeSet<>();
	}
	
	public boolean addNode(T node)
	{
		return network.add(node);
	}
	
	public boolean addAllNodes(Collection<T> nodes)
	{
		return network.addAll(nodes);
	}
	
	public boolean removeAllNodes(Collection<T> nodes)
	{
		return network.removeAll(nodes);
	}
	
	public boolean removeNode(T node)
	{
		return network.remove(node);
	}
	
	public Set<T> getNetwork()
	{
		return new TreeSet<>(network);
	}
	
	public T getHeadNode()
	{
		if (isEmpty())
		{
			return null;
		}
		return Collections.min(network);
	}
	
	public boolean isEmpty()
	{
		return network.isEmpty();
	}
	
	public Set<T> getLeaders(int numberOfLeaders)
	{
		return new TreeSet<>(new ArrayList<>(network).subList(0, Math.min(network.size(), numberOfLeaders)));
	}
}
