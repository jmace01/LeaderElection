package jmace.LeaderElection.network;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import jmace.LeaderElection.task.ChangeSubscriber;

public class Network<T extends Comparable<T>>
{
	private final Set<T> network;
	private final List<ChangeSubscriber> subscribers;
	
	public Network()
	{
		this.network = new TreeSet<>();
		this.subscribers = new ArrayList<>();
	}
	
	public void addChangeSubscriber(ChangeSubscriber subscriber)
	{
		subscribers.add(subscriber);
	}
	
	public boolean addNode(T node)
	{
		boolean changed = network.add(node);
		if (changed) alertSubscribers();
		return changed;
	}
	
	public boolean addAllNodes(Collection<T> nodes)
	{
		boolean changed = network.addAll(nodes); 
		if (changed) alertSubscribers();
		return changed;
	}
	
	public boolean removeAllNodes(Collection<T> nodes)
	{
		boolean changed = network.removeAll(nodes);
		if (changed) alertSubscribers();
		return changed;
	}
	
	public boolean removeNode(T node)
	{
		boolean changed = network.remove(node);
		if (changed) alertSubscribers();
		return changed;
	}
	
	public Set<T> getNodes()
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
	
	private void alertSubscribers()
	{
		subscribers.forEach(s -> s.handleChange());
	}
}
