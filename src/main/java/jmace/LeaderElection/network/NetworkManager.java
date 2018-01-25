package jmace.LeaderElection.network;

import java.io.IOException;
import java.util.Set;

public abstract class NetworkManager<T extends Comparable<T>>
{
	private Network<T> network;
	public NetworkManager()
	{
		this.network = new Network<>();
	}
	
	public final Network<T> getNetwork()
	{
		return this.network;
	}
	
	public abstract T getSelf();
	public abstract T getHead();
	public abstract Boolean isHead();
	public abstract Boolean isLeader();
	public abstract boolean isHeadUp();
	public abstract Set<T> getUpNodes();
	public abstract void pollForNodes();
	public abstract void broadcastNetwork();
	public abstract void broadcastRemoveFromNetwork(Set<T> nodes);
	public abstract void start();
	public abstract void stop();
}
