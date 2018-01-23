package jmace.LeaderElection.network;

import java.util.Set;
import jmace.LeaderElection.task.ChangeSubscriber;

public interface NetworkManager<T extends Comparable<T>>
{
	public T getSelf();
	public T getHead();
	public Boolean isHead();
	public Boolean isLeader();
	public boolean isHeadUp();
	public Set<T> getUpNodes();
	public Set<T> getNetwork();
	public void pollForNodes();
	public void broadcastNetwork();
	public void setChangeSubscriber(ChangeSubscriber subscriber);
	public void broadcastRemoveFromNetwork(Set<T> nodes);
	public void stop();
}
