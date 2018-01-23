package jmace.LeaderElection;

import java.util.HashSet;
import java.util.Set;

import jmace.LeaderElection.network.NetworkManager;
import jmace.LeaderElection.task.SimpleTimedTask;

public class LeaderElection<T extends Comparable<T>> extends Thread
{
	
	private Boolean isHead;
	private Boolean isLeader;
	private T headNode;
	private final long headPollDelayMS;
	private final long nodePollDelayMS;
	private boolean nodePolled;
	private boolean headPolled;
	private SimpleTimedTask task;
	private final NetworkManager<T> networkManager;
	
	/**
	 * Constructor for the leader election class
	 * 
	 * @param networkManager the class that will handle network communication.
	 * @param headPollDelayMS delay before a node on the network is considered dead if it cannot be contacted.
	 * @param nodePollDelayMS time before the head node can be considered dead if it has cannot be contacted.
	 *							Recommended to be more than headPollDelayMS * 2.
	 */
	public LeaderElection(NetworkManager<T> networkManager, long headPollDelayMS, long nodePollDelayMS)
	{
		this.isHead = null;
		this.isLeader = null;
		this.headNode = null;
		this.headPollDelayMS = headPollDelayMS;
		this.nodePollDelayMS = nodePollDelayMS;
		this.task = null;
		this.networkManager = networkManager;
		networkManager.setChangeSubscriber(() -> {
			handleNetworkChange();
		});
	}
	
	public void run()
	{
		networkManager.broadcastNetwork();
	}
	
	public Boolean isHead()
	{
		return isHead;
	}
	
	public Boolean isLeader()
	{
		return isLeader;
	}
	
	public T getSelfId()
	{
		return networkManager.getSelf();
	}
	
	private void startHeadTask()
	{
		if (task != null) task.interrupt();
		headPolled = false;
		task = new SimpleTimedTask(headPollDelayMS, () ->  {
			try
			{
				if (headPolled)
				{
					Set<T> nodes = networkManager.getNetwork();
					Set<T> upNodes = networkManager.getUpNodes();
					//If this node is not found in the responses, the poll was invalid
					if (upNodes.contains(networkManager.getSelf()))
					{
						nodes.removeAll(upNodes);
						nodes.remove(networkManager.getSelf());
						if (!nodes.isEmpty())
						{
							networkManager.broadcastRemoveFromNetwork(nodes);
						}
					}
				}
				headPolled = true;
				networkManager.pollForNodes();
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		});
		task.start();
	}
	
	private void startNodeTask()
	{
		if (task != null) task.interrupt();
		nodePolled = false;
		task = new SimpleTimedTask(nodePollDelayMS, () -> {
			try
			{
				if (nodePolled && !networkManager.isHeadUp())
				{
					Set<T> toRemove = new HashSet<>();
					toRemove.add(networkManager.getHead());
					networkManager.broadcastRemoveFromNetwork(toRemove);
				}
				nodePolled = true;
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		});
		task.start();
	}

	private void handleNetworkChange() {
		if (networkManager.isHead() && (isHead == null || !isHead))
		{
			startHeadTask();
		}
		else if (!networkManager.isHead() && (isHead == null || isHead))
		{
			startNodeTask();
		}
		
		if (networkManager.isLeader() && isLeader != null && !isLeader)
		{
			//start job
		}
		else if (!networkManager.isLeader() && isLeader != null && isLeader)
		{
			//stop job
		}
		
		this.isHead = networkManager.isHead();
		this.isLeader = networkManager.isLeader();
		
		if (!isHead && !networkManager.getHead().equals(headNode))
		{
			headNode = networkManager.getHead();
			task.restart();
		}
	}
	
	public void interrupt()
	{
		task.interrupt();
		networkManager.stop();
		super.interrupt();
	}
}
