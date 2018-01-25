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
		networkManager.getNetwork().addChangeSubscriber(() -> {
			handleNetworkChange();
		});
	}
	
	/**
	 * Begin running the leader background processes
	 */
	public void run()
	{
		networkManager.broadcastNetwork();
	}
	
	public Boolean isHead()
	{
		return isHead;
	}
	
	/**
	 * Check if the current node is a leader
	 * @return true if the node is a leader, false otherwise
	 */
	public Boolean isLeader()
	{
		return isLeader;
	}
	
	/**
	 * Get the ID of this node
	 * @return the ID of this node
	 */
	public T getSelfId()
	{
		return networkManager.getSelf();
	}
	
	/**
	 * Starts up the heart beat/polling task
	 * Used to ensure that all nodes on the network are still responsive
	 */
	private void startHeadTask()
	{
		if (task != null) task.interrupt();
		headPolled = false;
		task = new SimpleTimedTask(headPollDelayMS, () ->  {
			try
			{
				//Ensure we've polled at least once already so we have results to check
				if (headPolled)
				{
					//Get all the nodes on the network
					Set<T> nodes = networkManager.getNetwork().getNodes();
					//Get all the nodes that responded
					Set<T> upNodes = networkManager.getUpNodes();
					//If this node is not found in the responses, the poll was invalid
					if (upNodes.contains(networkManager.getSelf()))
					{
						//Get the nodes that didn't respond
						nodes.removeAll(upNodes);
						//If any didn't respond, have them removed
						if (!nodes.isEmpty())
						{
							networkManager.broadcastRemoveFromNetwork(nodes);
						}
					}
				}
				//Poll again
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
				//Ensure the process hasn't just started so that the head node has time to respond
				//If we have polled before (e.g. we didn't just start) and the head node hasn't polled in the expected time frame
				if (nodePolled && !networkManager.isHeadUp())
				{
					//Send a message to remove the head node
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

	/**
	 * Deal with changes in the network
	 * Needed for when this node becomes a leader or head node, or when leadership is usurped by another node
	 */
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
	
	/**
	 * Stop the processes
	 */
	public void interrupt()
	{
		task.interrupt();
		networkManager.stop();
		super.interrupt();
	}
}
