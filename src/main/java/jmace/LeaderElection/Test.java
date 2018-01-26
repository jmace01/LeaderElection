package jmace.LeaderElection;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import jmace.LeaderElection.network.NetworkManager;
import jmace.LeaderElection.network.RabbitMQNetworkManager;

public class Test 
{
	public static final String RMQ_HOST = "localhost";
	public static final String RMQ_QUEUE = "TestQueue";
	public static final long HEAD_POLL_DELAY_MS = 800;
	public static final long NODE_POLL_DELAY_MS = 2000;
	public static final int NUM_NODES = 5;
	public static final int NUM_LEADERS = 3;
	public static final String MESSAGE_BORDER = "======================================================================";
	
	private static void printMessage(String message)
	{
		int pad = (MESSAGE_BORDER.length() - message.length()) / 2;
		System.out.println(MESSAGE_BORDER);
		System.out.println(pad > 0 ? String.format("%" + (pad + message.length()) + "s", message) : message);
		System.out.println(MESSAGE_BORDER);
	}
	
	private static Set<LeaderElection<String>> createNodes(int numNodes, int numLeaders, String rmqHost, String rmqQueue, long headPollMS, long nodePollMS)
	{
		Set<LeaderElection<String>> nodes = new HashSet<>();
		for (int i = 0; i < numNodes; i++)
        {
        		NetworkManager<String> manager = new RabbitMQNetworkManager<>(UUID.randomUUID().toString(), numLeaders, rmqHost, rmqQueue);
        		LeaderElection<String> node = new LeaderElection<>(manager, headPollMS, nodePollMS); 
        		nodes.add(node);
        		node.start();
        }
		return nodes;
	}
	
	private static LeaderElection<String> pauseLeader(Set<LeaderElection<String>> nodes) throws IOException
	{
        for (LeaderElection<String> node : nodes)
        {
        		if (node.isLeader() && !node.isHead())
        		{
        			node.pause();
        			return node;
        		}
        }
        return null;
	}
	
	private static LeaderElection<String> pauseHead(Set<LeaderElection<String>> nodes) throws IOException
	{
		for (LeaderElection<String> node : nodes)
        {
        		if (node.isHead())
        		{
        			node.pause();
        			return node;
        		}
        }
		return null;
	}
	
	/**
	 * Simple demo
	 * @param args unused
	 * @throws InterruptedException unused
	 * @throws IOException 
	 */
    public static void main(String[] args) throws InterruptedException, IOException
    {
        printMessage("Nodes joining the network");
        Set<LeaderElection<String>> nodes = createNodes(NUM_NODES, NUM_LEADERS, RMQ_HOST, RMQ_QUEUE, HEAD_POLL_DELAY_MS, NODE_POLL_DELAY_MS);
        Thread.sleep(4000);
        
        printMessage("Pausing a leader node");
        LeaderElection<String> paused = pauseLeader(nodes);
        Thread.sleep(HEAD_POLL_DELAY_MS * 2);
        
        printMessage("Pausing the head node");
        LeaderElection<String> head = pauseHead(nodes);
        Thread.sleep((HEAD_POLL_DELAY_MS + NODE_POLL_DELAY_MS) * 2);
        
        printMessage("Unpausing leader node");
        if (paused != null) paused.unpause();
        Thread.sleep(HEAD_POLL_DELAY_MS * 8);
        
        printMessage("Unpausing head node");
        if (head != null) head.unpause();
        Thread.sleep(HEAD_POLL_DELAY_MS * 3);
        
        printMessage("Stopping all nodes");
        for (LeaderElection<String> node : nodes)
        {
        		node.interrupt();
        }
        
        Thread.sleep(2000);
        
        for (LeaderElection<String> node : nodes)
        {
        		System.out.println("[ " + node.getSelfId() + " ]\tisHead: " + node.isHead() + ",\tisLeader: " + node.isLeader() + ",\t killed: " + (paused != null && paused.getSelfId().equals(node.getSelfId()) || head != null && head.getSelfId().equals(node.getSelfId())));
        }
    }
}
