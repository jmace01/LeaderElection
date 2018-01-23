package jmace.LeaderElection;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import jmace.LeaderElection.network.NetworkManager;
import jmace.LeaderElection.network.RabbitMQNetworkManager;

public class Test 
{
	public static final String RMQ_HOST = "localhost";
	public static final String RMQ_QUEUE = "TestQueue";
	public static final long HEAD_POLL_DELAY_MS = 1200;
	public static final long NODE_POLL_DELAY_MS = 3000;
	public static final int NUM_NODES = 5;
	public static final int NUM_LEADERS = 3; 
	
    public static void main(String[] args) throws InterruptedException
    {
    		Set<LeaderElection<String>> nodes = new HashSet<>();
    		Set<String> downed = new HashSet<>();
    		
    		System.out.println("=======================================");
        System.out.println("      Nodes joining the network        ");
        System.out.println("=======================================");
    		
        for (int i = 0; i < NUM_NODES; i++)
        {
        		NetworkManager<String> manager = new RabbitMQNetworkManager<>(UUID.randomUUID().toString(), NUM_LEADERS, RMQ_HOST, RMQ_QUEUE);
        		LeaderElection<String> node = new LeaderElection<>(manager, HEAD_POLL_DELAY_MS, NODE_POLL_DELAY_MS); 
        		nodes.add(node);
        		node.start();
        }
        
        Thread.sleep(4000);
        
        System.out.println("=======================================");
        System.out.println("      Taking down a leader node        ");
        System.out.println("=======================================");
        
        for (LeaderElection<String> node : nodes)
        {
        		if (node.isLeader() && !node.isHead())
        		{
        			node.interrupt();
        			downed.add(node.getSelfId());
        			break;
        		}
        }
        
        Thread.sleep(HEAD_POLL_DELAY_MS * 2);
        
        System.out.println("=======================================");
        System.out.println("      Taking down the head node        ");
        System.out.println("=======================================");
        
        for (LeaderElection<String> node : nodes)
        {
        		if (node.isHead())
        		{
        			node.interrupt();
        			downed.add(node.getSelfId());
        			break;
        		}
        }
        
        Thread.sleep((HEAD_POLL_DELAY_MS + NODE_POLL_DELAY_MS) * 2);
        
        System.out.println("=======================================");
        System.out.println("      Stopping all of the nodes        ");
        System.out.println("=======================================");
        
        for (LeaderElection<String> node : nodes)
        {
        		node.interrupt();
        }
        
        Thread.sleep(1000);
        
        for (LeaderElection<String> node : nodes)
        {
        		System.out.println("[ " + node.getSelfId() + " ]\tisHead: " + node.isHead() + ",\tisLeader: " + node.isLeader() + ",\t killed: " + downed.contains(node.getSelfId()));
        }
    }
}
