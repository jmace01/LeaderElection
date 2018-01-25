package jmace.LeaderElection.network;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeoutException;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import jmace.LeaderElection.messages.Request;
import jmace.LeaderElection.messages.RequestType;
import jmace.LeaderElection.task.ChangeSubscriber;

public class RabbitMQNetworkManager<T extends Comparable<T>> extends NetworkManager<T>
{
	private static final String CONSUMER_TAG = "basicConsumer";
	
	private final T id;
	private final int numberOfLeaders;
	private final String queueHost;
	private final String exchangeName;
	private String queueName;
	private Connection connection;
	private Channel channel;
	boolean headIsUp;
	private Set<T> pollReponders;
	private final Gson gson;
	
	/**
	 * Constructor
	 * 
	 * @param id the ID of this node
	 * @param numberOfLeaders the number of leaders allowed on the network
	 * @param queueHost the RabbitMQ host
	 * @param exchangeName the RabbitMQ exchange
	 */
	public RabbitMQNetworkManager(T id, int numberOfLeaders, String queueHost, String exchangeName)
	{
		this.id = id;
		this.numberOfLeaders = numberOfLeaders;
		this.queueHost = queueHost;
		this.exchangeName = exchangeName;
		this.connection = null;
		this.channel = null;
		this.headIsUp = true;
		this.pollReponders = null;
		this.gson = new Gson();
	}
	
	/**
	 * Get the ID of this node
	 */
	public T getSelf()
	{
		return id;
	}
	
	/**
	 * Get the ID of the current network head
	 */
	public T getHead()
	{
		return getNetwork().getHeadNode();
	}
	
	/**
	 * Get whether or not this node is the head of the network
	 * @return true if this node is the network head
	 */
	public Boolean isHead()
	{
		if (getNetwork().isEmpty())
		{
			return null;
		}
		return id.equals(getNetwork().getHeadNode());
	}
	
	/**
	 * Check if this node currently a leader on the network
	 * @return true if this node is a leader, null if the network hasn't been established, false otherwise
	 */
	public Boolean isLeader()
	{
		if (getNetwork().isEmpty())
		{
			return null;
		}
		return getNetwork().getLeaders(numberOfLeaders).contains(id);
	}
	
	/**
	 * Checks if the head node has polled since the last time this method was called
	 * @return true if the head node has polled, false otherwise
	 */
	public boolean isHeadUp()
	{
		boolean wasUp = headIsUp;
		headIsUp = false;
		return wasUp;
	}
	
	/**
	 * Gets all nodes that have sent this node a still alive message
	 * @return a set of all nodes that responded with a still alive message
	 */
	public Set<T> getUpNodes()
	{
		return new TreeSet<>(pollReponders);
	}
	
	/**
	 * Establishes the connections to RabbitMQ and sets up a consumer
	 * @throws IOException
	 * @throws TimeoutException
	 */
	private void establishQueueConnection() throws IOException, TimeoutException
	{
		ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(queueHost);
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.exchangeDeclare(exchangeName, "topic");
        queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, exchangeName, id.toString());
        channel.queueBind(queueName, exchangeName, "all");
        
        Consumer consumer = new DefaultConsumer(channel)
        {
        		@Override
        		public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        			try
        			{
        				System.out.println(id + ":" + new String(body, "UTF-8"));
        				Request<T> request = gson.fromJson(new String(body, "UTF-8"), new TypeToken<Request<T>>(){}.getType());
        				handleMessage(request);
        			}
        			catch (Exception e)
        			{
        				e.printStackTrace();
        			}
        		}
        };
        channel.basicConsume(queueName, true, CONSUMER_TAG, consumer);
	}
	
	public void start()
	{
		try
		{
			establishQueueConnection();
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * Shuts down the connection to RabbitMQ
	 */
	public void stop()
	{
		try
		{
			if (channel != null) channel.close();
			if (connection != null) connection.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			if (channel != null && !channel.isOpen()) channel = null;
			if (connection != null && !connection.isOpen()) connection = null;
		}
	}
	
	/**
	 * Routes messages to the proper functions
	 * @param request the incoming message
	 */
	private void handleMessage(Request<T> request)
	{
		switch (request.getType())
		{
			case ADD_TO_NETWORK:
				Set<T> toAdd = gson.fromJson(request.getData(), new TypeToken<TreeSet<T>>(){}.getType());
				addToNetwork(request.getRequestingID(), toAdd, request.getRequestingID().toString());
			break;
			case REMOVE_FROM_NETWORK:
				Set<T> toRemove = gson.fromJson(request.getData(), new TypeToken<TreeSet<T>>(){}.getType());
				removeFromNetwork(toRemove);
			break;
			case POLL_NODES:
				sendStillAlive(request);
			break;
			case STILL_ALIVE:
				pollReponders.add(request.getRequestingID());
				if (getNetwork().addNode(request.getRequestingID()))
					broadcastNetwork();
			break;
		}
	}
	
	/**
	 * Add a node to the network
	 * @param sender the node that requested the addition
	 * @param toAdd to nodes to add
	 * @param resendRoutingKey the routing key if we need to send back missing nodes
	 */
	private void addToNetwork(T sender, Set<T> toAdd, String resendRoutingKey)
	{
		//If we have anything the sending process is missing, send over what we have
		Boolean isHead = isHead();
		boolean rebroadcast = isHead != null && isHead && toAdd.addAll(getNetwork().getNodes());
		
		getNetwork().addAllNodes(toAdd);
		if (isHead() && pollReponders != null)
		{
			pollReponders.add(sender);
		}
		
		if (rebroadcast)
			broadcastNetwork(resendRoutingKey);
	}
	
	/**
	 * Remove nodes from the network
	 * @param toRemove the nodes to remove
	 */
	public void removeFromNetwork(Set<T> toRemove)
	{
		if (toRemove.contains(getHead()))
		{
			//If the head node is being remove, mark the new head as up
			//so that it has time to respond
			headIsUp = true;
		}
		getNetwork().removeAllNodes(toRemove);
	}
	
	/**
	 * Tell the nodes on the network to remove a set of nodes from the network
	 * @param nodes the nodes to remove
	 */
	public void broadcastRemoveFromNetwork(Set<T> nodes)
	{
		try
		{
			Request<T> request = new Request<T>(id, RequestType.REMOVE_FROM_NETWORK, gson.toJson(nodes));
			broadcastMessage(exchangeName, null, request);
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Send a message to the head node letting it know this node is still responsive
	 * @param pollRequest the original poll request
	 */
	private void sendStillAlive(Request<T> pollRequest)
	{
		try
		{
			headIsUp = true;
			Request<T> request = new Request<T>(id, RequestType.STILL_ALIVE, null);
			broadcastMessage(exchangeName, pollRequest.getRequestingID().toString(), request);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	/**
	 * Send out this node's copy of the network
	 */
	public void broadcastNetwork()
	{
		broadcastNetwork(null);
	}
	
	/**
	 * Send out this node's copy of the network
	 * @param resendRoutingKey the node to send the network to
	 */
	private void broadcastNetwork(String resendRoutingKey)
	{
		try
		{
			Set<T> nodes;
			if (!getNetwork().isEmpty())
			{
				nodes = getNetwork().getNodes();
			}
			else
			{
				nodes = new TreeSet<>();
				nodes.add(id);
			}
			Request<T> request = new Request<T>(id, RequestType.ADD_TO_NETWORK, gson.toJson((nodes)));
			broadcastMessage(exchangeName, resendRoutingKey, request);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	/**
	 * Send a request to determine what nodes are still responsive
	 */
	public void pollForNodes()
	{
		try
		{
			pollReponders = new TreeSet<>();
			Request<T> request = new Request<T>(id, RequestType.POLL_NODES, null);
			broadcastMessage(exchangeName, null, request);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	/**
	 * Send a message over RabbitMQ
	 * @param queueName the name of the queue to send the message through
	 * @param routingKey the key to send a message through (e.g. the node to send a message to)
	 * @param request the request to send
	 * @throws UnsupportedEncodingException
	 * @throws IOException
	 * @throws TimeoutException
	 */
	private void broadcastMessage(String queueName, String routingKey, Request<T> request) throws UnsupportedEncodingException, IOException, TimeoutException
	{
		if (channel == null)
		{
			establishQueueConnection();
		}

		String message = gson.toJson(request);
		channel.basicPublish(queueName, routingKey == null ? "all" : routingKey, null, message.getBytes("UTF-8"));
	}
}
