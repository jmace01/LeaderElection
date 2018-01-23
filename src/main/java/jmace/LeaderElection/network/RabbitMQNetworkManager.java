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

public class RabbitMQNetworkManager<T extends Comparable<T>> implements NetworkManager<T>
{
	private final T id;
	private final int numberOfLeaders;
	private final String queueHost;
	private final String exchangeName;
	private Connection connection;
	private Channel channel;
	boolean headIsUp;
	private Set<T> pollReponders;
	private final Gson gson;
	private final Network<T> network;
	ChangeSubscriber subscriber;
	
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
		this.network = new Network<>();
		this.subscriber = null;
	}
	
	public T getSelf()
	{
		return id;
	}
	
	public T getHead()
	{
		return network.getHeadNode();
	}
	
	public Boolean isHead()
	{
		if (network.isEmpty())
		{
			return null;
		}
		return id.equals(network.getHeadNode());
	}
	
	public Boolean isLeader()
	{
		if (network.isEmpty())
		{
			return null;
		}
		return network.getLeaders(numberOfLeaders).contains(id);
	}
	
	public boolean isHeadUp()
	{
		boolean wasUp = headIsUp;
		headIsUp = false;
		return wasUp;
	}
	
	public Set<T> getUpNodes()
	{
		return new TreeSet<>(pollReponders);
	}
	
	public Set<T> getNetwork()
	{
		return new TreeSet<>(network.getNetwork());
	}
	
	public void setChangeSubscriber(ChangeSubscriber subscriber)
	{
		this.subscriber = subscriber;
	}
	
	private void establishQueueConnection() throws IOException, TimeoutException
	{
		ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(queueHost);
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.exchangeDeclare(exchangeName, "topic");
        String queueName = channel.queueDeclare().getQueue();
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
        channel.basicConsume(queueName, true, consumer);
	}

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
				if (network.addNode(request.getRequestingID()))
					broadcastNetwork();
			break;
		}
	}
	
	private void addToNetwork(T sender, Set<T> toAdd, String resendRoutingKey)
	{
		//If we have anything the sending process is missing, send over what we have
		Boolean isHead = isHead();
		boolean rebroadcast = isHead != null && isHead && toAdd.addAll(network.getNetwork());
		
		network.addAllNodes(toAdd);
		if (isHead() && pollReponders != null)
		{
			pollReponders.add(sender);
		}
		
		if (subscriber != null)
			subscriber.handleChange();
		
		if (rebroadcast)
			broadcastNetwork(resendRoutingKey);
	}
	
	public void removeFromNetwork(Set<T> toRemove)
	{
		if (toRemove.contains(getHead()))
		{
			//If the head node is being remove, mark the new head as up
			//so that it has time to respond
			headIsUp = true;
		}
		if (network.removeAllNodes(toRemove) && subscriber != null)
			subscriber.handleChange();
	}
	
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
	
	public void broadcastNetwork()
	{
		broadcastNetwork(null);
	}
	
	private void broadcastNetwork(String resendRoutingKey)
	{
		try
		{
			Set<T> nodes;
			if (!network.isEmpty())
			{
				nodes = network.getNetwork();
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
