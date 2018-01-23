package jmace.LeaderElection.messages;

public class Request<T>
{
	private T requestingID;
	private RequestType type;
	private String data;
	
	public Request(T requestingID, RequestType type, String data)
	{
		this.requestingID = requestingID;
		this.type = type;
		this.data = data;
	}
	
	public T getRequestingID() {
		return requestingID;
	}

	public void setRequestingID(T requestingID) {
		this.requestingID = requestingID;
	}

	public RequestType getType() {
		return type;
	}

	public void setType(RequestType type) {
		this.type = type;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}
}
