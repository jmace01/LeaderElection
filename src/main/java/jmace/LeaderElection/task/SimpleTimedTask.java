package jmace.LeaderElection.task;

public class SimpleTimedTask extends Thread
{
	private boolean isDead;
	private final long time;
	private final SimpleTimedTaskExecution exec;
	
	public SimpleTimedTask(long time, SimpleTimedTaskExecution exec)
	{
		this.isDead = false;
		this.time = time;
		this.exec = exec;
	}
	
	public void run()
	{
		do
		{
			try
			{
				Thread.sleep(time);
				if (!isDead)
				{
					exec.run();
				}
			}
			catch (Exception e)
			{
			}
		}
		while (!isDead);
	}
	
	public void restart()
	{
		super.interrupt();
	}
	
	public void interrupt()
	{
		isDead = true;
		super.interrupt();
	}
	
	public interface SimpleTimedTaskExecution
	{
		public void run();
	}
}
