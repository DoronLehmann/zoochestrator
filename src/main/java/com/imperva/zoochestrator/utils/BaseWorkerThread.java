package com.imperva.zoochestrator.utils;

import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;

import com.google.common.util.concurrent.FutureCallback;

public abstract class BaseWorkerThread extends Thread implements SelfMonitoringThread {

	protected static final ThreadLocal<BaseWorkerThread> current = new ThreadLocal<BaseWorkerThread>();

	public int index;
	public String status;
	
	protected long defaultLoopSleepMillis;
	
	private boolean baseWorkerInfoLogs;
	
	public BaseWorkerThread(String name, int id, int index) {
		super(name + "_" + id);
		this.index = index;
		this.baseWorkerInfoLogs = true;
		setDefaultUncaughtExceptionHandler( new UncaughtExceptionHandler() {
			@Override
			public void uncaughtException( Thread t, Throwable e ) {
				getLogger().error( "Uncaught exception by thread {}:", t.getName(), e );
			}
		});
		this.timers = new LinkedList<TimerEvent<?>>();
		this.checkpoint = new Checkpoint();
		this.messageQueue = new ConcurrentLinkedQueue<BaseWorkerThread.InterThreadMessage<?>>();	//TODO: consider a bounded queue here
		this.defaultLoopSleepMillis = 100L;
	}
	
	public BaseWorkerThread(String name, int id) {
		this(name, id, 1);
	}
	
	public BaseWorkerThread(String name, int id, boolean baseWorkerInfoLogs) {
		this(name, id, 1);
		this.baseWorkerInfoLogs = baseWorkerInfoLogs;
	}
	
	public static <T extends BaseWorkerThread> T current() {
		return (T) current.get();
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void run() {
		checkPoint("thread started");
		current.set(this);
		// reset timers times
		List<TimerEvent<?>> oldTimers = this.timers;
		this.timers = new LinkedList<TimerEvent<?>>();
		for ( TimerEvent timer : oldTimers ) {
			setTimer( timer.duration, timer.unit, timer.autoReschedule, timer.ctx, timer.callback );
		}
		this.running = true;
		loop();
		checkPoint("thread stopped");
	}
	
	protected abstract Logger getLogger();

	/**
	 * A default implementation of the loop method that utilizes all the features of the thread.
	 * Should be fine for most use-cases.
	 */
	protected void loop() {
		if ( baseWorkerInfoLogs ) {
			getLogger().info( "Worker starting" );
		}
		while (running) {
			if (!suspended) {
				triggerTimers();
				consumeMessages( -1 );
				unshelf();
			}
			checkPoint("Micro-sleep");
			baseSleep( defaultLoopSleepMillis, TimeUnit.MILLISECONDS );
		}
		if ( baseWorkerInfoLogs ) {
			getLogger().info( "Worker stopped" );
		}

	}

	/*************** States *****************/
	protected volatile boolean running;
	protected volatile boolean suspended;
	
	public void baseSleep(long duration , TimeUnit unit) {
		try {
			sleep(unit.toMillis(  duration ) );
		}
		catch ( InterruptedException e ) {
			// IGNORE
		}
	}
	
	public void baseStop() {
		this.running = false;
		if ( this.getState() == State.TIMED_WAITING || this.getState() == State.WAITING ) {
			this.interrupt();
		}
	}

	public void baseSuspend() {
		this.suspended = true;
	}
	
	public void baseJoin() {
		try {
			join();
		}
		catch ( InterruptedException e ) {
			// IGNORE
		}
	}
	
	/*************** Checkpoints ************/
	public static class Checkpoint {
		public String label;
		public long reportedAt;
		public State threadState;
		public StackTraceElement[] stackTrace;
	}

	protected Checkpoint checkpoint;

	public Checkpoint getCheckpoint() {
		Checkpoint cp;
		if (checkpoint != null) {
			cp = checkpoint;
		}
		else {
			cp = new Checkpoint();
			cp.reportedAt = System.currentTimeMillis();
			cp.label = "N/A";
		}
		cp.stackTrace = this.getStackTrace();
		return cp;
	}

	protected void checkPoint() {
		checkPoint(null);
	}
	
	protected void checkPoint(String label, int iteration) {
		checkPoint(label + ":" + iteration);
	}
	
	protected void checkPoint(String label) {
		checkpoint.label = label;
		checkpoint.reportedAt = System.currentTimeMillis();
		getLogger().trace( "CP: {}", label );
	}
	
	/*************** Timers *****************/
	public static class TimerEvent<TIMER_CTX> {
		public long duration;
		public TimeUnit unit;
		public long fireAt;
		public TIMER_CTX ctx;
		public TimerCallback<TIMER_CTX> callback;
		public boolean autoReschedule;
	}
	
	public interface TimerCallback<TIMER_CTX> {
		public void handle(TIMER_CTX ctx);
	}

	protected List<TimerEvent<?>> timers;
	
	protected void triggerTimers() {
		// List the timers that need to run by ascending time
		Set<TimerEvent<?>> timersToRun = new TreeSet<TimerEvent<?>>(new Comparator<TimerEvent<?>>() {
			@Override
			public int compare( TimerEvent<?> o1, TimerEvent<?> o2 ) {
				return (int)(o1.fireAt - o2.fireAt);
			}
		});
		Iterator<TimerEvent<?>> iterator = timers.iterator();
		while (iterator.hasNext()) {
			TimerEvent<?> timer = iterator.next();
			if ( System.currentTimeMillis() >= timer.fireAt ) {
				timersToRun.add( timer );
				if (!timer.autoReschedule) {
					iterator.remove();
				}
			}
		}
		// Run timers
		for (TimerEvent timer : timersToRun) {
			try {
				timer.callback.handle( timer.ctx );
				timer.fireAt = System.currentTimeMillis() + timer.unit.toMillis( timer.duration );
			}
			catch (Exception e) {
				getLogger().error("Error while triggering timer callback", e);
			}
		}
	}
	
	protected <TIMER_CTX> void setTimer(long duration, 
									 TimeUnit unit, 
									 boolean autoReschedule, 
									 TIMER_CTX ctx, 
									 TimerCallback<TIMER_CTX> callback) 
	{
		TimerEvent<TIMER_CTX> timer = new TimerEvent<TIMER_CTX>();
		timer.duration = duration;
		timer.unit = unit;
		timer.fireAt = System.currentTimeMillis() + unit.toMillis( duration );
		timer.autoReschedule = autoReschedule;
		timer.ctx = ctx;
		timer.callback = callback;
		timers.add( timer );
	}
	
	/*************** Messaging *****************/
	private static class InterThreadMessage<P> {
		
		P payload;
		MessageCallback<P> handler;
		BaseWorkerFuture<P> future;
	
		public InterThreadMessage(P payload, MessageCallback<P> handler) {
			this.payload = payload;
			this.handler = handler;
		}
		
	}
	
	public interface MessageCallback<P> {
		public void handle(P payload);
	}
	
	private Queue<InterThreadMessage<?>> messageQueue;
	
	/**
	 * Sends a message and expect a result
	 */
	public <P> Future<P> sendWithResult(P payload, MessageCallback<P> handler) {
		InterThreadMessage<P> msg = new InterThreadMessage<P>(payload, handler);
		msg.future = new BaseWorkerFuture<P>(payload);
		messageQueue.offer( msg );
		return msg.future;
	}

	/**
	 * Sends a message without a result
	 */
	public <P> void send(P payload, MessageCallback<P> handler) {
		InterThreadMessage<P> msg = new InterThreadMessage<P>(payload, handler);
		msg.future = new BaseWorkerFuture<P>(payload);
		messageQueue.offer( msg );
	}
	
	protected void consumeMessages(int limit) {
		int count = 0;
		while ((count < limit || limit < 0) && !messageQueue.isEmpty()) {
			InterThreadMessage msg = messageQueue.poll();
			if (msg.future != null) {
				try {
					if ( msg.future.timeoutAt > -1 && msg.future.timeoutAt < System.currentTimeMillis() ) {
						msg.future.timedOut = true;
					}
					else if (!msg.future.isCancelled) {
						// We can run the task
						msg.handler.handle( msg.payload );
					}
					count++;
				}
				catch (Exception e) {
					// The task failed
					msg.future.exception = e;
				}
				// In any case, this task is done
				msg.future.isDone = true;
				synchronized (msg.future) {
					msg.future.notifyAll();
				}
			}
			else {
				try {
					msg.handler.handle( msg.payload );
					count++;
				}
				catch (Exception e) {
					getLogger().error("Error processing message", e);
				}
			}
		}
	}
	
	private static class BaseWorkerFuture<T> implements Future<T> {

		long submittedAt;
		long timeoutAt = -1;
		volatile boolean isDone;
		volatile boolean isCancelled;
		volatile boolean timedOut;
		T result;
		Exception exception;
		
		BaseWorkerFuture(T result) {
			this.result = result;
		}
		
		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			this.isCancelled = true;
			return false;
		}

		@Override
		public boolean isCancelled() {
			return isCancelled;
		}

		@Override
		public boolean isDone() {
			return isDone;
		}

		@Override
		public T get() throws InterruptedException, ExecutionException {
			this.submittedAt = System.currentTimeMillis();
			if (!isDone) {
				synchronized (this) {
					while (!isDone) {
						wait();
					}
				}
			}
			if (exception == null) {
				return result;
			}
			else {
				throw new ExecutionException(exception);
			}
		}

		@Override
		public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
			this.submittedAt = System.currentTimeMillis();
			this.timeoutAt = submittedAt + unit.toMillis(timeout);
			if (!isDone) {
				synchronized (this) {
					while (!isDone) {
						wait();
					}
				}
			}
			if (timedOut) {
				throw new TimeoutException();
			}
			if (exception == null) {
				return result;
			}
			else {
				throw new ExecutionException(exception);
			}
		}
		
	}
	
	/*************** Task Shelving *****************/
	private static class ShelvedTask<T> {
		Future<T> future;
		FutureCallback<T> callback;
	}
	
	private List<ShelvedTask> shelved = new LinkedList<ShelvedTask>();
	
	public <T> void shelf(Future<T> future, FutureCallback<T> callback) {
		ShelvedTask<T> t = new ShelvedTask<T>();
		t.future = future;
		t.callback = callback;
		shelved.add(t);
	}
	
	protected void unshelf() {
		Iterator<ShelvedTask> iter = shelved.iterator();
		while (iter.hasNext()) {
			ShelvedTask task = iter.next();
			if (task.future.isDone()) {
				iter.remove();
				try {
					Object result = task.future.get();
					task.callback.onSuccess(result);
				} 
				catch (Exception e) {
					task.callback.onFailure(e);
				}
			}
		}
	}
	
	/*************** Monitoring *****************/
	
	public int stuckTimeThresholdMillis = Integer.parseInt( System.getProperty( "base.worker.stuck.threashold.millis", "600000" ) );
	
	public boolean isStuck() {
		Checkpoint currentCheckpoint = getCheckpoint();
		long delay = System.currentTimeMillis() - currentCheckpoint.reportedAt;
		boolean isStuck = delay > stuckTimeThresholdMillis;
		if ( isStuck && getLogger().isDebugEnabled() ) {
			getLogger().debug( getName() + " is stuck. Latest checkpoint is \"{}\" at {}", currentCheckpoint.label, currentCheckpoint.reportedAt );
			StringBuilder sb = new StringBuilder( "Current stack trace:\n");
			for ( StackTraceElement element : currentCheckpoint.stackTrace ) {
				sb.append( element.toString() ).append( "\n" );
			}
			getLogger().debug( sb.toString() );
		}
		return isStuck;
	}
	
	/**
	 * Hook for extending classes when the thread is detected to be stuck
	 * @param counter the number of consecutive times that the thread has been detected to be stuck. For example, if stuckTimeThresholdMillis equals 10 minutes, and the monitoring thread 
	 * runs every 1 minutes - the method will be called for the first time after 10 minutes with counter = 1, the next minute with counter = 2 and so on.
	 * If the thread is freed up the counter is zeroed.
	 */
	public void onStuckDetectionCallback( int counter ) {

	}
	
	/**
	 * Hook for extending classes when the thread is detected to be get freed up from being stuck
	 */
	public void onFreedUpCallback() {

	}
}


