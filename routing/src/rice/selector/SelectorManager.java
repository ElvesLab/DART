/*******************************************************************************

"FreePastry" Peer-to-Peer Application Development Substrate

Copyright 2002-2007, Rice University. Copyright 2006-2007, Max Planck Institute 
for Software Systems.  All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

- Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

- Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

- Neither the name of Rice  University (RICE), Max Planck Institute for Software 
Systems (MPI-SWS) nor the names of its contributors may be used to endorse or 
promote products derived from this software without specific prior written 
permission.

This software is provided by RICE, MPI-SWS and the contributors on an "as is" 
basis, without any representations or warranties of any kind, express or implied 
including, but not limited to, representations or warranties of 
non-infringement, merchantability or fitness for a particular purpose. In no 
event shall RICE, MPI-SWS or contributors be liable for any direct, indirect, 
incidental, special, exemplary, or consequential damages (including, but not 
limited to, procurement of substitute goods or services; loss of use, data, or 
profits; or business interruption) however caused and on any theory of 
liability, whether in contract, strict liability, or tort (including negligence
or otherwise) arising in any way out of the use of this software, even if 
advised of the possibility of such damage.

*******************************************************************************/ 
package rice.selector;

import java.io.IOException;
import java.nio.channels.*;
import java.util.*;

import rice.Destructable;
import rice.environment.Environment;
import rice.environment.logging.*;
import rice.environment.random.RandomSource;
import rice.environment.random.simple.SimpleRandomSource;
import rice.environment.time.TimeSource;
import rice.environment.time.simulated.DirectTimeSource;

/**
 * This class is the class which handles the selector, and listens for activity.
 * When activity occurs, it figures out who is interested in what has happened,
 * and hands off to that object.
 * 
 * @version $Id: SelectorManager.java 4675 2009-03-11 15:54:34Z jeffh $
 * @author Alan Mislove
 */
public class SelectorManager extends Thread implements Timer, Destructable {

  // the maximal time to sleep on a select operation
  public static int TIMEOUT = 500;

  // the underlying selector used
  protected Selector selector;

  // a list of the invocations that need to be done in this thread
  protected LinkedList<Runnable> invocations;

  // the list of handlers which want to change their key
  protected HashSet<SelectionKey> modifyKeys;

  // the list of keys waiting to be cancelled
  protected HashSet<SelectionKey> cancelledKeys;

  // the set used to store the timer events
//  protected TreeSet timerQueue = new TreeSet();
  protected Queue<TimerTask> timerQueue = new PriorityQueue<TimerTask>();

  // the next time the selector is scheduled to wake up
  protected long wakeupTime = 0;

  protected TimeSource timeSource;

  long lastTime = 0;

//  protected LogManager log;
  protected Logger logger;
  
  protected String instance;

  protected boolean running = true;
  
  /**
   * Can be disabled for the simulator to improve performance, only do this
   * if you know you don't need to select on anything
   */
  protected boolean select = true;
  
  protected RandomSource random;
  
  /**
   * Constructor, which is private since there is only one selector per JVM.
   */
  public SelectorManager(String instance,
      TimeSource timeSource, LogManager log, RandomSource random) {    
    super(instance == null ? "Selector Thread" : "Selector Thread -- "
        + instance);
    this.random = random;
    if (this.random == null) this.random = new SimpleRandomSource(log);
    this.instance = instance;
    this.logger = log.getLogger(getClass(), instance);
    this.invocations = new LinkedList<Runnable>();
    this.modifyKeys = new HashSet<SelectionKey>();
    this.cancelledKeys = new HashSet<SelectionKey>();
    this.timeSource = timeSource;

    // attempt to create selector
    try {
      selector = Selector.open();
    } catch (IOException e) {
      System.out
          .println("SEVERE ERROR (SelectorManager): Error creating selector "
              + e);
    }
    lastTime = timeSource.currentTimeMillis();
  }

  public SelectorManager(String instance,
      TimeSource timeSource, LogManager log) {    
    this(instance,timeSource,log,new SimpleRandomSource(log));
  }
  
  protected Environment environment;
  
  /**
   * Can only be called once.  The rest of the time the argument is rejected.
   * This is needed so that when the NodeFactory clones the Environment, it doesn't
   * get restarted/have multiple different environments etc...
   * 
   * @param env
   */
  public void setEnvironment(Environment env) {
    if (env == null) throw new IllegalArgumentException("env is null!");
    if (environment != null) return;
    environment = env;
    environment.addDestructable(this);
    start();
  }
  
  /**
   * Method which asks the Selector Manager to add the given key to the
   * cancelled set. If no one calls register on this key during the rest of this
   * select() operation, the key will be cancelled. Otherwise, it will be
   * returned as a result of the register operation.
   * 
   * @param key The key to cancel
   */
  public void cancel(SelectionKey key) {
    if (key == null)
      throw new NullPointerException();

    cancelledKeys.add(key);
  }

  /**
   * Utility method which returns the SelectionKey attached to the given
   * channel, if one exists
   * 
   * @param channel The channel to return the key for
   * @return The key
   */
  public SelectionKey getKey(SelectableChannel channel) {
    return channel.keyFor(selector);
  }

  /**
   * Registers a new channel with the selector, and attaches the given
   * SelectionKeyHandler as the handler for the newly created key. Operations
   * which the hanlder is interested in will be called as available.
   * 
   * @param channel The channel to regster with the selector
   * @param handler The handler to use for the callbacks
   * @param ops The initial interest operations
   * @return The SelectionKey which uniquely identifies this channel
   */
  public SelectionKey register(SelectableChannel channel,
      SelectionKeyHandler handler, int ops) throws IOException {
    if ((channel == null) || (handler == null))
      throw new NullPointerException();

    SelectionKey key = channel.register(selector, ops, handler);
    if (cancelledKeys != null) cancelledKeys.remove(key);

    return key;
  }
  
  /**
   * This method schedules a runnable task to be done by the selector thread
   * during the next select() call. All operations which modify the selector
   * should be done using this method, as they must be done in the selector
   * thread.
   * 
   * @param d The runnable task to invoke
   */
  public synchronized void invoke(Runnable d) {
    if (d == null)
      throw new NullPointerException();
    if (invocations == null) return;
    invocations.add(d);
    wakeup();
  }

  /**
   * Debug method which returns the number of pending invocations
   * 
   * @return The number of pending invocations
   */
  public int getNumInvocations() {
    return invocations.size();
  }

  /**
   * Adds a selectionkey handler into the list of handlers which wish to change
   * their keys. Thus, modifyKeys() will be called on the next selection
   * operation
   * 
   * @param key The key which is to be changed
   */
  public synchronized void modifyKey(SelectionKey key) {
    if (key == null)
      throw new NullPointerException();

    modifyKeys.add(key);
    wakeup();
  }

  /**
   * This method is to be implemented by a subclass to do some task each loop.
   */
  protected void onLoop() {
  }

  /**
   * This method starts the socket manager listening for events. It is designed
   * to be started when this thread's start() method is invoked.
   */
  public void run() {
    //System.out.println("SelectorManager starting..."+Thread.currentThread());
    if (logger.level <= Logger.INFO) logger.log("SelectorManager -- " + instance + " starting...");

    lastTime = timeSource.currentTimeMillis();
    // loop while waiting for activity
    while (running) {
      try {
        if (useLoopListeners)
          notifyLoopListeners();

        // NOTE: This is so we aren't always holding the selector lock when we
        // get context switched
        Thread.yield();
        executeDueTasks();
        onLoop();
        doInvocations();
        if (select) {
          doSelections();
          int selectTime = SelectorManager.TIMEOUT;
          if (timerQueue.size() > 0) {
            TimerTask first = (TimerTask) timerQueue.peek();
            selectTime = (int) (first.scheduledExecutionTime() - timeSource
                .currentTimeMillis());
          }

          select(selectTime);
          
          if (cancelledKeys.size() > 0) {
            Iterator<SelectionKey> i = cancelledKeys.iterator();

            while (i.hasNext())
              ((SelectionKey) i.next()).cancel();

            cancelledKeys.clear();

            // now, hack to make sure that all cancelled keys are actually
            // cancelled (dumb)
            selector.selectNow();
          }
        } // if select
      } catch (Throwable t) {
        if (logger.level <= Logger.SEVERE) logger.logException(
            "ERROR (SelectorManager.run): " , t);
        environment.getExceptionStrategy().handleException(this, t);
//        System.exit(-1);
      }
    } // while(running)
    invocations.clear();
    loopObservers.clear();
    cancelledKeys.clear();
    timerQueue.clear();
    invocations = null;
    loopObservers = null;
    cancelledKeys = null;
    timerQueue = null;
    try {
      if (selector != null) {
        selector.close();
      }
    } catch (IOException ioe) {
      if (logger.level <= Logger.WARNING) logger.logException("Error cancelling selector:",ioe);
    }
    if (logger.level <= Logger.INFO) logger.log("Selector "+instance+" shutting down.");
  }
  
  @SuppressWarnings("deprecation")
  public void destroy() {
    if (logger.level <= Logger.FINE) {
      logger.logException("destroying SelectorManager", new Exception("Stack Trace"));
    } else if (logger.level <= Logger.INFO) {
      logger.log("destroying SelectorManager");
    }
    running = false; 
  }
  
  /**
   * LoopListeners is used in case you are worried that your process may not get scheduled for a while
   * such as on an overloaded planetlab node, or if you hibernate your laptop, this is not needed 
   * for the simulator
   */
  protected boolean useLoopListeners = true;
  
  /**
   * Set this to false when using the simulator, because you don't need to notify loop observers.
   * @param val
   */
  public void useLoopListeners(boolean val) {
    useLoopListeners = val;
  }
  
  protected void notifyLoopListeners() {
    long now = timeSource.currentTimeMillis();
    long diff = now - lastTime;
    // notify observers
    synchronized (loopObservers) {
      Iterator<LoopObserver> i = loopObservers.iterator();
      while (i.hasNext()) {
        LoopObserver lo = (LoopObserver) i.next();
        if (lo.delayInterest() <= diff) {
          lo.loopTime((int) diff);
        }
      }
    }
    lastTime = now;
  }

  ArrayList<LoopObserver> loopObservers = new ArrayList<LoopObserver>();

  public void addLoopObserver(LoopObserver lo) {
    synchronized (loopObservers) {
      loopObservers.add(lo);
    }
  }

  public void removeLoopObserver(LoopObserver lo) {
    synchronized (loopObservers) {
      loopObservers.remove(lo);
    }
  }

  protected void doSelections() throws IOException {
    SelectionKey[] keys = selectedKeys();
    
    // to debug weird selection bug
    if (keys.length > 1000 && logger.level <= Logger.FINE) {
      logger.log("lots of selection keys!");
      HashMap<String, Integer> histo = new HashMap<String, Integer>();
      for (int i = 0; i < keys.length; i++) {
        String keyclass = keys[i].getClass().getName();
        if (histo.containsKey(keyclass)) {
          histo.put(keyclass, new Integer(((Integer)histo.get(keyclass)).intValue() + 1));
        } else {
          histo.put(keyclass, new Integer(1));
        }
      }
      logger.log("begin selection keys by class");
      Iterator<String> it = histo.keySet().iterator();
      while (it.hasNext()) {
        String name = (String)it.next();
        logger.log("Selection Key: " + name + ": "+histo.get(name));
      }
      logger.log("end selection keys by class");
    }

    for (int i = 0; i < keys.length; i++) {
//      System.out.println("handling key "+keys[i].attachment());
      selector.selectedKeys().remove(keys[i]);      

      synchronized (keys[i]) {
        SelectionKeyHandler skh = (SelectionKeyHandler) keys[i].attachment();

        if (skh != null) {
          // accept
          if (keys[i].isValid() && keys[i].isAcceptable()) {
            skh.accept(keys[i]);
          }

          // connect
          if (keys[i].isValid() && keys[i].isConnectable()) {
            skh.connect(keys[i]);
          }

          // read
          if (keys[i].isValid() && keys[i].isReadable()) {
            skh.read(keys[i]);
          }

          // write
          if (keys[i].isValid() && keys[i].isWritable()) {
            skh.write(keys[i]);
          }
        } else {
          keys[i].channel().close();
          keys[i].cancel();
        }
      }
    }
  }

  /**
   * Method which invokes all pending invocations. This method should *only* be
   * called by the selector thread.
   */
  protected void doInvocations() {
    if (logger.level <= Logger.FINEST) logger.log("SM.doInvocations()");
    Iterator<Runnable> i;
    synchronized (this) {
      i = new ArrayList<Runnable>(invocations).iterator();
      invocations.clear();
    }

    while (i.hasNext()) {
      Runnable run = (Runnable) i.next();
      try {
        run.run();
      } catch (RuntimeException e) {
        // place the rest of the items back into invocations
        synchronized(this) {
          int ctr = 0;
          while(i.hasNext()) {
            invocations.add(ctr,i.next());
            ctr++;
          }
        }        
//        if (logger.level <= Logger.SEVERE) logger.logException( 
//            "Invoking runnable caused exception " + e + " - continuing",e);
        throw e;
      }
    }

    Iterator<SelectionKey> i2;
    synchronized (this) {
      i2 = new ArrayList<SelectionKey>(modifyKeys).iterator();
      modifyKeys.clear();
    }

    while (i2.hasNext()) {
      SelectionKey key = (SelectionKey) i2.next();
      if (key.isValid() && (key.attachment() != null))
        ((SelectionKeyHandler) key.attachment()).modifyKey(key);
    }
  }

  /**
   * Method which synchroniously returns the first element off of the
   * invocations list.
   * 
   * @return An item from the invocations list
   */
  protected synchronized Runnable getInvocation() {
    if (invocations.size() > 0)
      return (Runnable) invocations.removeFirst();
    else
      return null;
  }

  /**
   * Method which synchroniously returns on element off of the modifyKeys list
   * 
   * @return An item from the invocations list
   */
  protected synchronized SelectionKey getModifyKey() {
    if (modifyKeys.size() > 0) {
      Object result = modifyKeys.iterator().next();
      modifyKeys.remove(result);
      return (SelectionKey) result;
    } else {
      return null;
    }
  }

  /**
   * Selects on the selector, and returns the result. Also properly synchronizes
   * around the selector
   * 
   * @return DESCRIBE THE RETURN VALUE
   * @exception IOException DESCRIBE THE EXCEPTION
   */
  protected int select(int time) throws IOException {
    if (logger.level <= Logger.FINEST) logger.log("SM.select("+time+")");

    if (time > TIMEOUT)
      time = TIMEOUT;

    try {
      if ((time <= 0) || (invocations.size() > 0) || (modifyKeys.size() > 0))
        return selector.selectNow();

      wakeupTime = timeSource.currentTimeMillis() + time;
      return selector.select(time);
    } catch (CancelledKeyException cce) {
      if (logger.level <= Logger.WARNING) logger.logException("CCE: cause:",cce.getCause());
      throw cce;
    } catch (IOException e) {
      if (e.getMessage().indexOf("Interrupted system call") >= 0) {
        if (logger.level <= Logger.WARNING) logger.log("Got interrupted system call, continuing anyway...");
        return 1;
      } else {
        throw e;
      }
    }
  }

  /**
   * Selects all of the keys on the selector and returns the result as an array
   * of keys.
   * 
   * @return The array of keys
   * @exception IOException DESCRIBE THE EXCEPTION
   */
//  private SelectionKey[] keys() throws IOException {
//    return (SelectionKey[]) selector.keys().toArray(new SelectionKey[0]);
//  }

  /**
   * Selects all of the currently selected keys on the selector and returns the
   * result as an array of keys.
   * 
   * @return The array of keys
   * @exception IOException DESCRIBE THE EXCEPTION
   */
  protected SelectionKey[] selectedKeys() throws IOException {
    Set<SelectionKey> s = selector.selectedKeys();
    SelectionKey[] k = (SelectionKey[])s.toArray(new SelectionKey[0]);
    // randomize k
    for (int c = 0; c < k.length; c++) {
      SelectionKey temp = k[c];
      int swapIndex = random.nextInt(k.length);
      k[c] = k[swapIndex];
      k[swapIndex] = temp;
    }
    
    return k;
  }

  /**
   * Returns whether or not this thread of execution is the selector thread
   * 
   * @return Whether or not this is the selector thread
   */
  public boolean isSelectorThread() {
    return (Thread.currentThread() == this);
  }

  /**
   * Method which schedules a task to run after a specified number of millis.  The task must have a proper nextExecutionTime set
   * 
   * @param task The task to run
   */
  public TimerTask schedule(TimerTask task) {
    addTask(task);
    return task;
  }
  
  public TimerTask schedule(TimerTask task, long delay) {
    task.setNextExecutionTime(timeSource.currentTimeMillis() + delay);
    addTask(task);
    return task;
  }

  /**
   * Method which schedules a task to run repeatedly after a specified delay and
   * period
   * 
   * @param task The task to run
   * @param delay The delay before first running, in milliseconds
   * @param period The period with which to run in milliseconds
   */
  public TimerTask schedule(TimerTask task, long delay, long period) {
    task.setNextExecutionTime(timeSource.currentTimeMillis() + delay);
    task.period = (int) period;
    addTask(task);
    return task;
  }

  /**
   * Method which schedules a task to run repeatedly (at a fixed rate) after a
   * specified delay and period
   * 
   * @param task The task to run
   * @param delay The delay before first running in milliseconds
   * @param period The period with which to run in milliseconds
   */
  public TimerTask scheduleAtFixedRate(TimerTask task, long delay, long period) {
    task.setNextExecutionTime(timeSource.currentTimeMillis() + delay);
    task.period = (int) period;
    task.fixedRate = true;
    addTask(task);
    return task;
  }

  /**
   * Internal method which adds a task to the task tree, waking up the selector
   * if necessary to recalculate the sleep time
   * 
   * @param task The task to add
   */
  protected Object seqLock = new Object();
  protected int seqCtr = Integer.MIN_VALUE;
  protected synchronized void addTask(TimerTask task) {
    synchronized(seqLock) {
      task.seq = seqCtr++;
    }
//    if (logger.level <= Logger.FINE && task.scheduledExecutionTime() <= timeSource.currentTimeMillis()) {
//      logger.logException("Scheduling task for now:"+task, new Exception());
//    }
    if (logger.level <= Logger.FINER) logger.log("addTask("+task+") scheduled for "+task.scheduledExecutionTime());
//    synchronized (selector) {
      if (!timerQueue.add(task)) {
        if (logger.level <= Logger.WARNING) logger.log("ERROR: Got false while enqueueing task "+task+"!");
        Thread.dumpStack();
      } else {
        task.setSelectorManager(this); 
      }

    // need to interrupt thread if waiting too long in selector
    if (select) {
      // using the network
      if (wakeupTime >= task.scheduledExecutionTime()) {
        // we need to wake up the selector because it's going to sleep too long
        wakeup();
      }
    } else {
      // using the simulator
      if (task.scheduledExecutionTime() == getNextTaskExecutionTime()) {
        // we need to wake up the selector because we are now the newest 
        // shortest wait, and may be delaying because of a later event
        wakeup();
      }
    }
    
//    } // synchronized

  }
  
  public synchronized void removeTask(TimerTask task) {
    if (logger.level <= Logger.FINER) logger.log("removeTask("+task+") scheduled for "+task.scheduledExecutionTime());
    timerQueue.remove(task);
  }

  /**
   * Note, should hold the selector's (this) lock to call this.
   *
   */
  public void wakeup() {
    //System.out.println("wakeup()");
    selector.wakeup();
    this.notifyAll();
  }

  public long getNextTaskExecutionTime() {
//    if (!invocations.isEmpty()) return timeSource.currentTimeMillis();
    if (timerQueue.size() > 0) {
      TimerTask next = (TimerTask) timerQueue.peek();
      //System.out.println(next);
      return next.scheduledExecutionTime();
    }
    return -1;    
  }
  
  /**
   * Internal method which finds all due tasks and executes them.
   */
  protected void executeDueTasks() {
    long now = timeSource.currentTimeMillis();
    if (logger.level <= Logger.FINEST) logger.log("SM.executeDueTasks() "+now);
    
    ArrayList<TimerTask> executeNow = new ArrayList<TimerTask>();

    // step 1, fetch all due timers
    synchronized (this) {
      boolean done = false;
      while (!done) {
        if (timerQueue.size() > 0) {
          TimerTask next = (TimerTask) timerQueue.peek();
          if (next.scheduledExecutionTime() <= now) {
            executeNow.add(next);
            //System.out.println("Removing:"+next);
//            timerQueue.remove(next);
            timerQueue.poll();
          } else {
            done = true;
          }
        } else {
          done = true;
        }
      }
    }

    // step 2, execute them all
    // items to be added back into the queue
    ArrayList<TimerTask> addBack = new ArrayList<TimerTask>();
    Iterator<TimerTask> i = executeNow.iterator();
    while (i.hasNext()) {
      TimerTask next = i.next();
      try {
        if (logger.level <= Logger.FINER) logger.log("executing task "+next);
        
        if (executeTask(next)) {
          addBack.add(next);
        }
      } catch (RuntimeException e) {
        // add back the un-executed tasks before continuing
        while(i.hasNext()) {
          addBack.add(i.next());
        }
        synchronized (this) {
          i = addBack.iterator();
          while (i.hasNext()) {
            TimerTask tt = (TimerTask) i.next();
            //System.out.println("SM.addBack("+tt+")");
            timerQueue.add(tt);
          }
        }
        
//        if (logger.level <= Logger.SEVERE) logger.logException("",e);
        throw e;
      }
    }

    // step 3, add them back if necessary
    synchronized (this) {
      i = addBack.iterator();
      while (i.hasNext()) {
        TimerTask tt = (TimerTask) i.next();
        //System.out.println("SM.addBack("+tt+")");
        timerQueue.add(tt);
      }
    }
  }

  protected boolean executeTask(TimerTask next) {
    return next.execute(timeSource);
  }
  
  /**
   * Returns the timer associated with this SelectorManager (in this case, it is
   * this).
   * 
   * @return The associated timer
   */
  public Timer getTimer() {
    return this;
  }
  
  public Selector getSelector() {
    return selector; 
  }

  public void setSelect(boolean b) {
    select = b;
  }

  public Environment getEnvironment() {
    return environment;
  }

  public void setLogLevel(int level) {
    logger.level = level;
  }

  
  public static void main(String[] args) {
    new Environment();
    new Environment();
  }
}
