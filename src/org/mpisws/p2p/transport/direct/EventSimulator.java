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
package org.mpisws.p2p.transport.direct;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.environment.random.RandomSource;
import rice.environment.time.TimeSource;
import rice.environment.time.simulated.DirectTimeSource;
import rice.selector.SelectorManager;

public class EventSimulator {

  protected Environment environment;

  protected RandomSource random;
  
  protected SelectorManager manager;

  // true if we are responsible for incrementing the time
  private boolean isDirectTimeSource = false;
  protected TimeSource timeSource;

  protected Logger logger;

  public EventSimulator(Environment env, RandomSource random, Logger logger) {
    this.environment = env;
    this.random = random;
    this.logger = logger;
    manager = environment.getSelectorManager();
    
    timeSource = env.getTimeSource();
    if (timeSource instanceof DirectTimeSource)
      isDirectTimeSource = true;
    manager.setSelect(false);

 
  }
  
  
  // System Time is the system clock
  // Sim Time is the simulated clock
  long maxSpeedRequestSystemTime = 0;
  long maxSpeedRequestSimTime = 0;
  float maxSpeed = 0.0f;
  
  /**
   * This is a guardian for printing the "Invalid TimeSource" warning.  So that it is only printed once.
   */
  boolean printedDirectTimeSourceWarning = false;
  
  public void setMaxSpeed(float speed) {
    if (!isDirectTimeSource) {
      if (!printedDirectTimeSourceWarning) {
        if (logger.level <= Logger.WARNING) logger.log("Invalid TimeSource for setMaxSpeed()/setFullSpeed().  Use Environment.directEnvironment() to construct your Environment.");
        printedDirectTimeSourceWarning = true;
      }
    }
    maxSpeedRequestSystemTime = System.currentTimeMillis();
    maxSpeedRequestSimTime = timeSource.currentTimeMillis();
    maxSpeed = speed;
  }
  
  public void setFullSpeed() {
    setMaxSpeed(-1.0f);
  }
  
  /**
   * Delivers 1 message. Will advance the clock if necessary.
   * 
   * If there is a message in the queue, deliver that and return true. If there
   * is a message in the taskQueue, update the clock if necessary, deliver that,
   * then return true. If both are empty, return false;
   */  
  protected boolean simulate() throws InterruptedException {
    if (!isDirectTimeSource) return true;
    if (!environment.getSelectorManager().isSelectorThread()) throw new RuntimeException("Must be on selector thread");
    synchronized(manager) { // so we can wait on it, and so the clock and nextExecution don't change
      
      long scheduledExecutionTime = manager.getNextTaskExecutionTime();
      if (scheduledExecutionTime < 0) {
        if (logger.level <= Logger.FINE) logger.log("taskQueue is empty");
        return false;
      }
      
      if (scheduledExecutionTime > timeSource.currentTimeMillis()) {
        long newSimTime = scheduledExecutionTime;
        if (maxSpeed > 0) {
          long sysTime = System.currentTimeMillis();
          long sysTimeDiff = sysTime-maxSpeedRequestSystemTime;
          
          long maxSimTime = (long)(maxSpeedRequestSimTime+(sysTimeDiff*maxSpeed));
          
          if (maxSimTime < newSimTime) {
            // we need to throttle
            long neededSysDelay = (long)((newSimTime-maxSimTime)/maxSpeed);
//            logger.log("Waiting for "+neededSysDelay+" at "+timeSource.currentTimeMillis());
            if (neededSysDelay >= 1) {
              manager.wait(neededSysDelay);          
              long now = System.currentTimeMillis();
              long delay = now-sysTime;
  //            System.out.println("Woke up after "+delay);
              if (delay < neededSysDelay) return true;
            }
          }
        }
          
        if (logger.level <= Logger.FINER) logger.log("the time is now "+newSimTime);              
        ((DirectTimeSource)timeSource).setTime(newSimTime);      
      }
    } // synchronized(manager)
    
//    TimerTask task;
//    synchronized(taskQueue) {
//      // take a task from the taskQueue
//      if (taskQueue.isEmpty()) {
//        if (logger.level <= Logger.FINE) logger.log("taskQueue is empty");
//        return false;
//      }
//      task = (TimerTask) taskQueue.first();
//      if (logger.level <= Logger.FINE) logger.log("simulate():"+task);
//      taskQueue.remove(task);
//    }
//      // increment the clock if needed
//      if (task.scheduledExecutionTime() > timeSource.currentTimeMillis()) {
//        if (logger.level <= Logger.FINER) logger.log("the time is now "+task.scheduledExecutionTime());        
//        timeSource.setTime(task.scheduledExecutionTime());
//      }
//  
//  
//      if (task.execute(timeSource)) {
//        addTask(task);
//      }    
      
      
      return true;
  }

  boolean running = false; // Invariant: only modified on the selector

  public void start() {
    // this makes things single threaded
    manager.invoke(new Runnable() {      
      public void run() {
        if (running) return;
        running = true;
        manager.invoke(new Runnable() {    
          public void run() {
//            logger.log("EventSimulator.run()");
            if (!running) return;            
            try {
            if (!simulate()) {
              synchronized(manager) {
                try {
                  manager.wait(100); // must wait on the real clock, because the simulated clock can only be advanced by simulate()
                } catch (InterruptedException ie) {
                  logger.logException("BasicNetworkSimulator interrupted.",ie); 
                }
              }
            }
            // re-invoke the simulation task
            manager.invoke(this);
            } catch (InterruptedException ie) {
              if (logger.level <= Logger.SEVERE) logger.logException("BasicNetworkSimulator.start()",ie); 
              stop();
            }
          }
        });
      }
    });
  }
    
  public void stop() {
    manager.invoke(new Runnable() {      
      public void run() {
        running = false;
      }
    });
  }
  

}
