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
package org.mpisws.p2p.transport.peerreview.replay.record;

import rice.environment.logging.LogManager;
import rice.environment.logging.Logger;
import rice.environment.random.RandomSource;
import rice.environment.time.TimeSource;
import rice.environment.time.simulated.DirectTimeSource;
import rice.selector.SelectorManager;
import rice.selector.TimerTask;

/**
 * This is the SelectorManager for PeerReview.  The invariant here is that we use a simTime that isn't updated near as 
 * frequently as the real clock.  This makes the events more discrete for replay.
 * 
 * @author Jeff Hoye
 *
 */
public class RecordSM extends SelectorManager {
  DirectTimeSource simTime;
  TimeSource realTime;
  
  public RecordSM(String instance, TimeSource realTime, DirectTimeSource simTime, LogManager log, RandomSource rs) {
    super(instance, simTime, log, rs);
    this.realTime = realTime;
    this.simTime = simTime;
  }

  
  @Override
  protected synchronized void addTask(TimerTask task) {
    long now = timeSource.currentTimeMillis();
    if ((task.scheduledExecutionTime() < now) && (timeSource instanceof DirectTimeSource)) {
//      task.setNextExecutionTime(now);
      if (logger.level <= Logger.WARNING) logger.logException("Can't schedule a task in the past. "+task+" now:"+now+" task.execTime:"+task.scheduledExecutionTime(), new Exception("Stack Trace"));
      throw new RuntimeException("Can't schedule a task in the past.");
    }
    super.addTask(task);
  }
  
  @Override
  protected void executeDueTasks() {
    //System.out.println("SM.executeDueTasks()");
    long now = realTime.currentTimeMillis();
        
    boolean done = false;
    while (!done) {
      TimerTask next = null;
      synchronized (this) {
        if (timerQueue.size() > 0) {
          next = (TimerTask) timerQueue.peek();
          if (next.scheduledExecutionTime() <= now) {
            timerQueue.poll(); // remove the event
            simTime.setTime(next.scheduledExecutionTime()); // set the time
          } else {
            done = true;
          }
        } else {
          done = true;
        }
      } // sync
      
      if (!done) {
        super.doInvocations();
        if (logger.level <= Logger.FINE) logger.log("executing task "+next);
        if (next.execute(simTime)) { // execute the event
          synchronized(this) {
            timerQueue.add(next); // if the event needs to be rescheduled, add it back on
          }
        }
      }
    }
    simTime.setTime(now); // so we always make some progress
    super.doInvocations();
  }  
  
  @Override
  protected void doInvocations() {
    // do nothing, this is called in executeDueTasks
  }
}

