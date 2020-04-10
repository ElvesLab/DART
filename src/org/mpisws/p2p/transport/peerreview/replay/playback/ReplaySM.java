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
package org.mpisws.p2p.transport.peerreview.replay.playback;

import java.io.IOException;

import org.mpisws.p2p.transport.peerreview.replay.Verifier;


import rice.environment.Environment;
import rice.environment.logging.LogManager;
import rice.environment.logging.Logger;
import rice.environment.time.TimeSource;
import rice.environment.time.simulated.DirectTimeSource;
import rice.selector.SelectorManager;
import rice.selector.TimerTask;

/**
 * There are normally 3 kinds of events:
 *   Invokations
 *   TimerTasks
 *   Network I/O
 *   
 * The Network I/O should match exactly with our Log, and so we only have to pump Invokations   
 *   
 * @author Jeff Hoye
 *
 */
public class ReplaySM extends SelectorManager {
  Verifier verifier;
  DirectTimeSource simTime;
  
  public ReplaySM(String instance, DirectTimeSource timeSource, LogManager log) {
    super(instance, timeSource, log, null);
    this.simTime = timeSource;
    setSelect(false);
  }
  
  public void setVerifier(Verifier v) {
    this.verifier = v;
  }

  /**
   * Don't automatically start the thread.
   */
  public void setEnvironment(Environment env) {
    if (env == null) throw new IllegalArgumentException("env is null!");
    if (environment != null) return;
    environment = env;
//    start();
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

  public boolean makeProgress() {
    // Handle any pending timers. Note that we have to be sure to call them in the exact same
    // order as in the main code; otherwise there can be subtle bugs and side-effects. 
    if (logger.level <= Logger.FINER) logger.log("executeDueTasks()");

    if (isSuccess()) return false;
    boolean timerProgress = true;
    long now = verifier.getNextEventTime();
    while (timerProgress) {
      now = verifier.getNextEventTime();
      if (now == -1) return false;
//      timerProgress = false;
  
//     int best = -1;
//     for (int i=0; i<numTimers; i++) {
//       if ((timer[i].time <= now) && ((best<0) || (timer[i].time<timer[best].time) || ((timer[i].time==timer[best].time) && (timer[i].id<timer[best].id))))
//         best = i;
//     }
//  
//     if (best >= 0) {
//       int id = timer[best].id;
//       TimerCallback *callback = timer[best].callback;
//       now = timer[best].time;
//       timer[best] = timer[--numTimers];
//       vlog(2, "Verifier: Timer expired (#%d, now=%lld)", id, now);
//       callback->timerExpired(id);
//       timerProgress = true;
//     }
//   }
//
//    
      TimerTask next = null;
      
      synchronized (this) {
        if (timerQueue.size() > 0) {
          next = (TimerTask) timerQueue.peek();
          if (next.scheduledExecutionTime() <= now) {
            timerQueue.poll(); // remove the event
            simTime.setTime(next.scheduledExecutionTime()); // set the time            
          } else {
            timerProgress = false;
          }
        } else {
          timerProgress = false;
        }
      }
      
      if (timerProgress) {
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
    
    return verifier.makeProgress();
//    if (!verifier.makeProgress()) {
//      isSuccess();
////      if (!verifier.verifiedOK()) throw new RuntimeException("Verification failed.");
//    }
  }

  protected boolean isSuccess() {
    boolean ret = verifier.isSuccess();
    if (ret) {
      // success!
//      logger.log("success!");
      // TODO: do something different 
      environment.destroy();    
    }
    return ret;
  }
  
  
  @Override
  protected void doInvocations() {
    // do nothing, this is called in executeDueTasks
  }
}
