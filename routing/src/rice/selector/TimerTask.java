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
/*
 * Created on Nov 17, 2004
 */
package rice.selector;

import java.util.Queue;

import rice.environment.time.TimeSource;
import rice.p2p.commonapi.CancellableTask;

/**
 * @author Jeff Hoye
 */
public abstract class TimerTask implements Comparable<TimerTask>, CancellableTask {
  private long nextExecutionTime;
  protected boolean cancelled = false;
  protected int seq;
  protected SelectorManager selector;

  /**
   * If period is positive, task will be rescheduled.
   */
  protected int period = -1;    
    
  protected boolean fixedRate = false;
  
  public abstract void run();

  /**
   * Returns true if should re-insert.
   * @return
   */
  public boolean execute(TimeSource ts) {
    if (cancelled) return false;
    run();
    // often cancelled in the execution
    if (cancelled) return false;
    if (period > 0) {
      if (fixedRate) {
        nextExecutionTime+=period;
        return true;
      } else {
        nextExecutionTime = ts.currentTimeMillis()+period;
        return true;
      }
    } else {
      return false;
    }
  }
  
  public boolean cancel() {
    if (cancelled) {
      return false;
    }
    if (selector != null) {
      selector.removeTask(this); 
    }
    cancelled = true;
    return true;
  }
  
  public long scheduledExecutionTime() {
    return nextExecutionTime; 
  }

  public int compareTo(TimerTask arg0) {
    TimerTask tt = (TimerTask)arg0;
    if (tt == this) return 0;
//    return (int)(tt.nextExecutionTime-nextExecutionTime);
    int diff = (int)(nextExecutionTime-tt.nextExecutionTime);
    if (diff == 0) {
      // compare the sequence numbers
      diff = seq-tt.seq;
      
      // if still same, try the hashcode
      if (diff == 0) {      
        return System.identityHashCode(this) < System.identityHashCode(tt) ? 1 : -1;
      }
    }
    return diff;
  }

  public boolean isCancelled() {
    return cancelled;
  }
   
  /**
   * Makes the cancel operation remove the task from the queue as well.
   * 
   * @param timerQueue that we are enqueued on
   */
  public void setSelectorManager(SelectorManager selector) {
    this.selector = selector;
  }

  protected void setNextExecutionTime(long l) {
    nextExecutionTime = l;
  }
}
