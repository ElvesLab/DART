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
 * Created on May 6, 2004
 *
 * To change the template for this generated file go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package rice.pastry;


import rice.pastry.messaging.Message;
import rice.selector.Timer;
import rice.selector.TimerTask;

/**
 * @author jeffh
 *
 * To change the template for this generated type comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
public class ExponentialBackoffScheduledMessage extends ScheduledMessage {
  boolean cancelled = false;
  EBTimerTask myTask;
  Timer timer;
  long initialPeriod;
  double expBase;
  int numTimes = 0;
  long lastTime = 0;
  long maxTime = -1;

  /**
   * @param node
   * @param msg
   * @param initialPeriod
   * @param expBase
   */
  public ExponentialBackoffScheduledMessage(PastryNode node, Message msg, Timer timer, long initialDelay, long initialPeriod, double expBase, long maxPeriod) {
    super(node,msg);
    this.timer = timer;
    this.initialPeriod = initialPeriod;
    this.expBase = expBase;
    this.maxTime = maxPeriod;
    schedule(initialDelay);
  }

  public ExponentialBackoffScheduledMessage(PastryNode node, Message msg, Timer timer, long initialDelay, double expBase) {
    this(node, msg, timer, initialDelay, initialDelay, expBase, -1);
//    super(node,msg);
//    this.timer = timer;
//    this.initialPeriod = initialDelay;
//    this.expBase = expBase;
//    schedule(initialDelay);
    numTimes=1;
  }

  
  private void schedule(long time) {
    myTask = new EBTimerTask();
    timer.schedule(myTask,time);          
  }
  
  public boolean cancel() {
    super.cancel();
    if (myTask!=null) {
      myTask.cancel();
      myTask = null;
    }
    boolean temp = cancelled;
    cancelled = true;
    return temp;
  }
  
  public void run() {
    if (!cancelled) {
      if (myTask!=null) {
        lastTime = myTask.scheduledExecutionTime();
      }
      super.run();
      long time = (long)(initialPeriod * Math.pow(expBase,numTimes));
      if (maxTime >= 0) {
        time = Math.min(time, maxTime);
      }
      schedule(time);
      numTimes++;
    }
  }
  
  public long scheduledExecutionTime() {
    return lastTime;        
  }
  
  class EBTimerTask extends TimerTask {
    public void run() {
      ExponentialBackoffScheduledMessage.this.run();
    }
  }
}
