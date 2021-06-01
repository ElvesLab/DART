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
 * Created on Aug 9, 2005
 */
package rice.environment.processing.simple;

import rice.*;
import rice.environment.logging.*;
import rice.environment.time.TimeSource;
import rice.p2p.commonapi.Cancellable;
import rice.selector.SelectorManager;

/**
 * Scheduling with a lower priority number will be executed first (is higher Priority)
 * 
 * @author Jeff Hoye
 */
@SuppressWarnings("unchecked")
public class ProcessingRequest implements Runnable,
    Comparable<ProcessingRequest>, Cancellable {
  Continuation c;
  Executable r;
  private boolean cancelled = false;
  private boolean running = false;
  
  TimeSource timeSource;
  SelectorManager selectorManager;
  Logger logger;
  int priority = 0;
  long seq;

  public ProcessingRequest(Executable r, Continuation c, int priority, long seq,
      LogManager logging, TimeSource timeSource, SelectorManager selectorManager) {
    this.r = r;
    this.c = c;

    logger = logging.getLogger(getClass(), null);
    this.timeSource = timeSource;
    this.selectorManager = selectorManager;
    this.priority = priority;
    this.seq = seq;
  }

  public void returnResult(Object o) {
    c.receiveResult(o);
  }

  public void returnError(Exception e) {
    c.receiveException(e);
  }

  public int getPriority() {
    return priority;
  }

  public int compareTo(ProcessingRequest request) {
    if (priority == request.getPriority()) {
      if (seq > request.seq) return 1;
      return -1;
    }
    if (priority > request.getPriority()) return 1;
    return -1;
  }

  public void run() {
    if (cancelled) return;
    running = true;
    if (logger.level <= Logger.FINER)
      logger.log("COUNT: Starting execution of " + this);
    try {
      long start = timeSource.currentTimeMillis();
      final Object result = r.execute();
      if (logger.level <= Logger.FINEST)
        logger.log("QT: " + (timeSource.currentTimeMillis() - start) + " "
            + r.toString());

      selectorManager.invoke(new Runnable() {
        public void run() {
          returnResult(result);
        }

        public String toString() {
          return "return ProcessingRequest for " + r + " to " + c;
        }
      });
    } catch (final Exception e) {
      selectorManager.invoke(new Runnable() {
        public void run() {
          returnError(e);
        }

        public String toString() {
          return "return ProcessingRequest for " + r + " to " + c;
        }
      });
    }
    if (logger.level <= Logger.FINER)
      logger.log("COUNT: Done execution of " + this);
  }

  public boolean cancel() {
    cancelled = true;
    return !running;
  }
}
