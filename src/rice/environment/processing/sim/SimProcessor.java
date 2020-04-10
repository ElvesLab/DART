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
 * Created on Feb 6, 2006
 */
package rice.environment.processing.sim;

import rice.*;
import rice.environment.logging.LogManager;
import rice.environment.processing.*;
import rice.environment.processing.simple.ProcessingRequest;
import rice.environment.time.TimeSource;
import rice.p2p.commonapi.Cancellable;
import rice.selector.SelectorManager;

public class SimProcessor implements Processor {
  SelectorManager selector;

  public SimProcessor(SelectorManager selector) {
    this.selector = selector;
  }

  public <R, E extends Exception> Cancellable process(Executable<R,E> task, Continuation<R, E> command,
      SelectorManager selector, TimeSource ts, LogManager log) {
    return process(task, command, 0, selector, ts, log);
  }

  public <R, E extends Exception> Cancellable process(Executable<R,E> task, Continuation<R, E> command, int priority,
      SelectorManager selector, TimeSource ts, LogManager log) {
    ProcessingRequest ret = new ProcessingRequest(task, command, 0, 0, log, ts, selector);
    selector.invoke(ret);
    return ret;
  }

  public Cancellable processBlockingIO(WorkRequest request) {
    selector.invoke(request);
    return request;
  }

  public void destroy() {
    // TODO Auto-generated method stub

  }

}
