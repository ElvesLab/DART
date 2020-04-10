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
 * Created on Apr 6, 2005
 */
package rice.environment.time;

import rice.Destructable;

/**
 * Virtualized clock for FreePastry.  
 * 
 * Can return the current time, or be blocked on.
 * 
 * Usually acquired by calling environment.getTimeSource().
 * 
 * TODO: add wait(lock, timeout) that is the same a s lock.wait(timeout)
 * 
 * @author Jeff Hoye
 */
public interface TimeSource extends Destructable {
  /**
   * @return the current time in millis
   */
  public long currentTimeMillis();
  
  /**
   * block for this many millis
   * 
   * @param delay the amount of time to sleep
   * @throws InterruptedException 
   */
  public void sleep(long delay) throws InterruptedException;

  /**
   * This method has the same syntax as lock.wait(timeToWait), but works in the simulator.
   * 
   * You need to be holding the lock on lock before calling this.
   * You may not call this on the SelectorThread
   *  
   * @param lock
   * @param timeToWait
   * @throws InterruptedException 
   */
//  public void wait(Object lock, int timeToWait) throws InterruptedException;
}
