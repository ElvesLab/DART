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

import org.mpisws.p2p.transport.liveness.LivenessProvider;

import rice.environment.Environment;
import rice.environment.random.RandomSource;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.CancellableTask;

public interface GenericNetworkSimulator<Identifier, MessageType> extends LivenessProvider<Identifier> {

  public Environment getEnvironment();
  /**
   * Get the environment related to a specific node.
   * @param i
   * @return
   */
  public Environment getEnvironment(Identifier i);

  /**
   * Determines delivery time from a to b.
   * 
   * @param a a node id.
   * @param b another node id.
   * 
   * @return delay of b from a.
   */
  public float networkDelay(Identifier a, Identifier b);

  
  /**
   * Deliver message.
   * 
   * @param msg message to deliver.
   * @param node the Pastry node to deliver it to.
   * @param how long to delay to deliver the message
   */
  public Cancellable deliverMessage(MessageType msg, Identifier to, Identifier from, int delay);

  /**
   * Deliver message.
   * 
   * @param msg message to deliver.
   * @param node the Pastry node to deliver it to.
   * @param how long to delay to deliver the message
   * @param period to deliver the message after the delay
   */
//  public CancellableTask enqueueDelivery(Delivery del);  
  public CancellableTask enqueueDelivery(Delivery del, int delay);
   
  public DirectTransportLayer<Identifier, MessageType> getTL(Identifier i);

  public boolean isAlive(Identifier i);
  
  /**
   * Kill identifier.
   * @param i
   */
  public void remove(Identifier i);
  

  public void start();
  
  public void stop();

  /**
   * The max rate of the simulator compared to realtime. 
   * 
   * The rule is that the simulated clock will not be set to a value greater 
   * than the factor from system-time that the call was made.  Thus
   * 
   * if 1 hour ago, you said the simulator should run at 10x realtime the simulated
   * clock will only have advanced 10 hours.  
   * 
   * Note that if the simulator cannot keep up with the system clock in the early 
   * part, it may move faster than the value you set to "catch up" 
   * 
   * To prevent this speed-up from becoming unbounded, you may wish to call
   * setMaxSpeed() periodically or immediately after periods of expensive calculations.
   * 
   * Setting the simulation speed to zero will not pause the simulation, you must 
   * call stop() to do that.
   * 
   * @param the multiple on realtime that the simulator is allowed to run at, 
   * zero or less will cause no bound on the simulation speed
   * 
   */
  public void setMaxSpeed(float rate);

  /**
   * unlimited maxSpeed
   *
   */
  public void setFullSpeed();

  public RandomSource getRandomSource();

}
