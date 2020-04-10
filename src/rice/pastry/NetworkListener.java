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

package rice.pastry;

import java.net.*;

/**
 * Represents a listener to pastry network activity.  This interface will notify
 * you of opening/closing sockets, and all network traffic.
 * 
 * <p/>You hear about opening/closing sockets via <code>channelOpened()</code>/<code>channelClosed()</code> methods.
 * 
 * <p/>You hear about network input via <code>dataReceived()</code> and network output via
 * <code>dataSent()</code>.  
 *
 * <p/>channelOpened() has a reason for opening the chanel.  The reason falls under 
 * matrix of reasons:
 * <ul>
 *  <li>It may have been initiated locally, or be accepting of the socket from a
 *   remote node.  Reasons who's name has ACC in them are accepted, all of the 
 *   rest are initiated locally.</li>
 * 
 *  <li>It may be for a Normal connection, a source route (SR), a bootstrap (which 
 * just requests internal state from FreePastry) or for an application level socket.</li>
 * </ul>
 * 
 * <p/>There are currently the following limitations for NetworkListener regarding 
 * Application Sockets which may be fixed later:
 * <ol>
 *   <li>when accepting an application socket the reason is REASON_ACC_NORMAL</li>
 *   <li>you are not notified about data to/from these sockets, only that they 
 *   have been opened.</li> 
 * </ol>
 * 
 * <p/>the <code>dataSent()</code>/<code>dataReceived()</code> methods include a wireType.  These tell you if the 
 * data was TCP/UDP and wether it was part of the SourceRoute
 *
 * @version $Id: NetworkListener.java 3613 2007-02-15 14:45:14Z jstewart $
 *
 * @author Jeff Hoye
 */
public interface NetworkListener {
  
  /**
   * TCP traffic (not source-routed)
   */
  public static int TYPE_TCP    = 0x00;
  /**
   * UDP traffic (not source-routed)
   */
  public static int TYPE_UDP    = 0x01;
  /**
   * TCP traffic source-routed (you are an intermediate hop, not the source nor destination)
   */
  public static int TYPE_SR_TCP = 0x10;
  /**
   * UDP traffic source-routed (you are an intermediate hop, not the source nor destination)
   */
  public static int TYPE_SR_UDP = 0x11;
  
  /**
   * Local node opened a socket for normal FreePastry traffic.
   */
  public static int REASON_NORMAL = 0;
  /**
   * Local node opened a socket for source routing.
   */
  public static int REASON_SR = 1;
  /**
   * Local node opened a to acquire bootstrap information.
   */
  public static int REASON_BOOTSTRAP = 2;
  
  /**
   * Remote node opened a socket for normal FreePastry traffic.
   */
  public static int REASON_ACC_NORMAL = 3;
  /**
   * Remote node opened a socket for source routing.
   */
  public static int REASON_ACC_SR = 4;
  /**
   * Remote node opened a to acquire bootstrap information.
   */
  public static int REASON_ACC_BOOTSTRAP = 5;
  /**
   * Local node opened an application level socket.
   */
  public static int REASON_APP_SOCKET_NORMAL = 6;
  
  /**
   * Called when a socket is opened.
   * 
   * @param addr the address the socket was opened to
   * @param reason see above
   */
  public void channelOpened(InetSocketAddress addr, int reason);
  /**
   * Called when a socket is closed.
   * 
   * @param addr the address the socket was opened to
   */
  public void channelClosed(InetSocketAddress addr);

  /**
   * called when data is sent.
   * 
   * @param msgAddress the application the message belongs to
   * @param msgType the type of message for that application
   * @param socketAddress the socket it is from
   * @param size the size of the message
   * @param wireType UDP/TCP
   */
  public void dataSent(int msgAddress, short msgType, InetSocketAddress socketAddress, int size, int wireType);
  
  /**
   * called when data is received.
   * 
   * @param msgAddress the application the message belongs to
   * @param msgType the type of message for that application
   * @param socketAddress the socket it is from
   * @param size the size of the message
   * @param wireType UDP/TCP
   */
  public void dataReceived(int msgAddress, short msgType, InetSocketAddress socketAddress, int size, int wireType);
   
}


