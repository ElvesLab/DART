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
package org.mpisws.p2p.transport.wire;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;

import rice.environment.logging.Logger;
import rice.environment.params.Parameters;
import rice.selector.SelectionKeyHandler;

public class UDPLayerImpl extends SelectionKeyHandler implements UDPLayer {
  public static final Map<String, Object> OPTIONS;  
  static {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put(WireTransportLayer.OPTION_TRANSPORT_TYPE, WireTransportLayer.TRANSPORT_TYPE_DATAGRAM);
    OPTIONS = Collections.unmodifiableMap(map);    
  }
  
  Logger logger;
  
  // the channel used from talking to the network
  private DatagramChannel channel;

  // the key used to determine what has taken place
  private SelectionKey key;
  
  // the size of the buffer used to read incoming datagrams must be big enough
  // to encompass multiple datagram packets
  public int DATAGRAM_RECEIVE_BUFFER_SIZE;
  
  // the size of the buffer used to send outgoing datagrams this is also the
  // largest message size than can be sent via UDP
  public int DATAGRAM_SEND_BUFFER_SIZE;
  
  /**
   * We always send this, and only pass up messages that match this.
   * 
   * If the message doesn't match this, then we call the error handler.
   * 
   * Usually a magic number/version
   * 
   * TODO: Extend this to accept different versions, perhaps have a different layer-callback/version
   */
  
  List<Envelope> pendingMsgs;
  
  WireTransportLayerImpl wire;
  
  ByteBuffer readBuffer;
  
  public UDPLayerImpl(WireTransportLayerImpl wire) throws IOException {
    this.wire = wire;
    
    this.logger = wire.environment.getLogManager().getLogger(UDPLayer.class, null);

    this.pendingMsgs = new LinkedList<Envelope>();
    openServerSocket();
  }

  /**
   * The ack is not the end to end, it's called when actually sent
   * 
   * @param destination
   * @param m
   * @param deliverAckToMe ack is when the message is sent to the wire
   */
  public MessageRequestHandle<InetSocketAddress, ByteBuffer> sendMessage(
      InetSocketAddress destination, 
      ByteBuffer msg,
      MessageCallback<InetSocketAddress, ByteBuffer> deliverAckToMe, 
      Map<String, Object> options) {
    //logger.log("sendMessage("+destination+","+msg+","+deliverAckToMe+")"); 
    Envelope envelope;
    if (logger.level <= Logger.FINER-3) logger.log("sendMessage("+destination+","+msg+","+deliverAckToMe+")"); 
//    try {
    envelope = new Envelope(destination, msg, deliverAckToMe, options);
      synchronized (pendingMsgs) {        
        pendingMsgs.add(envelope);
      }

      wire.environment.getSelectorManager().modifyKey(key);
//    } catch (IOException e) {
//      if (logger.level <= Logger.SEVERE) logger.log(
//          "ERROR: Received exceptoin " + e + " while enqueuing ping " + msg);
//    }
      return envelope;
  }

  protected void openServerSocket() throws IOException {
//    logger.log("openServerSocket("+wire.bindAddress+")");
    Parameters p = wire.environment.getParameters();
    DATAGRAM_RECEIVE_BUFFER_SIZE = p.getInt("transport_wire_datagram_receive_buffer_size");
    DATAGRAM_SEND_BUFFER_SIZE = p.getInt("transport_wire_datagram_send_buffer_size");

    // allocate enough bytes to read data
    this.readBuffer = ByteBuffer.allocateDirect(DATAGRAM_SEND_BUFFER_SIZE);

    try {
      // bind to the appropriate port
      channel = DatagramChannel.open();
      channel.configureBlocking(false);
      
      // this needs to be false because in windows, TCP doesn't do the right thing, so we need UDP to 
      // fail with a BindException
      /*
       * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6421091 The
       * setReuseAddress option toggles the SO_REUSEADDR socket option.
       * Unfortunately on Windows this socket option does not provide BSD
       * semantics and does not prevent multiple TCP sockets from binding to the
       * same address/port. This explains why the bind method does not fail in
       * this test case. The reason the connect method throws an exception stems
       * from the fact that the TCP/IP protocol requires a unique 4-tuple to
       * distinguish connections (4-tuple = local-addr/port and
       * remote-addr/port). As both SocketChannels are bound to the same
       * address/port and connect to the same remote address/port it means the
       * second connect is required to fail. It fails with a bind error and that
       * is why we throw a BindException.
       * 
       * So is this problem fixable? In Windows 2000 SP1 Microsoft added the
       * SO_EXCLUSIVEADDRUSE which prevents multiple sockets from binding to the
       * same address/port. This socket option was initially only usable by
       * processes with Administrator privileges but that was changed in Windows
       * XP SP2 to allow non-Administrators use the option. Unfortunately this
       * socket option has side effects. We can't use it for MulticastSocket for
       * example as we need to be able to bind multiple UDP sockets to the same
       * address/port for multicasting purposes. Also, the socket option creates
       * problems for ServerSocket/ServerSocketChannel as it prevents the
       * re-binding of the socket when a there is a previous connection in
       * TIMED_WAIT state.
       * 
       * One possible workaround for the submitter of this bug is to set the
       * DisableAddressSharing registry.
       * (HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\services\Afd\Parameters)
       * and reboot. This registry setting prevents multiple sockets from
       * binding to the same port and is essentially enabling
       * SO_EXCLUSIVEADDRUSE on all sockets. We have not done any testing with
       * this registry setting so there may be side-effects (like unable to
       * re-bind when a previous connection is in timed wait for example).
       * Posted Date : 2006-05-03 11:40:31.0
       */
      channel.socket().setReuseAddress(false);
      
      channel.socket().bind(wire.bindAddress);
      channel.socket().setSendBufferSize(DATAGRAM_SEND_BUFFER_SIZE);
      channel.socket().setReceiveBufferSize(DATAGRAM_RECEIVE_BUFFER_SIZE);

      key = wire.environment.getSelectorManager().register(channel, this, 0);
      key.interestOps(SelectionKey.OP_READ);
      if (logger.level <= Logger.INFO) logger.log("UDPLayer bound to "+wire.bindAddress);
    } catch (BindException be) {
      // Java's stupid exception isn't helpful, make a helpful one...
      throw new BindException("Address already in use:"+wire.bindAddress);
    } catch (IOException e) {
      throw e;
    }
  }
  

  
  /**
   * DESCRIBE THE METHOD
   *
   * @param key DESCRIBE THE PARAMETER
   */
  public void read(SelectionKey key) {
//    logger.log("read");

    try {
      InetSocketAddress address = null;
      
      while ((address = (InetSocketAddress) channel.receive(readBuffer)) != null) {
        readBuffer.flip();

        if (readBuffer.remaining() > 0) {
          readHeader(address);
          readBuffer.clear();
        } else {
          if (logger.level <= Logger.INFO) logger.log(
            "(PM) Read from datagram channel, but no bytes were there - no bad, but wierd.");
          break;
        }
      }
    } catch (IOException e) {
      wire.errorHandler.receivedException(null, e);
//      if (logger.level <= Logger.WARNING) logger.logException(
//          "ERROR (datagrammanager:read): ", e);
    } finally {
      readBuffer.clear();
    }
  }

  protected void readHeader(InetSocketAddress address) throws IOException {
    // see if we have the header
//    if (readBuffer.remaining() < wire.HEADER.length) {
//      byte[] remaining = new byte[readBuffer.remaining()];      
//      readBuffer.get(remaining);
//      wire.errorHandler.receivedUnexpectedData(address, remaining);
//      return;
//    }
    // get the header
//    byte[] header = new byte[wire.HEADER.length];
//    readBuffer.get(header, 0, wire.HEADER.length);
//    if (!Arrays.equals(header, wire.HEADER)) {
//      wire.errorHandler.receivedUnexpectedData(address, header);      
//      return;
//    }
    if (logger.level <= Logger.FINE) 
      logger.log("readHeader("+address+","+readBuffer.remaining()+")");
    byte[] remaining = new byte[readBuffer.remaining()];      
    readBuffer.get(remaining);
    wire.messageReceived(address, ByteBuffer.wrap(remaining), OPTIONS);    
  }
  

  /**
   * DESCRIBE THE METHOD
   *
   * @param key DESCRIBE THE PARAMETER
   */
  public void write(SelectionKey key) {
//    logger.log("write");
    Envelope write = null;
    
    try {
      synchronized (pendingMsgs) {
        Iterator<Envelope> i = pendingMsgs.iterator();

        while (i.hasNext()) {
          write = i.next();          
          try {            
//            byte[] whole_msg = new byte[wire.HEADER.length+write.msg.remaining()];
//            System.arraycopy(wire.HEADER, 0, whole_msg, 0, wire.HEADER.length);
//            write.msg.get(whole_msg, wire.HEADER.length, write.msg.remaining());
//            ByteBuffer buf = ByteBuffer.wrap(whole_msg);

            int len = write.msg.remaining();
            if (logger.level <= Logger.FINE) {
              logger.log("writing "+len+" to "+write.destination);
            }
            if (channel.send(write.msg, write.destination) == len) {
//              wire.msgSent(write.destination, whole_msg, WireTransportLayer.TRANSPORT_TYPE_DATAGRAM);
              if (write.continuation != null) write.continuation.ack(write);
              i.remove();              
            } else {
              break;
            }
          } catch (IOException e) {
            if (write.continuation == null) {
              wire.errorHandler.receivedException(write.destination, e);
            } else {
              write.continuation.sendFailed(write, e);
            }
            i.remove();
            //throw e;
            return; // to get another call to write() later
          }
        }
      }
    } catch (Exception e) {
      if (logger.level <= Logger.WARNING) {
        // This code prevents this line from filling up logs during some kinds of network outages
        // it makes this error only be printed 1ce/second
//        long now = timeSource.currentTimeMillis();
//        if (lastTimePrinted+1000 > now) return;
//        lastTimePrinted = now;
        
        logger.logException(
          "ERROR (datagrammanager:write) to " + write.destination, e);
      }        
    } finally {
      if (pendingMsgs.isEmpty()) 
        key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
    }
  }

  long lastTimePrinted = 0;
  
  /**
   * DESCRIBE THE METHOD
   *
   * @param key DESCRIBE THE PARAMETER
   */
  public void modifyKey(SelectionKey key) {
    synchronized (pendingMsgs) {
      if (! pendingMsgs.isEmpty()) 
        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
    }
  }
  
  /**
   * Internal class which holds a pending datagram
   *
   * @author amislove
   */
  public class Envelope implements MessageRequestHandle<InetSocketAddress, ByteBuffer> {
    protected InetSocketAddress destination;
    /**
     * The message sans header.
     */
    protected ByteBuffer msg;
    protected MessageCallback<InetSocketAddress, ByteBuffer> continuation;
    Map<String, Object> options;

    /**
     * Constructor for Envelope.
     *
     * @param adr DESCRIBE THE PARAMETER
     * @param m DESCRIBE THE PARAMETER
     */
    public Envelope(InetSocketAddress destination, 
        ByteBuffer msg,
        MessageCallback<InetSocketAddress, ByteBuffer> deliverAckToMe, 
        Map<String, Object> options) {
      this.destination = destination;
      this.msg = msg;
      this.continuation = deliverAckToMe;
      this.options = options;
    }

    public boolean cancel() {
      if (pendingMsgs.remove(this)) {
//      continuation.receiveResult(msg); // do we want to do this?
        return true;
      }      
      return false;
    }

    public InetSocketAddress getIdentifier() {
      return destination;
    }

    public ByteBuffer getMessage() {
      return msg;
    }

    public Map<String, Object> getOptions() {
      return options;
    }
  }

  public void destroy() {
    Runnable r = new Runnable(){    
      public void run() { 
        try {
//          logger.logException("destroy", new Exception("stack trace"));
          if (logger.level <= Logger.INFO) logger.log("destroy(): "+channel);
          if (key != null) {
            if (key.channel() != null)
              key.channel().close();
            key.cancel();
            key.attach(null);
          }
        } catch (IOException ioe) {
          if (logger.level <= Logger.WARNING) logger.logException("Error destroying UDPLayer", ioe); 
        }
      }
    };
    
    // thread safety
    if (wire.environment.getSelectorManager().isSelectorThread()) {
      r.run();
    } else {
      wire.environment.getSelectorManager().invoke(r);
    }    
  }

  public void acceptMessages(final boolean b) {
    Runnable r = new Runnable(){    
      public void run() {
        if (b) {
          key.interestOps(key.interestOps() | SelectionKey.OP_READ);
        } else {
          key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
        }
      }    
    };
    
    // thread safety
    if (wire.environment.getSelectorManager().isSelectorThread()) {
      r.run();
    } else {
      wire.environment.getSelectorManager().invoke(r);
    }
  }  
}
