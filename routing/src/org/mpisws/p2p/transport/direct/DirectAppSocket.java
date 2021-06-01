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
 * Created on Jan 30, 2006
 */
package org.mpisws.p2p.transport.direct;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.exception.NodeIsFaultyException;

import rice.environment.Environment;
import rice.environment.logging.Logger;

public class DirectAppSocket<Identifier, MessageType> {
  public static final byte[] EOF = new byte[0];
  
  /**
   * The sum the simulated read/write buffers for one direction of a socket
   */
  private static final int MAX_BYTES_IN_FLIGHT = 10000;
  
  Identifier acceptor, connector;
  
  SocketCallback<Identifier> connectorReceiver;
  
  GenericNetworkSimulator<Identifier, MessageType> simulator;
  
  DirectAppSocketEndpoint acceptorEndpoint;
  DirectAppSocketEndpoint connectorEndpoint;
  
  SocketRequestHandle<Identifier> connectorHandle; 
  
  /**
   * So they show up with the correct prefixes/files of the logs
   */
  Logger acceptorLogger;
  Logger connectorLogger;

  Map<String, Object> options;
  
  public DirectAppSocket(Identifier acceptor, Identifier connector, SocketCallback<Identifier> connectorCallback, 
      GenericNetworkSimulator<Identifier, MessageType> simulator, SocketRequestHandle<Identifier> handle, Map<String, Object> options) {
    this.options = options;
    this.acceptor = acceptor;
    this.connector = connector;
    this.connectorReceiver = connectorCallback;
    this.simulator = simulator;
    this.connectorHandle = handle;
    Environment aEnv = simulator.getEnvironment(acceptor);
    Environment cEnv = simulator.getEnvironment(connector);
    acceptorLogger = aEnv.getLogManager().getLogger(DirectAppSocket.class,"");
    connectorLogger = cEnv.getLogManager().getLogger(DirectAppSocket.class,"");
    
    
    acceptorEndpoint = new DirectAppSocketEndpoint(acceptor, acceptorLogger);
    connectorEndpoint = new DirectAppSocketEndpoint(connector, connectorLogger);
    acceptorEndpoint.setCounterpart(connectorEndpoint);
    connectorEndpoint.setCounterpart(acceptorEndpoint);
  }
  
  class DirectAppSocketEndpoint implements P2PSocket<Identifier> {
    Logger logger;
    DirectAppSocketEndpoint counterpart;
    
    P2PSocketReceiver<Identifier> reader;
    P2PSocketReceiver<Identifier> writer;
    Identifier localNodeHandle;
    int seq = 0;
    
//    boolean inputClosed;
    boolean outputClosed;
    
    // these three are tightly related, and should only be modified in synchronized methods on DirectAppSocketEndpoint.this
    // bytes that are either in deliveries, or in the local buffer
    int bytesInFlight = 0;
    /**
     * of byte[]
     */
    LinkedList<byte[]> byteDeliveries = new LinkedList<byte[]>();
    /**
     * The offset of the first delivery, in case the reader didn't have enough space to read everything available.
     */
    int firstOffset = 0;
 
    
    public DirectAppSocketEndpoint(Identifier localNodeHandle, Logger logger) {
      this.localNodeHandle = localNodeHandle;
      this.logger = logger;
    }
    
    public void setCounterpart(DirectAppSocketEndpoint counterpart) {
      this.counterpart = counterpart;
    }

    public Identifier getRemoteNodeHandle() {
      return counterpart.localNodeHandle;
    }
  
    public long read(ByteBuffer dsts) throws IOException {
//      ByteBuffer[] foo = new ByteBuffer[1];
//      foo[0] = dsts;
//      return read(foo, 0, 1);
//    }
//
//    public long read(ByteBuffer[] dsts, int offset, int length) {
      int lengthRead = 0;
      
      synchronized(this) {
        if (byteDeliveries.isEmpty()) {
          return 0;
        }
        if (byteDeliveries.getFirst() == EOF) {
//          inputClosed = true;
          return -1;          
        }
        Iterator<byte[]> i = byteDeliveries.iterator();
        // loop over all messages to be delivered
        while(i.hasNext()) {
          byte[] msg = (byte[])i.next();          
          
          // loop through all the dsts, and fill them with the current message if possible
//          for (int dstCtr = offset; dstCtr < offset+length;dstCtr++) {
            ByteBuffer curBuffer = dsts;
            int lengthToPut = curBuffer.remaining();
            if (lengthToPut > (msg.length-firstOffset)) {
              lengthToPut = msg.length-firstOffset;
            }
            
            curBuffer.put(msg,firstOffset,lengthToPut);
            firstOffset+=lengthToPut;
            lengthRead+=lengthToPut;
            
            // we finished a message
//            if (firstOffset == msg.length)
//              break; // for distCtr loop
            
            // optimization: if we are here then there must be no more remaining in curBuffer
//            offset=dstCtr+1;
//          }
          
          // see if we finished a message
          if (firstOffset == msg.length) {
            i.remove();
            firstOffset = 0;
          } else {
            break; // i.hasNext() loop
          }
        }   
      } // synchronized(this)

      bytesInFlight-=lengthRead;
      if (logger.level <= Logger.FINER) logger.log(this+".write("+dsts+") len:"+lengthRead+" inFlight:"+bytesInFlight);

      simulator.enqueueDelivery(new Delivery() {              
        public void deliver() {
          counterpart.notifyCanWrite();            
        }            
        public int getSeq() {
          return 0;            
        }
        public String toString() {
          return DirectAppSocketEndpoint.this.toString()+" counterpart notifyCanWrite()";
        }
      }, 0);            
      return lengthRead;
    }

    public long write(ByteBuffer srcs) throws IOException {
//      ByteBuffer[] foo = new ByteBuffer[1];
//      foo[0] = srcs;
//      return write(foo, 0, 1);
//    }
//
//    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
      if (outputClosed) return -1;
      
      if (!simulator.isAlive(counterpart.localNodeHandle)) {
        return -1; // TODO: Eventually simulate a socket reset.
      }
      
      int availableToWrite = srcs.remaining();
//      for (int i = offset; i < offset+length; i++) {
//        availableToWrite+=srcs[i].remaining(); 
//      }
      
      int lengthToWrite;
      synchronized(counterpart) {
        lengthToWrite = MAX_BYTES_IN_FLIGHT - counterpart.bytesInFlight;
        if (lengthToWrite > availableToWrite) lengthToWrite = availableToWrite;
        counterpart.bytesInFlight+=lengthToWrite;
      }

      final byte[] msg = new byte[lengthToWrite]; 
      int remaining = lengthToWrite;
//      int i = offset;
      while(remaining > 0) {
        int lengthToReadFromBuffer = srcs.remaining();
        if (remaining < lengthToReadFromBuffer) lengthToReadFromBuffer = remaining;
        srcs.get(msg,lengthToWrite-remaining,lengthToReadFromBuffer);
        remaining-=lengthToReadFromBuffer;
//        i++;
      }
      
      if (logger.level <= Logger.FINER) logger.log(this+".write("+srcs+") len:"+lengthToWrite+" inFlight:"+counterpart.bytesInFlight);
      simulator.enqueueDelivery(new Delivery() {      
        int mySeq = seq++;
        public void deliver() {
          counterpart.addToReadQueue(msg);      
        }
        public int getSeq() {
          return mySeq; 
        }
        public String toString() {
          return DirectAppSocketEndpoint.this.toString()+" deliver msg "+msg;
        }
      }, (int)Math.round(simulator.networkDelay(localNodeHandle, counterpart.localNodeHandle)));      
      return lengthToWrite;
    }
  
    /**
     * only called on simulator thread
     * @param bb
     */
    protected void addToReadQueue(byte[] msg) {
      synchronized(this) {
        if (logger.level <= Logger.FINE) {
          if (msg == EOF) {
            logger.log(this+": addToReadQueue(EOF)");
          } else {
            logger.log(this+": addToReadQueue("+msg.length+")");            
          }
        }
        byteDeliveries.addLast(msg);
      }
      notifyCanRead();
    }    
    
    /**
     * must be called on the simulator thread
     */
    protected void notifyCanWrite() {
      if (writer == null) return;
      if (counterpart.bytesInFlight < MAX_BYTES_IN_FLIGHT) {
        P2PSocketReceiver<Identifier> temp = writer;
        writer = null;
        try {
          if (logger.level <= Logger.FINEST) logger.log(this+".notifyCanWrite()");
          temp.receiveSelectResult(this, false, true);
        } catch (IOException ioe) {
          logger.logException("Error in "+temp, ioe);
        }
      }
    }

    /**
     * must be called on the simulator thread
     */
    protected void notifyCanRead() {
      if (byteDeliveries.isEmpty()) return;
      if (reader != null) {
        P2PSocketReceiver<Identifier> temp = reader;
        reader = null;
        try {
          if (logger.level <= Logger.FINEST) logger.log(this+".notifyCanRead()");
          temp.receiveSelectResult(this, true, false);
        } catch (IOException ioe) {
          logger.logException("Error in "+temp, ioe);
        }
      }
    }    

    /**
     * Can be called on any thread
     */
    public void register(boolean wantToRead, boolean wantToWrite, 
        P2PSocketReceiver<Identifier> receiver) {
      if (wantToWrite) {
        writer = receiver; 
        
        simulator.enqueueDelivery(new Delivery() {              
          public void deliver() {
            if (!simulator.isAlive(localNodeHandle)) return;
            notifyCanWrite(); // only actually notifies if proper at the time
          }
          // I don't think this needs a sequence number, but I may be wrong
          public int getSeq() {
            return 0;
          }
          public String toString() {
            return DirectAppSocketEndpoint.this.toString()+" notifyCanWrite()";
          }

        }, 0); // I dont think this needs a delay, but I could be wrong            
      }
      
      if (wantToRead) {
        reader = receiver;
        
        simulator.enqueueDelivery(new Delivery() {              
          public void deliver() {
            if (!simulator.isAlive(localNodeHandle)) return;
            notifyCanRead(); // only actually notifies if proper at the time           
          }            
          // I don't think this needs a sequence number, but I may be wrong
          public int getSeq() {
            return 0;
          }
          public String toString() {
            return DirectAppSocketEndpoint.this.toString()+" notifyCanRead()";
          }
        }, 0); // I dont think this needs a delay, but I could be wrong            
      }        
    }
  
    public void shutdownOutput() {
      if (logger.level <= Logger.FINER) logger.log(this+".shutdownOutput()");
      outputClosed = true;
      if (!simulator.isAlive(counterpart.localNodeHandle)) return; // do nothing
      simulator.enqueueDelivery(new Delivery() {      
        int mySeq = seq++;
        public void deliver() {
          counterpart.addToReadQueue(EOF);      
        }
        public int getSeq() {
          return mySeq;
        }
        public String toString() {
          return DirectAppSocketEndpoint.this.toString()+" counterpart shutDownOutput()";
        }
      }, (int)Math.round(simulator.networkDelay(localNodeHandle, counterpart.localNodeHandle))); // I dont think this needs a delay, but I could be wrong            
    }
  
    public void shutdownInput() {
//      inputClosed = true;
    }
  
    public void close() {
      shutdownOutput();
      shutdownInput();
    }
    
    public String toString() {
      return "DAS{"+localNodeHandle+":"+simulator.isAlive(localNodeHandle)+"->"+counterpart.localNodeHandle+":"+simulator.isAlive(counterpart.localNodeHandle)+" w:"+writer+" r:"+reader+"}"; 
    }

    public Identifier getIdentifier() {
      return getRemoteNodeHandle();
    }

    public Map<String, Object> getOptions() {
      return options;
    }
  }  

  
  /**
   * This is how the Acceptor Responds, success is the ConnectorDelivery, failure is the ConnectorExceptionDelivery.
   * 
   * When connect() this is sent to the Acceptor, then it responds with a ConnectorDelivery
   * 
   * @author Jeff Hoye
   */
  class AcceptorDelivery implements Delivery {
    public void deliver() {
      if (simulator.isAlive(acceptor)) {
        DirectTransportLayer<Identifier, MessageType> acceptorTL = simulator.getTL(acceptor);
        if (acceptorTL.canReceiveSocket()) {
          acceptorTL.finishReceiveSocket(acceptorEndpoint);
          simulator.enqueueDelivery(new ConnectorDelivery(),
              (int)Math.round(simulator.networkDelay(acceptor, connector))); 
        } else {
          simulator.enqueueDelivery(new ConnectorExceptionDelivery<Identifier>(connectorReceiver,connectorHandle,new SocketTimeoutException()),
              (int)Math.round(simulator.networkDelay(acceptor, connector))); 
        }
      } else {
        simulator.enqueueDelivery(new ConnectorExceptionDelivery<Identifier>(connectorReceiver,connectorHandle,new NodeIsFaultyException(acceptor)),0);
        // TODO: this should probably take into account a real delay, however, acceptor has already been removed from the simulator
//            (int)Math.round(simulator.networkDelay(acceptor, connector))+
//            (int)Math.round(simulator.networkDelay(connector, acceptor))); 
      }
    }
    public int getSeq() {
      return -1; 
    }
  }
  
  class ConnectorDelivery implements Delivery {
    public void deliver() {      
      if (simulator.isAlive(connector)) {
        connectorReceiver.receiveResult(connectorHandle, connectorEndpoint);
      } else {
        System.out.println("NOT IMPLEMENTED: Connector died during application socket initiation.");
//        simulator.enqueueDelivery(new ConnectorExceptionDelivery(new NodeIsDeadException(acceptorNodeHandle))); 
      }
    }
    // out of band, needs to get in front of any other message
    public int getSeq() {
      return -1; 
    }
  }
  
  public Delivery getAcceptorDelivery() {
    return new AcceptorDelivery();
  }
  
  public String toString() {
    return "DAS{"+connector+"["+connectorReceiver+"]->"+acceptor+"}"; 
  }
}
