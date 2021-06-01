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
package org.mpisws.p2p.transport.table;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.util.BufferReader;
import org.mpisws.p2p.transport.util.BufferWriter;
import org.mpisws.p2p.transport.util.DefaultErrorHandler;
import org.mpisws.p2p.transport.util.Serializer;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;

import rice.Continuation;
import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * @author Jeff Hoye
 *
 */
public class TableTransprotLayerImpl<Identifier, Key, Value> implements 
    TableTransportLayer<Identifier, Key, Value>, 
    TransportLayerCallback<Identifier, ByteBuffer> {
  public static final byte PASSTHROUGH = 0;
  public static final byte REQUEST = 1;
  public static final byte RESPONSE_SUCCESS = 2;
  public static final byte RESPONSE_FAILED = 3;
  
  /**
   * Could just be a hashTable
   */
  protected TableStore<Key, Value> knownValues;

  protected TransportLayerCallback<Identifier, ByteBuffer> callback;
  protected TransportLayer<Identifier, ByteBuffer> tl;

  protected Serializer<Key> keySerializer;
  protected Serializer<Value> valueSerializer;
   
  protected ErrorHandler<Identifier> errorHandler;
  protected Logger logger;
  
  /**
   * 
   * @param iSerializer
   * @param cSerializer
   * @param tableStore should be pre-populated with any persistent or initial values
   * @param tl
   * @param env
   */
  public TableTransprotLayerImpl(Serializer<Key> iSerializer, Serializer<Value> cSerializer, TableStore<Key, Value> tableStore, TransportLayer<Identifier, ByteBuffer> tl, Environment env) {
    this.keySerializer = iSerializer;
    this.valueSerializer = cSerializer;
    this.knownValues = tableStore;
    this.tl = tl;
    
    this.logger = env.getLogManager().getLogger(getClass(), null);
    this.errorHandler = new DefaultErrorHandler<Identifier>(this.logger);
    
  }
  
  /**
   * REQUEST, int requestId, Key
   */
  public Cancellable requestValue(final Identifier source,
      final Key principal, final Continuation<Value, Exception> c,
      Map<String, Object> options) {    
    
    if (logger.level <= Logger.FINE) logger.log("requestValue("+source+","+principal+")");
    if (knownValues.containsKey(principal)) {
      if (c != null) c.receiveResult(knownValues.get(principal));
      return null;
    }
    
    if (logger.level <= Logger.FINER) logger.log("requestValue("+source+","+principal+") opening socket");
    return tl.openSocket(source, new SocketCallback<Identifier>() {

      public void receiveResult(SocketRequestHandle<Identifier> cancellable,
          P2PSocket<Identifier> sock) {
        try {          
          SimpleOutputBuffer sob = new SimpleOutputBuffer();
          keySerializer.serialize(principal,sob);
                    
          SimpleOutputBuffer sob2 = new SimpleOutputBuffer();
          sob2.writeByte(REQUEST);
          
          // here, we are allocating the space for the size value
          sob2.writeInt(sob.getWritten()); 
          sob2.write(sob.getBytes());          
          
          new BufferWriter<Identifier>(sob2.getByteBuffer(), sock, new Continuation<P2PSocket<Identifier>, Exception>() {
  
            public void receiveException(Exception exception) {
              c.receiveException(exception);
            }
  
            public void receiveResult(P2PSocket<Identifier> result) {
              new BufferReader<Identifier>(result,new Continuation<ByteBuffer, Exception>() {
              
                public void receiveResult(ByteBuffer result) {
                  try {
                    SimpleInputBuffer sib = new SimpleInputBuffer(result);
                    byte response = sib.readByte();
                    switch(response) {
                    case RESPONSE_SUCCESS:
                      Value value = valueSerializer.deserialize(sib);
                      
                      if (logger.level <= Logger.FINER) logger.log("requestValue("+source+","+principal+") got value "+value);

                      knownValues.put(principal, value);
                      c.receiveResult(value);
                      break;
                    case RESPONSE_FAILED:
                      c.receiveException(new UnknownValueException(source, principal));
                      break;
                    default:
                      c.receiveException(new IllegalStateException("Unknown response:"+response));    
                      break;
                    }
                  } catch (Exception ioe) {
                    c.receiveException(ioe);
                  }
                }
              
                public void receiveException(Exception exception) {
                  c.receiveException(exception);
                }
              
              });
            }        
          },false);
        } catch (IOException ioe) {
          c.receiveException(ioe);
        }
      }    
      
      public void receiveException(SocketRequestHandle<Identifier> s,
          Exception ex) {
        c.receiveException(ex);
      }

    }, options);
  }
  
  public SocketRequestHandle<Identifier> openSocket(Identifier i,
      final SocketCallback<Identifier> deliverSocketToMe, Map<String, Object> options) {
    final SocketRequestHandleImpl<Identifier> ret = new SocketRequestHandleImpl<Identifier>(i,options,logger);
    
    ret.setSubCancellable(tl.openSocket(i, new SocketCallback<Identifier>() {

      public void receiveException(SocketRequestHandle<Identifier> s,
          Exception ex) {
        deliverSocketToMe.receiveException(ret, ex);
      }

      public void receiveResult(SocketRequestHandle<Identifier> cancellable,
          P2PSocket<Identifier> sock) {
        ByteBuffer writeMe = ByteBuffer.allocate(1);
        writeMe.put(PASSTHROUGH);
        writeMe.clear();
        new BufferWriter<Identifier>(writeMe, sock, new Continuation<P2PSocket<Identifier>, Exception>() {

          public void receiveException(Exception exception) {
            deliverSocketToMe.receiveException(ret, exception);
          }

          public void receiveResult(P2PSocket<Identifier> result) {
            deliverSocketToMe.receiveResult(ret, result);
          }
        
        }, false);
      }
      
    }, options));
    return ret;
  }
  
  public void incomingSocket(final P2PSocket<Identifier> sock) throws IOException {
    if (logger.level <= Logger.FINEST) logger.log("incomingSocket() from "+sock);
    new BufferReader<Identifier>(sock,new Continuation<ByteBuffer, Exception>() {    
      public void receiveResult(ByteBuffer result) {
        byte type = result.get();
        if (logger.level <= Logger.FINEST) logger.log("incomingSocket() from "+sock+" "+type);
        
        switch (type) {
        case PASSTHROUGH:
          try {
            callback.incomingSocket(sock);
          } catch (IOException ioe) {
            errorHandler.receivedException(sock.getIdentifier(), ioe);
          }
          return;
        case REQUEST:
          handleValueRequest(sock);
          return;
        default:
          errorHandler.receivedUnexpectedData(sock.getIdentifier(), new byte[] {type}, 0, sock.getOptions());
          sock.close();
        }
      }
    
      public void receiveException(Exception exception) {
        errorHandler.receivedException(sock.getIdentifier(), exception);
      }    
    },1);    
  }

  public void handleValueRequest(final P2PSocket<Identifier> sock) {
    if (logger.level <= Logger.FINER) logger.log("handleValueRequest() from "+sock);
    new BufferReader<Identifier>(sock,new Continuation<ByteBuffer, Exception>() {
    
      public void receiveResult(ByteBuffer result) {
        try {
          SimpleInputBuffer sib = new SimpleInputBuffer(result);
          Key principal = keySerializer.deserialize(sib);
          ByteBuffer writeMe;
          if (knownValues.containsKey(principal)) {
            SimpleOutputBuffer sob = new SimpleOutputBuffer();
            sob.writeByte(RESPONSE_SUCCESS);
            valueSerializer.serialize(knownValues.get(principal), sob);
            writeMe = sob.getByteBuffer();
          } else {
            writeMe = ByteBuffer.allocate(1);
            writeMe.put(RESPONSE_FAILED);
            writeMe.clear();
          }
          new BufferWriter<Identifier>(writeMe,sock,null);
        } catch (Exception ioe) {
          errorHandler.receivedException(sock.getIdentifier(), ioe);
          sock.close();
        }
      }
    
      public void receiveException(Exception exception) {
        errorHandler.receivedException(sock.getIdentifier(), exception);
      }
    
    });
  }
  
  public boolean hasKey(Key i) {
    return knownValues.containsKey(i);
  }
  
  public void acceptMessages(boolean b) {
    tl.acceptMessages(b);
  }
  
  public void acceptSockets(boolean b) {
    tl.acceptSockets(b);
  }
  
  public Identifier getLocalIdentifier() {
    return tl.getLocalIdentifier();
  }
  
  public MessageRequestHandle<Identifier, ByteBuffer> sendMessage(Identifier i,
      ByteBuffer m, MessageCallback<Identifier, ByteBuffer> deliverAckToMe,
      Map<String, Object> options) {
    return tl.sendMessage(i, m, deliverAckToMe, options);
  }
  
  public void setCallback(
      TransportLayerCallback<Identifier, ByteBuffer> callback) {
    this.callback = callback;
  }
  public void setErrorHandler(ErrorHandler<Identifier> handler) {
    this.errorHandler = handler;
  }
  
  public void destroy() {
    tl.destroy();
  }
  
  public void messageReceived(Identifier i, ByteBuffer m,
      Map<String, Object> options) throws IOException {
    callback.messageReceived(i, m, options);
  }
}
