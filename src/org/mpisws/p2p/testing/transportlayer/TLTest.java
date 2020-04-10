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
package org.mpisws.p2p.testing.transportlayer;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;

import rice.Continuation;
import rice.environment.Environment;
import rice.environment.logging.Logger;

public abstract class TLTest<Identifier> {
  
  static Environment env;
  static Logger logger;
  static Map<String, Object> options = new HashMap<String, Object>();
  static TransportLayer alice, bob;
  static final byte[] sentBytes = {0,1,2,3,4,5,6,7};
  static final int START_PORT = 5009;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    env = new Environment();
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    env.destroy();
  }

  /**
   * A's view of b
   * @param a
   * @param b
   * 
   * @return A's view of b
   */
  public Identifier getIdentifier(
      TransportLayer<Identifier, ByteBuffer> a, 
      TransportLayer<Identifier, ByteBuffer> b) {
    return b.getLocalIdentifier(); 
  }

  /**
   * The identifer to a bogus node.
   * 
   * @return
   */
  public abstract Identifier getBogusIdentifier(Identifier local) throws IOException;
  
  /*********************** TCP *************************/  
  /**
   * 3 parts:
   * 
   * - Alice opens a TCP socket to Bob
   * - Alice sends to Bob
   * - Alice closes the Socket
   */
  @Test
  public void openTCP() throws Exception {
    final List<P2PSocket<Identifier>> aliceSockets = new ArrayList<P2PSocket<Identifier>>();
    final List<P2PSocket<Identifier>> bobSockets = new ArrayList<P2PSocket<Identifier>>();
    final List<Exception> exceptionList = new ArrayList<Exception>(1);
    final List<ByteBuffer> receivedList = new ArrayList<ByteBuffer>(1);
    final List<ByteBuffer> sentList = new ArrayList<ByteBuffer>(1);
    final Object lock = new Object();
    
    // Part I opening a connection
    bob.setCallback(new TransportLayerCallback<Identifier, ByteBuffer>() {
    
      public void messageReceived(Identifier i, ByteBuffer m, Map<String, Object> options)
          throws IOException {
        // TODO Auto-generated method stub
    
      }
    
      public void incomingSocket(P2PSocket<Identifier> s)
          throws IOException {
        synchronized(lock) {
          bobSockets.add(s);
          lock.notify();
        }
      }
    
    });
    
    alice.openSocket(getIdentifier(alice, bob), new SocketCallback<Identifier>(){    
      public void receiveResult(SocketRequestHandle<Identifier> s, P2PSocket<Identifier> result) {
        synchronized(lock) {
          aliceSockets.add(result);
          lock.notify();
        }
      }
    
      public void receiveException(SocketRequestHandle<Identifier> s, Exception exception) {
        synchronized(lock) {
          exceptionList.add(exception);
          lock.notify();
        }
      }
    }, 
    options);
    
    // wait for it to open
    long timeout = env.getTimeSource().currentTimeMillis()+4000;
    synchronized(lock) {
      while((env.getTimeSource().currentTimeMillis()<timeout) && exceptionList.isEmpty() && (aliceSockets.isEmpty() || bobSockets.isEmpty())) {
        lock.wait(1000); 
      }
    }
    assertTrue(aliceSockets.size() == 1);
    assertTrue(bobSockets.size() == 1);
    assertTrue(exceptionList.isEmpty());
    

    // Part II sending data
    final byte[] sentBytes = {0,1,2,3,4,5,6,7};
    final ByteBuffer sentBuffer = ByteBuffer.wrap(sentBytes); 
    final ByteBuffer in = ByteBuffer.allocate(sentBytes.length);
    
    bobSockets.get(0).register(true, false, new P2PSocketReceiver<Identifier>() {    
      public void receiveSelectResult(P2PSocket<Identifier> socket, boolean canRead,
          boolean canWrite) throws IOException {
        
        socket.read(in);
        
        if (in.hasRemaining()) {
          socket.register(true, false, this); 
        } else {
          synchronized(lock) {
            receivedList.add(in);
            lock.notify(); 
          }
        }
      }
    
      public void receiveException(P2PSocket<Identifier> socket, Exception e) {
        synchronized(lock) {
          exceptionList.add(e);
          lock.notify(); 
        }
      }    
    });

    aliceSockets.get(0).register(false, true, new P2PSocketReceiver<Identifier>() {    
      public void receiveSelectResult(P2PSocket<Identifier> socket, boolean canRead,
          boolean canWrite) throws IOException {
        socket.write(sentBuffer);
        
        if (sentBuffer.hasRemaining()) {
          socket.register(false, true, this); 
        } else {
          synchronized(lock) {
            sentList.add(sentBuffer);
            lock.notify(); 
          }          
        }
      }
    
      public void receiveException(P2PSocket socket, Exception e) {
        synchronized(lock) {
          exceptionList.add(e);
          lock.notify(); 
        }
      }
    
    });
    
    timeout = env.getTimeSource().currentTimeMillis()+4000;
    synchronized(lock) {
      while((env.getTimeSource().currentTimeMillis()<timeout) && exceptionList.isEmpty() && (receivedList.isEmpty() || sentList.isEmpty())) {
        lock.wait(1000); 
      }
    }
    
    assertTrue(sentList.size() == 1);
    assertTrue(receivedList.size() == 1);
    assertTrue("recList.arr:"+receivedList.get(0).array()[0]+","+receivedList.get(0).array()[1],Arrays.equals(sentList.get(0).array(), receivedList.get(0).array()));
    assertTrue(exceptionList.isEmpty());

    // Part III test shutdown/close()
    final List<P2PSocket> closed = new ArrayList<P2PSocket>();
    
    aliceSockets.get(0).shutdownOutput();
    
    bobSockets.get(0).register(true, false, new P2PSocketReceiver<Identifier>() {    
      public void receiveSelectResult(P2PSocket<Identifier> socket, boolean canRead,
          boolean canWrite) throws IOException {
        
        ByteBuffer bogus = ByteBuffer.allocate(1);
        if (socket.read(bogus) == -1) {
          socket.close();
          synchronized(lock) {
            closed.add(socket);
            lock.notify(); 
          }
        } else {
          synchronized(lock) {
            receivedList.add(bogus);
            lock.notify(); 
          }
        }        
      }
    
      public void receiveException(P2PSocket<Identifier> socket, Exception e) {
        synchronized(lock) {
          exceptionList.add(e);
          lock.notify(); 
        }
      }    
    });

    aliceSockets.get(0).register(true, false, new P2PSocketReceiver<Identifier>() {    
      public void receiveSelectResult(P2PSocket<Identifier> socket, boolean canRead,
          boolean canWrite) throws IOException {
        
        ByteBuffer bogus = ByteBuffer.allocate(1);
        if (socket.read(bogus) == -1) {
          socket.close();
          synchronized(lock) {
            closed.add(socket);
            lock.notify(); 
          }
        } else {
          synchronized(lock) {
            receivedList.add(bogus);
            lock.notify(); 
          }
        }        
      }
    
      public void receiveException(P2PSocket<Identifier> socket, Exception e) {
        synchronized(lock) {
          exceptionList.add(e);
          lock.notify(); 
        }
      }    
    });
    
    
    timeout = env.getTimeSource().currentTimeMillis()+4000;
    synchronized(lock) {
      while((env.getTimeSource().currentTimeMillis()<timeout) && 
          exceptionList.isEmpty() && 
          receivedList.size() == 1 && 
          (closed.size() < 2)) {
        lock.wait(1000); 
      }
    }

    bob.setCallback(null);

    assertTrue(exceptionList.isEmpty());
    assertTrue("receivedList.size():"+receivedList.size(), receivedList.size() == 1);
    assertTrue(closed.size() == 2);
    assertTrue(closed.contains(aliceSockets.get(0)));
    assertTrue(closed.contains(bobSockets.get(0)));
  }
  
  public void testSocketThreadSafety() {
    // send on the selector
    // send not on the selector
    
    // TODO: do the same to send UDP message
  }
  
  /*********************** UDP *************************/  
  /**
   * Sends udp message from alice to bob.
   */
  @Test
  public void sendUDP() throws Exception {
    final ByteBuffer sentBuffer = ByteBuffer.wrap(sentBytes); 
   
    class Tupel<Identifier> {
      Identifier i;
      ByteBuffer buf;
      public Tupel(Identifier i, ByteBuffer buf) {
        this.i = i;
        this.buf = buf;
      }
    }
    
    // added to every time bob receives something
    final List<Tupel> receivedList = new ArrayList<Tupel>(1);
    final List<Exception> exceptionList = new ArrayList<Exception>(1);
    final List<MessageRequestHandle> sentList = new ArrayList<MessageRequestHandle>(1);
    final Object lock = new Object();
//    TestEnv<Identifier> tenv = getTestEnv();

    // make a way for bob to receive the callback
    bob.setCallback(new TransportLayerCallback<Identifier, ByteBuffer>() {    
      public void messageReceived(Identifier i, ByteBuffer buf, Map<String, Object> options) throws IOException {
        synchronized(lock) {
          receivedList.add(new Tupel(i, buf));
          lock.notify();
        }
      }    
      public void incomingSocket(P2PSocket s) {}    
    });
    
    MessageRequestHandle handle = alice.sendMessage(
        getIdentifier(alice, bob), 
        sentBuffer, 
        new MessageCallback<Identifier, ByteBuffer>() {
    
          public void ack(MessageRequestHandle msg) {
            synchronized(lock) {
              sentList.add(msg);
              lock.notify();
            }        
          }    
          
          public void sendFailed(MessageRequestHandle msg, Exception ex) {
            synchronized(lock) {
              ex.printStackTrace();
              exceptionList.add(ex);
              lock.notify();
            }
          }
    }, 
    options);
    
    // block for completion
    long timeout = env.getTimeSource().currentTimeMillis()+4000;
    synchronized(lock) {
      while((env.getTimeSource().currentTimeMillis()<timeout) && exceptionList.isEmpty() && (receivedList.isEmpty() || sentList.isEmpty())) {
        lock.wait(1000); 
      }
    }

    // results
    if (!exceptionList.isEmpty()) throw exceptionList.get(0); // we got no exceptions
    assertTrue("receivedList.size():"+receivedList.size(),receivedList.size() == 1);  // we got 1 result
    byte[] rec = new byte[receivedList.get(0).buf.remaining()];
    receivedList.get(0).buf.get(rec);
    assertTrue(Arrays.equals(rec, sentBuffer.array())); // it is equal
    assertTrue("i:"+receivedList.get(0).i+" getI():"+getIdentifier(bob, alice), receivedList.get(0).i.equals(getIdentifier(bob, alice)));
    assertTrue("sentList.size():"+sentList.size(),sentList.size() == 1); // we got one notification
    assertTrue(sentList.get(0) == handle); // it is the buffer we sent        
    assertTrue("handle.getMessage():"+handle.getMessage()+" sentBuffer:"+sentBuffer,handle.getMessage() == sentBuffer); // it is the buffer we sent        
    bob.setCallback(null);
  }
  
  /**
   * Sends huge udp message from alice to bob, should fail.
   */
  @Test
  public void messageTooBigUDP() throws Exception {
    final byte[] sentBytes = new byte[1000000]; // a big message
    final ByteBuffer sentBuffer = ByteBuffer.wrap(sentBytes); 
   
    // added to every time bob receives something
    final List<MessageRequestHandle> failedList = new ArrayList<MessageRequestHandle>(1);
    final List<MessageRequestHandle> sentList = new ArrayList<MessageRequestHandle>(1);
    final List<Exception> exceptionList = new ArrayList<Exception>(1);
    final Object lock = new Object();
//    TestEnv<Identifier> tenv = getTestEnv();

    MessageRequestHandle cancellable = alice.sendMessage(
        getIdentifier(alice, bob), 
        sentBuffer, 
        new MessageCallback<Identifier, ByteBuffer>() {
    
          public void ack(MessageRequestHandle msg) {
            synchronized(lock) {
              sentList.add(msg);
              lock.notify();
            }        
          }    
          public void sendFailed(MessageRequestHandle msg, Exception exception) {
            synchronized(lock) {
              failedList.add(msg);
//              logger.logException("foo", exception);
              exceptionList.add(exception);
              lock.notify();
            }
          }
        }, 
        options);
    
    // block for completion
    long timeout = env.getTimeSource().currentTimeMillis()+4000;
    synchronized(lock) {
      while((env.getTimeSource().currentTimeMillis()<timeout) && exceptionList.isEmpty() && sentList.isEmpty()) {
        lock.wait(1000); 
      }
    }

    // results
    assertTrue(sentList.isEmpty());  // we got 1 result
    assertTrue("exceptionList.size():"+exceptionList.size(), exceptionList.size() == 1);
    assertTrue(failedList.size() == 1);
    assertTrue(failedList.get(0) == cancellable);
    assertTrue(cancellable.getMessage() == sentBuffer);
    
//    logger.logException("Expected:", exceptionList.get(0));
  }  
  
  @Test(expected=IllegalArgumentException.class)
  public void noCallbackTest() throws Exception {
    final ByteBuffer sentBuffer = ByteBuffer.wrap(sentBytes); 
    
    class Tupel<Identifier> {
      Identifier i;
      ByteBuffer buf;
      public Tupel(Identifier i, ByteBuffer buf) {
        this.i = i;
        this.buf = buf;
      }
    }
    
    // added to every time bob receives something
    final List<Tupel> receivedList = new ArrayList<Tupel>(1);
    final List<Exception> exceptionList = new ArrayList<Exception>(1);
    final Object lock = new Object();
//    TestEnv<Identifier> tenv = getTestEnv();

    // make a way for bob to receive the callback
    bob.setCallback(new TransportLayerCallback<Identifier, ByteBuffer>() {    
      public void messageReceived(Identifier i, ByteBuffer buf, Map<String, Object> options) throws IOException {
        synchronized(lock) {
          receivedList.add(new Tupel(i, buf));
          lock.notify();
        }
      }    
      public void incomingSocket(P2PSocket s) {}    
    });
    
    alice.sendMessage(
        getIdentifier(alice, bob), 
        sentBuffer, 
        null, 
        null);
    
    // block for completion
    long timeout = env.getTimeSource().currentTimeMillis()+4000;
    synchronized(lock) {
      while((env.getTimeSource().currentTimeMillis()<timeout) && exceptionList.isEmpty() && receivedList.isEmpty()) {
        lock.wait(1000); 
      }
    }

    // results
    if (!exceptionList.isEmpty()) throw exceptionList.get(0); // we got no exceptions
    assertTrue("receivedList.size():"+receivedList.size(),receivedList.size() == 1);  // we got 1 result
    byte[] rec = new byte[receivedList.get(0).buf.remaining()];
    receivedList.get(0).buf.get(rec);
    assertTrue(Arrays.equals(rec, sentBuffer.array())); // it is equal
    assertTrue(receivedList.get(0).i.equals(getIdentifier(bob, alice)));
    bob.setCallback(null); 
    
    alice.openSocket(getIdentifier(alice, bob), null, options);
  }

}
