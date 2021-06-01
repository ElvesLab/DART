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

package rice.p2p.past;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.logging.*;

import rice.*;
import rice.Continuation.*;
import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.environment.params.Parameters;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.appsocket.*;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.past.PastPolicy.*;
import rice.p2p.past.messaging.*;
import rice.p2p.past.rawserialization.*;
import rice.p2p.replication.*;
import rice.p2p.replication.manager.*;
import rice.p2p.util.MathUtils;
import rice.p2p.util.rawserialization.*;
import rice.persistence.*;

/**
 * @(#) PastImpl.java
 *
 * This is an implementation of the Past interface.
 *
 * @version $Id: PastImpl.java 4654 2009-01-08 16:33:07Z jeffh $
 * @author Alan Mislove
 * @author Ansley Post
 * @author Peter Druschel
 */
@SuppressWarnings("unchecked")
public class PastImpl implements Past, Application, ReplicationManagerClient {
  
  
  // ----- STATIC FIELDS -----
  // the number of milliseconds to wait before declaring a message lost
  public final int MESSAGE_TIMEOUT;// = 30000;
  
  // the percentage of successful replica inserts in order to declare success
  public final double SUCCESSFUL_INSERT_THRESHOLD;// = 0.5;
  

  // ----- VARIABLE FIELDS -----

  // this application's endpoint
  protected Endpoint endpoint;

  // the storage manager used by this Past
  protected StorageManager storage;
  
  // The trash can, or where objects should go once removed.  If null, they are deleted
  protected StorageManager trash;
  
  // The backup store, or location of over-replicated objects, helping PAST to better deal with churn
  protected Cache backup;

  // the replication factor for Past
  protected int replicationFactor;

  // the replica manager used by Past
  protected ReplicationManager replicaManager;
  
  protected LockManager lockManager;
  
  // the policy used for application-specific behavior
  protected PastPolicy policy;

  // the unique ids used by the messages sent across the wire
  private int id;

  // the hashtable of outstanding messages
  private Hashtable outstanding;
  
  // the hashtable of outstanding timer tasks
  private Hashtable timers;

  // the factory for manipulating ids
  protected IdFactory factory;

  // the instance name we are running with
  protected String instance;
  
  // debug variables
  public int inserts = 0;
  public int lookups = 0;
  public int fetchHandles = 0;
  public int other = 0;
  
  protected Environment environment;
  protected Logger logger;

  protected PastContentDeserializer contentDeserializer;
  protected PastContentHandleDeserializer contentHandleDeserializer;
  
  public SocketStrategy socketStrategy;
  
  /**
   * Constructor for Past, using the default policy
   *
   * @param node The node below this Past implementation
   * @param manager The storage manager to be used by Past
   * @param replicas The number of object replicas
   * @param instance The unique instance name of this Past
   */
  public PastImpl(Node node, StorageManager manager, int replicas, String instance) {
    this(node, manager, replicas, instance, new DefaultPastPolicy());
  }
  
  /**
   * Constructor for Past
   *
   * @param node The node below this Past implementation
   * @param manager The storage manager to be used by Past
   * @param replicas The number of object replicas
   * @param instance The unique instance name of this Past
   */
  public PastImpl(Node node, StorageManager manager, int replicas, String instance, PastPolicy policy) {
    this(node, manager, null, replicas, instance, policy, null);
  }
  
  
  protected class PastDeserializer implements MessageDeserializer {
    public Message deserialize(InputBuffer buf, short type, int priority,
        NodeHandle sender) throws IOException {
      try {
        switch(type) {
          case CacheMessage.TYPE:
            return CacheMessage.build(buf, endpoint, contentDeserializer);
          case FetchHandleMessage.TYPE:
            return FetchHandleMessage.build(buf, endpoint, contentHandleDeserializer);
          case FetchMessage.TYPE:
            return FetchMessage.build(buf, endpoint, contentDeserializer, contentHandleDeserializer);
          case InsertMessage.TYPE:
            return InsertMessage.build(buf, endpoint, contentDeserializer);
          case LookupHandlesMessage.TYPE:
            return LookupHandlesMessage.build(buf, endpoint);
          case LookupMessage.TYPE:
            return LookupMessage.build(buf, endpoint, contentDeserializer);
        }
      } catch (IOException e) {
        if (logger.level <= Logger.SEVERE) logger.log("Exception in deserializer in "+PastImpl.this.endpoint.toString()+":"+instance+" "+e);
        throw e;
      }
      throw new IllegalArgumentException("Unknown type:"+type+" in "+PastImpl.this.toString());
    }     
  }
  public PastImpl(Node node, StorageManager manager, Cache backup, int replicas, String instance, PastPolicy policy, StorageManager trash) {
    this(node, manager, backup, replicas, instance, policy, trash, false);
  }
  
  /**
   * 
   * @param node
   * @param manager
   * @param backup
   * @param replicas
   * @param instance
   * @param policy
   * @param trash
   * @param useOwnSocket send all inserts/fetches over a socket (default is false)
   */
  public PastImpl(Node node, StorageManager manager, Cache backup, int replicas, String instance, PastPolicy policy, StorageManager trash, boolean useOwnSocket) {
    this(node, manager, backup, replicas, instance, policy, trash, new DefaultSocketStrategy(useOwnSocket));
  }
  
  /**
   * Constructor for Past
   *
   * @param node The node below this Past implementation
   * @param manager The storage manager to be used by Past
   * @param replicas The number of object replicas
   * @param instance The unique instance name of this Past
   */
  public PastImpl(Node node, StorageManager manager, Cache backup, int replicas, String instance, PastPolicy policy, StorageManager trash, SocketStrategy strategy) {
    this.environment = node.getEnvironment();
    logger = environment.getLogManager().getLogger(getClass(), instance);
    Parameters p = environment.getParameters();
    MESSAGE_TIMEOUT = p.getInt("p2p_past_messageTimeout");// = 30000;
    SUCCESSFUL_INSERT_THRESHOLD = p.getDouble("p2p_past_successfulInsertThreshold");// = 0.5;
    this.socketStrategy = strategy;
    this.storage = manager;
    this.backup = backup;
    this.contentDeserializer = new JavaPastContentDeserializer();
    this.contentHandleDeserializer = new JavaPastContentHandleDeserializer();
    this.endpoint = node.buildEndpoint(this, instance);
    this.endpoint.setDeserializer(new PastDeserializer());
    this.factory = node.getIdFactory();
    this.policy = policy;
    this.instance = instance;
    this.trash = trash;
    
    this.id = Integer.MIN_VALUE;
    this.outstanding = new Hashtable();
    this.timers = new Hashtable();
    this.replicationFactor = replicas;
    
    //   log.addHandler(new ConsoleHandler());
    //   log.setLevel(Level.FINE);
    //   log.getHandlers()[0].setLevel(Level.FINE);
    
    this.replicaManager = buildReplicationManager(node, instance);
    
    this.lockManager = new LockManagerImpl(environment);
    
    this.endpoint.accept(new AppSocketReceiver() {
      
      public void receiveSocket(AppSocket socket) {
        if (logger.level <= Logger.FINE) logger.log("Received Socket from "+socket);
        socket.register(true, false, 10000, this);
        endpoint.accept(this);
      }    
      
      public void receiveSelectResult(AppSocket socket, boolean canRead,
          boolean canWrite) {        
        if (logger.level <= Logger.FINER) logger.log("Reading from "+socket);
        try {
          ByteBuffer[] bb = (ByteBuffer[])pendingSocketTransactions.get(socket);
          if (bb == null) {
            // this is a new message
            
            // read the size
            bb = new ByteBuffer[1];
            bb[0] = ByteBuffer.allocate(4);
            if (socket.read(bb,0,1) == -1) {
              close(socket);
              return;
            }
            
            // TODO: need to handle the condition where we don't read the whole size...          
            byte[] sizeArr = bb[0].array();            
            int size = MathUtils.byteArrayToInt(sizeArr);
                        
            if (logger.level <= Logger.FINER) logger.log("Found object of size "+size+" from "+socket);
            
            // allocate a buffer to store the object, save it in the pst
            bb[0] = ByteBuffer.allocate(size);
            pendingSocketTransactions.put(socket,bb);
          }
          
          // now we have a bb
          
          // read some bytes
          if (socket.read(bb,0,1) == -1) {
            close(socket);
          }
          
          // deserialize or reregister
          if (bb[0].remaining() == 0) {
            // make sure to clear things up so we can keep receiving          
            pendingSocketTransactions.remove(socket);
            
            if (logger.level <= Logger.FINEST) logger.log("bb[0].limit() "+bb[0].limit()+" bb[0].remaining() "+bb[0].remaining()+" from "+socket);
            
            // deserialize the object
            SimpleInputBuffer sib = new SimpleInputBuffer(bb[0].array());
            
            short type = sib.readShort();
            
            PastMessage result = (PastMessage)endpoint.getDeserializer().deserialize(sib,type,(byte)0,null);
            deliver(null,result);
            
          } 
            
          // there will be more data on the socket if we haven't received everything yet
          // need to register either way to be able to read from the sockets when they are closed remotely, could alternatively close early
          // cause we are currently only sending 1 message per socket, but it's better to just keep reading in case we one day reuse sockets
            socket.register(true, false, 10000, this);

            // recursive call to handle next object
            // cant do this becasue calling read when not ready throws an exception
//          receiveSelectResult(socket, canRead, canWrite);        
        } catch (IOException ioe) {
          receiveException(socket, ioe);
        }
      }

      public void receiveException(AppSocket socket, Exception e) {
        if (logger.level <= Logger.WARNING) logger.logException("Error receiving message",e);
        close(socket);
      }
    
      public void close(AppSocket socket) {
        if (socket == null) return;
        //System.out.println("Closing "+socket);
        pendingSocketTransactions.remove(socket); 
        socket.close();
      }
      
    });
    endpoint.register();
  }

  public String toString() {
    if (endpoint == null) return super.toString();
    return "PastImpl["+endpoint.getInstance()+"]";
  }
  
  public Environment getEnvironment() {
    return environment; 
  }
  
  // ----- INTERNAL METHODS -----

  /**
   * Internal method which builds the replication manager.  Can be overridden by subclasses.
   *
   * @param node The node to base the RM off of
   * @param instance The instance name to use
   * @return The replication manager, ready for use
   */
  protected ReplicationManager buildReplicationManager(Node node, String instance) {
    return new ReplicationManagerImpl(node, this, replicationFactor, instance);
  }
  
  /**
   * Returns of the outstanding messages.  This is a DEBUGGING method ONLY!
   *
   * @return The list of all the outstanding messages
   */
  public Continuation[] getOutstandingMessages() {
    return (Continuation[]) outstanding.values().toArray(new Continuation[0]);
  }
  
  /**
   * Returns the endpoint associated with the Past - ONLY FOR TESTING - DO NOT USE
   *
   * @return The endpoint
   */
  public Endpoint getEndpoint() {
    return endpoint;
  }
  
  /**
   * Returns a new uid for a message
   *
   * @return A new id
   */
  protected synchronized int getUID() {
    return id++;
  }

  /**
   * Returns a continuation which will respond to the given message.
   *
   * @return A new id
   */
  protected Continuation getResponseContinuation(final PastMessage msg) {
    if (logger.level <= Logger.FINER) logger.log("Getting the Continuation to respond to the message " + msg);
    final ContinuationMessage cmsg = (ContinuationMessage) msg;
    
    return new Continuation() {
      public void receiveResult(Object o) {
        cmsg.receiveResult(o);
        endpoint.route(null, cmsg, msg.getSource());
      }

      public void receiveException(Exception e) {
        cmsg.receiveException(e);
        endpoint.route(null, cmsg, msg.getSource());
      }
    };
  }
  
  /**
   * Do like above, but use a socket
   * @param msg
   * @return
   */
  protected Continuation getFetchResponseContinuation(final PastMessage msg) {
    final ContinuationMessage cmsg = (ContinuationMessage) msg;
    
    return new Continuation() {
      public void receiveResult(Object o) {
        cmsg.receiveResult(o);
        PastContent content = (PastContent)o;
        if (socketStrategy.sendAlongSocket(SocketStrategy.TYPE_FETCH, content)) {
          sendViaSocket(msg.getSource(), cmsg, null);
        } else {
          endpoint.route(null, cmsg, msg.getSource());
        }
      }

      public void receiveException(Exception e) {
        cmsg.receiveException(e);
        endpoint.route(null, cmsg, msg.getSource());
      }
    };    
  }
  
  /**
   * AppSocket -> ByteBuffer[]
   * 
   * Used for receiving the objects.
   */
  WeakHashMap pendingSocketTransactions = new WeakHashMap();
  
  private void sendViaSocket(final NodeHandle handle, final PastMessage m, final Continuation c) {
    if (c != null) {
      CancellableTask timer = endpoint.scheduleMessage(new MessageLostMessage(m.getUID(), getLocalNodeHandle(), null, m, handle), MESSAGE_TIMEOUT);
      insertPending(m.getUID(), timer, c);
    }
    
    // create a bb[] to be written    
    SimpleOutputBuffer sob = new SimpleOutputBuffer();
    try {
      sob.writeInt(0); // place holder for size...
      sob.writeShort(m.getType());
      m.serialize(sob);
    } catch (IOException ioe) {
      if (c != null) c.receiveException(ioe); 
    }
    
    // add the size back to the beginning...
    int size = sob.getWritten()-4; // remove the size of the size :)
    if (logger.level <= Logger.FINER) logger.log("Sending size of "+size+" to "+handle+" to send "+m);
    byte[] bytes = sob.getBytes();
    MathUtils.intToByteArray(size,bytes,0);
    
    // prepare the bytes for writing
    final ByteBuffer[] bb = new ByteBuffer[1];
    bb[0] = ByteBuffer.wrap(bytes, 0, sob.getWritten()); // the whole thing
    

    if (logger.level <= Logger.FINE) logger.log("Opening socket to "+handle+" to send "+m);
    endpoint.connect(handle, new AppSocketReceiver() {
    
      public void receiveSocket(AppSocket socket) {
        if (logger.level <= Logger.FINER) logger.log("Opened socket to "+handle+":"+socket+" to send "+m);
        socket.register(false, true, 10000, this);
      }
      
      public void receiveSelectResult(AppSocket socket, boolean canRead,
          boolean canWrite) {
        if (logger.level <= Logger.FINEST) logger.log("Writing to "+handle+":"+socket+" to send "+m);
        
        
        try {
//          ByteBuffer[] outs = new ByteBuffer[1];    
//          ByteBuffer out = ByteBuffer.wrap(endpoint.getLocalNodeHandle().getId().toByteArray());
//          outs[0] = out;
//          socket.write(outs, 0, 1);
          
          socket.write(bb,0,1);
        } catch (IOException ioe) {
          if (c != null) 
            c.receiveException(ioe);
          else
            if (logger.level <= Logger.WARNING) logger.logException("Error sending "+m,ioe);
          return; // don't continue to try to send
        }
        if (bb[0].remaining() > 0) {
          socket.register(false, true, 10000, this);        
        } else {
          socket.close(); 
        }
      }
      
      public void receiveException(AppSocket socket, Exception e) {
        if (c != null) c.receiveException(e);
      }        
    },
    10000);    
  }
  
  
  /**
   * Sends a request message across the wire, and stores the appropriate
   * continuation.
   *
   * @param id The destination id
   * @param message The message to send.
   * @param command The command to run once a result is received
   */
  protected void sendRequest(Id id, PastMessage message, Continuation command) {
    sendRequest(id, message, null, command);
  }
  
  /**
   * Sends a request message across the wire, and stores the appropriate
   * continuation.
   *
   * @param handle The node handle to send directly too
   * @param message The message to send.
   * @param command The command to run once a result is received
   */
  protected void sendRequest(NodeHandle handle, PastMessage message, Continuation command) {
    sendRequest(null, message, handle, command);
  }

  /**
   * Sends a request message across the wire, and stores the appropriate
   * continuation.  Sends the message using the provided handle as a hint.
   *
   * @param id The destination id
   * @param message The message to send.
   * @param handle The first hop hint
   * @param command The command to run once a result is received
   */
  protected void sendRequest(Id id, PastMessage message, NodeHandle hint, Continuation command) {
    if (logger.level <= Logger.FINER) logger.log("Sending request message " + message + " {"+message.getUID()+"} to id " + id + " via " + hint);
    CancellableTask timer = endpoint.scheduleMessage(new MessageLostMessage(message.getUID(), getLocalNodeHandle(), id, message, hint), MESSAGE_TIMEOUT);
    insertPending(message.getUID(), timer, command);
    endpoint.route(id, message, hint);
  }

  /**
   * Loads the provided continuation into the pending table
   *
   * @param uid The id of the message
   * @param command The continuation to run
   */
  private void insertPending(int uid, CancellableTask timer, Continuation command) {
    if (logger.level <= Logger.FINER) logger.log("Loading continuation " + uid + " into pending table");
    timers.put(new Integer(uid), timer);
    outstanding.put(new Integer(uid), command);
  }

  /**
   * Removes and returns the provided continuation from the pending table
   *
   * @param uid The id of the message
   * @return The continuation to run
   */
  private Continuation removePending(int uid) {
    if (logger.level <= Logger.FINER) logger.log("Removing and returning continuation " + uid + " from pending table");
    CancellableTask timer = (CancellableTask) timers.remove(new Integer(uid));
    
    if (timer != null)
      timer.cancel();
    
    return (Continuation) outstanding.remove(new Integer(uid));
  }

  /**
   * Handles the response message from a request.
   *
   * @param message The message that arrived
   */
  private void handleResponse(PastMessage message) {
    if (logger.level <= Logger.FINE) logger.log("handling reponse message " + message + " from the request");
    Continuation command = removePending(message.getUID());

    if (command != null) {
      message.returnResponse(command, environment, instance);
    } 
  }
  
  /**
   * Internal method which returns the handles to an object.  It first checks to see if
   * the handles can be determined locally, and if so, returns.  Otherwise, it sends a
   * LookupHandles messsage out to find out the nodes.
   *
   * @param id The id to fetch the handles for
   * @param max The maximum number of handles to return
   * @param command The command to call with the result (NodeHandle[])
   */
  protected void getHandles(Id id, final int max, Continuation command) {
    NodeHandleSet set = endpoint.replicaSet(id, max);

    if (set.size() == max) {
      command.receiveResult(set);
    } else {
      sendRequest(id, new LookupHandlesMessage(getUID(), id, max, getLocalNodeHandle(), id), new StandardContinuation(command) {
        public void receiveResult(Object o) {
          NodeHandleSet replicas = (NodeHandleSet) o;

          // check to make sure we've fetched the correct number of replicas
          // the deal with this is for the small ring.  If you are requesting 
          // 4 nodes, but the ring is only 3, you are only going to get 3
          // Note: this is still kind of funky, because the replicationFactor+1 
          // argument is kind of weird, but I don't know how to get it right
          // -Jeff 1/24/07
          if (Math.min(max,endpoint.replicaSet(endpoint.getLocalNodeHandle().getId(), replicationFactor+1).size()) > replicas.size()) 
            parent.receiveException(new PastException("Only received " + replicas.size() + " replicas - cannot insert as we know about more nodes."));
          else
            parent.receiveResult(replicas);
        }
      });
    }
  }

  /**
   * Method which inserts the given object into the cache
   *
   * @param content The content to cache
   */
  private void cache(final PastContent content) {
    cache(content, new ListenerContinuation("Caching of " + content, environment));
  }
  
  /**
   * Method which inserts the given object into the cache
   *
   * @param content The content to cache
   * @param command The command to run once done
   */
  public void cache(final PastContent content, final Continuation command) {   
    if (logger.level <= Logger.FINER) logger.log("Inserting PastContent object " + content + " into cache");
    
    if ((content != null) && (! content.isMutable())) 
      storage.cache(content.getId(), null, content, command);
    else
      command.receiveResult(new Boolean(true));
  }
  
  /**
   * Internal method which actually performs an insert for a given object.
   * Here so that subclasses can override the types of insert messages
   * which are sent across the wire.
   *
   * @param obj The object to insert
   * @param builder The object which builds the messages
   * @param command The command to call once done
   * 
   */
  protected void doInsert(final Id id, final MessageBuilder builder, Continuation command, final boolean useSocket) {
    // first, we get all of the replicas for this id
    getHandles(id, replicationFactor+1, new StandardContinuation(command) {
      public void receiveResult(Object o) {
        NodeHandleSet replicas = (NodeHandleSet) o;
        if (logger.level <= Logger.FINER) logger.log("Received replicas " + replicas + " for id " + id);
        
        // then we send inserts to each replica and wait for at least
        // threshold * num to return successfully
        MultiContinuation multi = new MultiContinuation(parent, replicas.size()) {
          public boolean isDone() throws Exception {
            int numSuccess = 0;
            for (int i=0; i<haveResult.length; i++) 
              if ((haveResult[i]) && (Boolean.TRUE.equals(result[i])))  
                numSuccess++;
            
            if (numSuccess >= (SUCCESSFUL_INSERT_THRESHOLD * haveResult.length)) 
              return true;
            
            if (super.isDone()) {
              for (int i=0; i<result.length; i++) 
                if (result[i] instanceof Exception)
                  if (logger.level <= Logger.WARNING) logger.logException("result["+i+"]:",(Exception)result[i]);
              
              throw new PastException("Had only " +  numSuccess + " successful inserts out of " + result.length + " - aborting.");
            }
            return false;
          }
          
          public Object getResult() {
            Boolean[] b = new Boolean[result.length];
            for (int i=0; i<b.length; i++)
              b[i] = new Boolean((result[i] == null) || Boolean.TRUE.equals(result[i]));
            
            return b;
          }
        };
        
        for (int i=0; i<replicas.size(); i++) {
          NodeHandle handle = replicas.getHandle(i);
          PastMessage m = builder.buildMessage();
          Continuation c = new NamedContinuation("InsertMessage to " + replicas.getHandle(i) + " for " + id, multi.getSubContinuation(i));
          if (useSocket) { 
            sendViaSocket(handle, m, c);
          } else {
            sendRequest(handle, m, c);
          }
        }
      }
    });
  }
  
  
  // ----- PAST METHODS -----
  
  /**
   * Inserts an object with the given ID into this instance of Past.
   * Asynchronously returns a PastException to command, if the
   * operation was unsuccessful.  If the operation was successful, a
   * Boolean[] is returned representing the responses from each of
   * the replicas which inserted the object.
   *
   * @param obj the object to be inserted
   * @param command Command to be performed when the result is received
   */
  public void insert(final PastContent obj, final Continuation command) {
    if (logger.level <= Logger.FINER) logger.log("Inserting the object " + obj + " with the id " + obj.getId());
    
    if (logger.level <= Logger.FINEST) logger.log(" Inserting data of class " + obj.getClass().getName() + " under " + obj.getId().toStringFull());

    doInsert(obj.getId(), new MessageBuilder() {
      public PastMessage buildMessage() {
        return new InsertMessage(getUID(), obj, getLocalNodeHandle(), obj.getId());
      }
    }, new StandardContinuation(command) {
      public void receiveResult(final Object array) {
        cache(obj, new SimpleContinuation()  {
          public void receiveResult(Object o) {
            parent.receiveResult(array);
          }
        });
      }
    },
    socketStrategy.sendAlongSocket(SocketStrategy.TYPE_INSERT, obj));
  }

  /**
   * Retrieves the object stored in this instance of Past with the
   * given ID.  Asynchronously returns a PastContent object as the
   * result to the provided Continuation, or a PastException. This
   * method is provided for convenience; its effect is identical to a
   * lookupHandles() and a subsequent fetch() to the handle that is
   * nearest in the network.
   *
   * The client must authenticate the object. In case of failure, an
   * alternate replica of the object can be obtained via
   * lookupHandles() and fetch().
   *
   * This method is not safe if the object is immutable and storage
   * nodes are not trusted. In this case, clients should used the
   * lookUpHandles method to obtains the handles of all primary
   * replicas and determine which replica is fresh in an
   * application-specific manner.
   *
   * @param id the key to be queried
   * @param command Command to be performed when the result is received
   */
  public void lookup(final Id id, final Continuation<PastContent, Exception> command) {
    lookup(id, true, command);
  }
  
  /**
   * Method which performs the same as lookup(), but allows the 
   * callee to specify if the data should be cached.
   *
   * 
   * @param id the key to be queried
   * @param cache Whether or not the data should be cached
   * @param command Command to be performed when the result is received
   */
  public void lookup(final Id id, final boolean cache, final Continuation command) {
    if (logger.level <= Logger.FINER) logger.log(" Performing lookup on " + id.toStringFull());
    
    storage.getObject(id, new StandardContinuation(command) {
      public void receiveResult(Object o) {
        if (o != null) {
          command.receiveResult(o);
        } else {
          // send the request across the wire, and see if the result is null or not
          sendRequest(id, new LookupMessage(getUID(), id, getLocalNodeHandle(), id), new NamedContinuation("LookupMessage for " + id, this) {
            public void receiveResult(final Object o) {
              // if we have an object, we return it
              // otherwise, we must check all replicas in order to make sure that
              // the object doesn't exist anywhere
              if (o != null) {
                // lastly, try and cache object locally for future use
                if (cache) {
                  cache((PastContent) o, new SimpleContinuation()  {
                    public void receiveResult(Object object) {
                      command.receiveResult(o);
                    }
                  });
                } else {
                  command.receiveResult(o);                            
                }
              } else {
                lookupHandles(id, replicationFactor+1, new Continuation() {
                  public void receiveResult(Object o) {
                    PastContentHandle[] handles = (PastContentHandle[]) o;

                    for (int i=0; i<handles.length; i++) {
                      if (handles[i] != null) {
                        fetch(handles[i], new StandardContinuation(parent) {
                          public void receiveResult(final Object o) {
                            // lastly, try and cache object locally for future use
                            if (cache) {
                              cache((PastContent) o, new SimpleContinuation()  {
                                public void receiveResult(Object object) {
                                  command.receiveResult(o);
                                }
                              });
                            } else {
                              command.receiveResult(o);                            
                            }
                          }
                        });
                        
                        return;
                      }
                    }

                    // there were no replicas of the object
                    command.receiveResult(null);
                  }
                  
                  public void receiveException(Exception e) {
                    command.receiveException(e);
                  }
                });
              }
            }
            
            public void receiveException(Exception e) {
              // If the lookup message failed , we then try to fetch all of the handles, just
              // in case.  This may fail too, but at least we tried.
              receiveResult(null);
            }
          });
        }
      }
    });
  }

  /**
   * Retrieves the handles of up to max replicas of the object stored
   * in this instance of Past with the given ID.  Asynchronously
   * returns an array of PastContentHandles as the result to the
   * provided Continuation, or a PastException.
   *
   * Each replica handle is obtained from a different primary storage
   * root for the the given key. If max exceeds the replication factor
   * r of this Past instance, only r replicas are returned.
   *
   * This method will return a PastContentHandle[] array containing all
   * of the handles.
   *
   * @param id the key to be queried
   * @param max the maximal number of replicas requested
   * @param command Command to be performed when the result is received
   */
  public void lookupHandles(final Id id, int max, final Continuation command) {
    if (logger.level <= Logger.FINE) logger.log("Retrieving handles of up to " + max + " replicas of the object stored in Past with id " + id);

    if (logger.level <= Logger.FINER) logger.log("Fetching up to " + max + " handles of " + id.toStringFull());
    
    getHandles(id, max, new StandardContinuation(command) {
      public void receiveResult(Object o) {
        NodeHandleSet replicas = (NodeHandleSet) o;
        if (logger.level <= Logger.FINER) logger.log("Receiving replicas " + replicas + " for lookup Id " + id);
        
        MultiContinuation multi = new MultiContinuation(parent, replicas.size()) {
          public Object getResult() {
            PastContentHandle[] p = new PastContentHandle[result.length];
            
            for (int i=0; i<result.length; i++)
              if (result[i] instanceof PastContentHandle)
                p[i] = (PastContentHandle) result[i];
            
            return p;
          }
        };
        
        for (int i=0; i<replicas.size(); i++) 
          lookupHandle(id, replicas.getHandle(i), multi.getSubContinuation(i));
      }
    });
  }
  
  /**
   * Retrieves the handle for the given object stored on the requested 
   * node.  Asynchronously returns a PostContentHandle (or null) to
   * the provided continuation.
   *
   * @param id the key to be queried
   * @param handle The node on which the handle is requested
   * @param command Command to be performed when the result is received 
   */
  public void lookupHandle(Id id, NodeHandle handle, Continuation command) {
    if (logger.level <= Logger.FINE) logger.log("Retrieving handle for id " + id + " from node " + handle);
    
    sendRequest(handle, new FetchHandleMessage(getUID(), id, getLocalNodeHandle(), handle.getId()), 
                new NamedContinuation("FetchHandleMessage to " + handle + " for " + id, command));
  }

  /**
   * Retrieves the object associated with a given content handle.
   * Asynchronously returns a PastContent object as the result to the
   * provided Continuation, or a PastException.
   *
   * The client must authenticate the object. In case of failure, an
   * alternate replica can be obtained using a different handle obtained via
   * lookupHandles().
   *
   * @param id the key to be queried
   * @param command Command to be performed when the result is received
   */
  public void fetch(PastContentHandle handle, Continuation command) {
    if (logger.level <= Logger.FINE) logger.log("Retrieving object associated with content handle " + handle);

    if (logger.level <= Logger.FINER) logger.log("Fetching object under id " + handle.getId().toStringFull() +  " on " + handle.getNodeHandle());
    
    NodeHandle han = handle.getNodeHandle();
    sendRequest(han, new FetchMessage(getUID(), handle, getLocalNodeHandle(), han.getId()), 
                new NamedContinuation("FetchMessage to " + handle.getNodeHandle() + " for " + handle.getId(), command));
  }

  /**
   * get the nodeHandle of the local Past node
   *
   * @return the nodehandle
   */
  public NodeHandle getLocalNodeHandle() {
    return endpoint.getLocalNodeHandle();
  }

  /**
   * Returns the number of replicas used in this Past
   *
   * @return the number of replicas for each object
   */
  public int getReplicationFactor() {
    return replicationFactor;
  }
  

  // ----- COMMON API METHODS -----

  /**
   * This method is invoked on applications when the underlying node
   * is about to forward the given message with the provided target to
   * the specified next hop.  Applications can change the contents of
   * the message, specify a different nextHop (through re-routing), or
   * completely terminate the message.
   *
   * @param message The message being sent, containing an internal message
   * along with a destination key and nodeHandle next hop.
   *
   * @return Whether or not to forward the message further
   */
  public boolean forward(final RouteMessage message) {
    Message internal;
    try {
      internal = message.getMessage(endpoint.getDeserializer());
    } catch (IOException ioe) {
      throw new RuntimeException(ioe); 
    }
      
    if (internal instanceof LookupMessage) {
      final LookupMessage lmsg = (LookupMessage)internal;
      Id id = lmsg.getId();

      // if it is a request, look in the cache
      if (! lmsg.isResponse()) {
        if (logger.level <= Logger.FINER) logger.log("Lookup message " + lmsg + " is a request; look in the cache");
        if (storage.exists(id)) {
          // deliver the message, which will do what we want
          if (logger.level <= Logger.FINE) logger.log("Request for " + id + " satisfied locally - responding");
          deliver(endpoint.getId(), lmsg);
          return false;
        }
      } 
    } else if (internal instanceof LookupHandlesMessage) {
      LookupHandlesMessage lmsg = (LookupHandlesMessage) internal;
      
      if (! lmsg.isResponse()) {
        if (endpoint.replicaSet(lmsg.getId(), lmsg.getMax()).size() == lmsg.getMax()) {          
          if (logger.level <= Logger.FINE) logger.log("Hijacking lookup handles request for " + lmsg.getId());
          
          deliver(endpoint.getId(), lmsg);
          return false;
        }
      }
    }

    return true;
  }

  /**
   * This method is called on the application at the destination node
   * for the given id.
   *
   * @param id The destination id of the message
   * @param message The message being sent
   */
  public void deliver(Id id, Message message) {
    final PastMessage msg = (PastMessage) message;

    if (msg.isResponse()) {
      handleResponse((PastMessage) message);
    } else {
      if (logger.level <= Logger.INFO) logger.log("Received message " + message + " with destination " + id);
      
      if (msg instanceof InsertMessage) {
        final InsertMessage imsg = (InsertMessage) msg;        
        
        // make sure the policy allows the insert
        if (policy.allowInsert(imsg.getContent())) {
          inserts++;
          final Id msgid = imsg.getContent().getId();
          
          lockManager.lock(msgid, new StandardContinuation(getResponseContinuation(msg)) {

            public void receiveResult(Object result) {
              storage.getObject(msgid, new StandardContinuation(parent) {
                public void receiveResult(Object o) {
                  try {
                    // allow the object to check the insert, and then insert the data
                    PastContent content = imsg.getContent().checkInsert(msgid, (PastContent) o);
                    storage.store(msgid, null, content, new StandardContinuation(parent) {
                      public void receiveResult(Object result) {
                        getResponseContinuation(msg).receiveResult(result);
                        lockManager.unlock(msgid);
                      }
                    });
                  } catch (PastException e) {
                    parent.receiveException(e);
                  }
                }
              });
            }
          });
        } else {
          getResponseContinuation(msg).receiveResult(new Boolean(false));
        }
      } else if (msg instanceof LookupMessage) {
        final LookupMessage lmsg = (LookupMessage) msg;
        lookups++;
        
        // if the data is here, we send the reply, as well as push a cached copy
        // back to the previous node
        storage.getObject(lmsg.getId(), new StandardContinuation(getResponseContinuation(lmsg)) {
          public void receiveResult(Object o) {
            if (logger.level <= Logger.FINE) logger.log("Received object " + o + " for id " + lmsg.getId());

            // send result back
            parent.receiveResult(o);
            
            // if possible, pushed copy into previous hop cache
            if ((lmsg.getPreviousNodeHandle() != null) &&
                (o != null) &&
                (! ((PastContent) o).isMutable())) {
              NodeHandle handle = lmsg.getPreviousNodeHandle();
              if (logger.level <= Logger.FINE) logger.log("Pushing cached copy of " + ((PastContent) o).getId() + " to " + handle);
              
              CacheMessage cmsg = new CacheMessage(getUID(), (PastContent) o, getLocalNodeHandle(), handle.getId());    
              //endpoint.route(null, cmsg, handle);
            }
          }
        });
      } else if (msg instanceof LookupHandlesMessage) {
        LookupHandlesMessage lmsg = (LookupHandlesMessage) msg;
        NodeHandleSet set = endpoint.replicaSet(lmsg.getId(), lmsg.getMax());
        if (logger.level <= Logger.FINER) logger.log("Returning replica set " + set + " for lookup handles of id " + lmsg.getId() + " max " + lmsg.getMax() + " at " + endpoint.getId());
        getResponseContinuation(msg).receiveResult(set);
      } else if (msg instanceof FetchMessage) {
        FetchMessage fmsg = (FetchMessage) msg;
        lookups++;

        Continuation c;
//        c = getResponseContinuation(msg);
        c = getFetchResponseContinuation(msg); // has to be special to determine how to send the message
        
        storage.getObject(fmsg.getHandle().getId(), c);
      } else if (msg instanceof FetchHandleMessage) {
        final FetchHandleMessage fmsg = (FetchHandleMessage) msg;
        fetchHandles++;
        
        storage.getObject(fmsg.getId(), new StandardContinuation(getResponseContinuation(msg)) {
          public void receiveResult(Object o) {
            PastContent content = (PastContent) o;

            if (content != null) {
              if (logger.level <= Logger.FINE) logger.log("Retrieved data for fetch handles of id " + fmsg.getId());
              parent.receiveResult(content.getHandle(PastImpl.this));
            } else {
              parent.receiveResult(null);
            }
          } 
        });
      } else if (msg instanceof CacheMessage) {
        cache(((CacheMessage) msg).getContent());
      } else {
        if (logger.level <= Logger.SEVERE) logger.log("ERROR - Received message " + msg + "of unknown type.");
      }
    }
  } 

  /**
   * This method is invoked to inform the application that the given node
   * has either joined or left the neighbor set of the local node, as the set
   * would be returned by the neighborSet call.
   *
   * @param handle The handle that has joined/left
   * @param joined Whether the node has joined or left
   */
  public void update(NodeHandle handle, boolean joined) {
  }

  
  // ----- REPLICATION MANAGER METHODS -----

  /**
   * This upcall is invoked to tell the client to fetch the given id, 
   * and to call the given command with the boolean result once the fetch
   * is completed.  The client *MUST* call the command at some point in the
   * future, as the manager waits for the command to return before continuing.
   *
   * @param id The id to fetch
   */
  public void fetch(final Id id, NodeHandle hint, Continuation command) {
    if (logger.level <= Logger.FINER) logger.log("Sending out replication fetch request for the id " + id);
    
    policy.fetch(id, hint, backup, this, new StandardContinuation(command) {
      public void receiveResult(Object o) {
        if (o == null) {
          if (logger.level <= Logger.WARNING) logger.log("Could not fetch id " + id + " - policy returned null in namespace " + instance);
          parent.receiveResult(new Boolean(false));
        } else {
          if (logger.level <= Logger.FINEST) logger.log("inserting replica of id " + id);
          
          if (! (o instanceof PastContent))
            if (logger.level <= Logger.WARNING) logger.log("ERROR! Not PastContent " + o.getClass().getName() + " " + o);
          storage.getStorage().store(((PastContent) o).getId(), null, (PastContent) o, parent);
        }
      }
    });
  }
  
  /**
   * This upcall is to notify the client that the given id can be safely removed
   * from the storage.  The client may choose to perform advanced behavior, such
   * as caching the object, or may simply delete it.
   *
   * @param id The id to remove
   */
  public void remove(final Id id, Continuation command) {
    if (backup != null) {                        
      storage.getObject(id, new StandardContinuation(command) {
        public void receiveResult(Object o) {
          backup.cache(id, storage.getMetadata(id), (Serializable) o, new StandardContinuation(parent) {
            public void receiveResult(Object o) {
              storage.unstore(id, parent);
            }
          });
        }
      });
    } else {
      storage.unstore(id, command);
    }
  }
  
  /**
    * This upcall should return the set of keys that the application
   * currently stores in this range. Should return a empty IdSet (not null),
   * in the case that no keys belong to this range.
   *
   * @param range the requested range
   */
  public IdSet scan(IdRange range) {
    return storage.getStorage().scan(range);
  }
  
  /**
   * This upcall should return the set of keys that the application
   * currently stores.  Should return a empty IdSet (not null),
   * in the case that no keys belong to this range.
   *
   * @param range the requested range
   */
  public IdSet scan() {
    return storage.getStorage().scan();
  }
  
  /**
    * This upcall should return whether or not the given id is currently stored
   * by the client.
   *
   * @param id The id in question
   * @return Whether or not the id exists
   */
  public boolean exists(Id id) {
    return storage.getStorage().exists(id);
  }
  
  public void existsInOverlay(Id id, Continuation command) {
    lookupHandles(id, replicationFactor+1, new StandardContinuation(command) {
      public void receiveResult(Object result) {
        Object results[] = (Object[]) result;
        for (int i = 0; i< results.length; i++) {
          if (results[i] instanceof PastContentHandle) {
            parent.receiveResult(Boolean.TRUE);
            return;
          }
        }
        parent.receiveResult(Boolean.FALSE);
      }
    });
  }

  public void reInsert(Id id, Continuation command) {
    storage.getObject(id, new StandardContinuation(command) {
      public void receiveResult(final Object o) {
        insert((PastContent)o, new StandardContinuation(parent) {
          public void receiveResult(Object result) {
            Boolean results[] = (Boolean[])result;
            for (int i = 0; i < results.length; i++) {
              if (results[i].booleanValue()) {
                parent.receiveResult(Boolean.TRUE);
                return;
              }
            }
            parent.receiveResult(Boolean.FALSE);
          }
        });
      }
    });
  }

  // ----- UTILITY METHODS -----
  
  /**
   * Returns the replica manager for this Past instance.  Should *ONLY* be used
   * for testing.  Messing with this will cause unknown behavior.
   *
   * @return This Past's replica manager
   */
  public Replication getReplication() {
    return replicaManager.getReplication();
  }

  /**
   * Returns this Past's storage manager. Should *ONLY* be used
   * for testing.  Messing with this will cause unknown behavior.
   *
   * @return This Past's storage manager.
   */
  public StorageManager getStorageManager() {
    return storage;
  }
  
  /**
   * Class which builds a message
   */
  public interface MessageBuilder {
    public PastMessage buildMessage();
  }
  
  public String getInstance() {
    return instance;
  }
  
  public void setContentDeserializer(PastContentDeserializer deserializer) {
    contentDeserializer = deserializer;
  }

  public void setContentHandleDeserializer(PastContentHandleDeserializer deserializer) {
    contentHandleDeserializer = deserializer;
  }

}
