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

package rice.p2p.scribe;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.*;

import rice.*;
import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.environment.params.Parameters;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.scribe.javaserialized.JavaScribeContentDeserializer;
import rice.p2p.scribe.maintenance.MaintainableScribe;
import rice.p2p.scribe.maintenance.ScribeMaintenancePolicy;
import rice.p2p.scribe.messaging.*;
import rice.p2p.scribe.rawserialization.*;

/**
 * @(#) ScribeImpl.java Thie provided implementation of Scribe.
 *
 * @version $Id: ScribeImpl.java 4495 2008-09-26 14:48:02Z jeffh $
 * @author Alan Mislove
 */
public class ScribeImpl implements Scribe, MaintainableScribe, Application, Observer {
  // Special Log Levels
  public static final int INFO_2 = 850;
  
  /**
   * The interval with which to perform maintenance
   */
  public final int MAINTENANCE_INTERVAL;

  /**
   * the timeout for a subscribe message
   */
  public final int MESSAGE_TIMEOUT;

  /**
   * the hashtable of topic -> TopicManager
   */
  public Hashtable<Topic, TopicManager> topicManagers;

  /**
   * this scribe's policy
   */
  protected ScribePolicy policy;

  /**
   * this scribe's maintenancePolicy
   */
  private ScribeMaintenancePolicy maintenancePolicy;
  
  /**
   * this application's endpoint
   */
  protected Endpoint endpoint;

  /**
   * the local node handle
   */
  protected NodeHandle localHandle;

  /**
   * The hashtable of outstanding lost messags keyed by the UID of the SubscribeMessage
   */
  private HashMap<Integer, SubscribeLostMessage> subscribeLostMessages;

  /**
   * the next unique id
   */
  private int id;
  
  Environment environment;

  Logger logger;
  
  private String instance;
  
  protected Node node;
  
  /**
   * This contains a mapping of child - > all topics for which the local node
   * has this node(hashtable key) as a child
   */
  public HashMap<NodeHandle, Collection<Topic>> allChildren;

  /**
   * This contains a mapping of parent - > all topics for which the local node
   * has this node(hashtable key) as a parent
   */
  public HashMap<NodeHandle, Collection<Topic>> allParents;
  
  /**
   * Topics that we are the root.
   */
  public Set<Topic> roots = new HashSet<Topic>();
  
  /**
   * Topics that (should) have an outsanding subscription.
   * Topics that are in this set have no parent, and we are not the root.
   */
  public Set<Topic> pending = new HashSet<Topic>();
  
  ScribeContentDeserializer contentDeserializer;

  /**
   * Constructor for Scribe, using the default policy.
   *
   * @param node The node below this Scribe implementation
   * @param instance The unique instance name of this Scribe
   */
  public ScribeImpl(Node node, String instance) {
    this(node, new ScribePolicy.DefaultScribePolicy(node.getEnvironment()), instance);
  }

  /**
   * Constructor for Scribe
   *
   * @param node The node below this Scribe implementation
   * @param policy The policy for this Scribe
   * @param instance The unique instance name of this Scribe
   */
  public ScribeImpl(Node node, ScribePolicy policy, String instance) {
    this(node, policy, instance, new ScribeMaintenancePolicy.DefaultScribeMaintenancePolicy(node.getEnvironment()));
  }
  
  /**
   * Constructor for Scribe
   *
   * @param node The node below this Scribe implementation
   * @param policy The policy for this Scribe
   * @param instance The unique instance name of this Scribe
   */
  public ScribeImpl(Node node, ScribePolicy policy, String instance, ScribeMaintenancePolicy maintenancePolicy) {
    this.environment = node.getEnvironment();
    this.node = node;
    logger = environment.getLogManager().getLogger(ScribeImpl.class, instance);
    
    Parameters p = environment.getParameters();
    MAINTENANCE_INTERVAL = p.getInt("p2p_scribe_maintenance_interval");
    MESSAGE_TIMEOUT = p.getInt("p2p_scribe_message_timeout");
    this.allChildren = new HashMap<NodeHandle, Collection<Topic>>();
    this.allParents = new HashMap<NodeHandle, Collection<Topic>>();
    this.instance = instance;
    this.endpoint = node.buildEndpoint(this, instance);
    this.contentDeserializer = new JavaScribeContentDeserializer();
    this.endpoint.setDeserializer(new MessageDeserializer() {
    
      public Message deserialize(InputBuffer buf, short type, int priority,
          NodeHandle sender) throws IOException {
        try {
          switch(type) {
            case AnycastMessage.TYPE:
              return AnycastMessage.build(buf, endpoint, contentDeserializer);
            case SubscribeMessage.TYPE:
              return SubscribeMessage.buildSM(buf, endpoint, contentDeserializer);
            case SubscribeAckMessage.TYPE:
              return SubscribeAckMessage.build(buf, endpoint);
            case SubscribeFailedMessage.TYPE:
              return SubscribeFailedMessage.build(buf, endpoint);
            case DropMessage.TYPE:
              return DropMessage.build(buf, endpoint);
            case PublishMessage.TYPE:
              return PublishMessage.build(buf, endpoint, contentDeserializer);
            case PublishRequestMessage.TYPE:
              return PublishRequestMessage.build(buf, endpoint, contentDeserializer);
            case UnsubscribeMessage.TYPE:
              return UnsubscribeMessage.build(buf, endpoint);
              // new in FP 2.1:
            case AnycastFailureMessage.TYPE:
              return AnycastFailureMessage.build(buf, endpoint, contentDeserializer);
          }
        } catch (IOException e) {
          if (logger.level <= Logger.SEVERE) logger.log("Exception in deserializer in "+ScribeImpl.this.endpoint.toString()+":"+ScribeImpl.this.instance+" "+contentDeserializer+" "+e);
          throw e;
        }
        throw new IllegalArgumentException("Unknown type:"+type);
      }
    
    });
    this.topicManagers = new Hashtable<Topic, TopicManager>();
    this.subscribeLostMessages = new HashMap<Integer, SubscribeLostMessage>();
    this.policy = policy;
    this.maintenancePolicy = maintenancePolicy;
    this.localHandle = endpoint.getLocalNodeHandle();
    this.id = Integer.MIN_VALUE;
    
    endpoint.register();
    
    // schedule the period liveness checks of the parent
    endpoint.scheduleMessage(new MaintenanceMessage(), environment.getRandomSource().nextInt(MAINTENANCE_INTERVAL), MAINTENANCE_INTERVAL);

    if (logger.level <= Logger.FINER) logger.log("Starting up Scribe");
  }

  public Environment getEnvironment() {
    return environment; 
  }
  
  /**
   * Returns the current policy for this scribe object
   *
   * @return The current policy for this scribe
   */
  public ScribePolicy getPolicy() {
    return policy;
  }

  /**
   * Sets the current policy for this scribe object
   *
   * @param policy The current policy for this scribe
   */
  public void setPolicy(ScribePolicy policy) {
    this.policy = policy;
  }

  /**
   * Returns the Id of the local node
   *
   * @return The Id of the local node
   */
  public Id getId() {
    return endpoint.getId();
  }

  public int numChildren(Topic topic) {
    if (topicManagers.get(topic) != null) {
      return ((TopicManager) topicManagers.get(topic)).numChildren();
    }
    return 0;
  }

  /** 
   * Returns true if there is a TopicManager associated with this topic (any
   * parent/children/client exists)
   */
  public boolean containsTopic(Topic topic) {
    if (topicManagers.get(topic) != null) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * Returns the list of clients for a given topic
   *
   * @param topic The topic to return the clients of
   * @return The clients of the topic
   */
//  public ScribeClient[] getClients(Topic topic) {
//    return (ScribeClient[]) getClientsByTopic(topic).toArray(new ScribeClient[0]);
//  }
  
  public Collection<ScribeClient> getClients(Topic topic) { 
    TopicManager manager = topicManagers.get(topic);
    if (manager != null) {
      return getSimpleClients(manager.getClients()); 
    }
    return new ArrayList<ScribeClient>();
  }
  
  public Collection<ScribeMultiClient> getClientsByTopic(Topic topic) { 
    TopicManager manager = topicManagers.get(topic);
    if (manager != null) {
      return manager.getClients(); 
    }
    return new ArrayList<ScribeMultiClient>();
  }
  
  
  /**
   * Adapts an old ScribeClient to a new ScribeMultiClient
   * 
   * This is for reverse compatibility.
   */
  public static class ScribeClientConverter implements ScribeMultiClient {
    WeakReference<ScribeClient> client;
    
    public ScribeClientConverter(ScribeClient client) {
      this.client = new WeakReference<ScribeClient>(client);
    }

    public void subscribeFailed(Collection<Topic> topics) {
      ScribeClient theClient = client.get();
      if (theClient == null) return;
      for (Topic topic : topics) {
        theClient.subscribeFailed(topic); 
      }
    }

    public void subscribeSuccess(Collection<Topic> topics) {
//      System.out.println("ScribeClientConverter.subscribeSuccess("+topics+")");
      // do nothing, this is a new interface
    }

    public boolean anycast(Topic topic, ScribeContent content) {
      ScribeClient theClient = client.get();
      if (theClient == null) return false;      
      return theClient.anycast(topic, content);
    }

    public void childAdded(Topic topic, NodeHandle child) {
      ScribeClient theClient = client.get();
      if (theClient == null) return;      
      theClient.childAdded(topic, child);
    }

    public void childRemoved(Topic topic, NodeHandle child) {
      ScribeClient theClient = client.get();
      if (theClient == null) return;      
      theClient.childRemoved(topic, child);
    }

    public void deliver(Topic topic, ScribeContent content) {
      ScribeClient theClient = client.get();
      if (theClient == null) return;      
      theClient.deliver(topic, content);
    }

    public void subscribeFailed(Topic topic) {
      ScribeClient theClient = client.get();
      if (theClient == null) return;      
      theClient.subscribeFailed(topic);
    }    
  }  
  
  protected Collection<ScribeClient> getSimpleClients(Collection<ScribeMultiClient> multi) {
    ArrayList<ScribeClient> ret = new ArrayList<ScribeClient>(multi.size());
    for(ScribeMultiClient client : multi) {
      if (client instanceof ScribeClientConverter) {
        ScribeClient theClient = ((ScribeClientConverter)client).client.get();
        if (theClient != null) {              
          ret.add(theClient); 
        }
      } else {
        ret.add(client); 
      }
    }
    return ret;
  }
  
  
  private Map<ScribeClient,ScribeClientConverter> clientConverters = new WeakHashMap<ScribeClient,ScribeClientConverter>();
  
  protected ScribeMultiClient getMultiClient(ScribeClient client) {
    if (client instanceof ScribeMultiClient) {
      return (ScribeMultiClient)client;
    } else {
      // it's a simple client
      synchronized(clientConverters) {
        ScribeClientConverter scc = clientConverters.get(client);
        if (scc == null || scc.client.get() == null) {
          scc = new ScribeClientConverter(client);
          clientConverters.put(client, scc);
        }
        return scc;
      }
    }
  }
  
  
  
  /**
   * Returns the list of children for a given topic
   *
   * @param topic The topic to return the children of
   * @return The children of the topic
   */
  public NodeHandle[] getChildren(Topic topic) {
    if (topicManagers.get(topic) != null) {
      return ((TopicManager) topicManagers.get(topic)).getChildren().toArray(new NodeHandle[0]);
    }

    return new NodeHandle[0];
  }

  public Collection<NodeHandle> getChildrenOfTopic(Topic topic) {
    TopicManager manager = topicManagers.get(topic);    
    if (manager != null) {
      return manager.getChildren();
    }

    return new ArrayList<NodeHandle>(0);
  }
  
  
  /**
   * Returns the parent for a given topic
   *
   * @param topic The topic to return the parent of
   * @return The parent of the topic
   */
  public NodeHandle getParent(Topic topic) {
    if (topicManagers.get(topic) != null) {
      return ((TopicManager) topicManagers.get(topic)).getParent();
    }

    return null;
  }

  /**
   * Returns whether or not this Scribe is the root for the given topic
   *
   * @param topic The topic in question
   * @return Whether or not we are currently the root
   */
  public boolean isRoot(Topic topic) {
    NodeHandleSet set = endpoint.replicaSet(topic.getId(), 1);

    if (set.size() == 0)
      return false;
    else
      return set.getHandle(0).getId().equals(endpoint.getId());
  }
  
  public NodeHandle getRoot(Topic topic) {
    NodeHandleSet set = endpoint.replicaSet(topic.getId(), 1);

    if (set.size() == 0)
      return null;
    else
      return set.getHandle(0);
  }
  
  /**
   * Internal methods for sending a subscribe message
   */
  private void sendSubscribe(Topic topic, ScribeMultiClient client, RawScribeContent content, NodeHandle hint) {
    sendSubscribe(Collections.singletonList(topic), client, content, hint);
  }
  
  private void sendSubscribe(List<Topic> topics, ScribeMultiClient client, RawScribeContent content, NodeHandle hint) {
    // remember that we're subscribing
    pending.addAll(topics);

    // choose the UID
    int theId;
    synchronized(this) {
      id++;
      if (id == MAINTENANCE_ID) id++;
      theId = id;
    }
    
    // sort the topics
    if (topics.size() > 1) {
      Collections.sort(topics);
    }
    
    if (logger.level <= Logger.FINEST) logger.log("sendSubscribe("+topics+","+client+","+content+","+hint+") theId:"+theId);
    // schedule a LostMessage
    SubscribeLostMessage slm = new SubscribeLostMessage(localHandle, topics, theId, client);
    CancellableTask task = endpoint.scheduleMessage(slm, MESSAGE_TIMEOUT);    
    slm.putTask(task);
    subscribeLostMessages.put(theId, slm);

    if (hint == null) {
      // break them into groups based on the next hop
      HashMap<NodeHandle, List<Topic>> manifest = buildManifests(topics);
      
      for (NodeHandle nextHop : manifest.keySet()) {
        List<Topic> theTopics = manifest.get(nextHop);
        SubscribeMessage msg = new SubscribeMessage(localHandle, theTopics, theId, convert(policy.divideContent(theTopics, content)));
        
        // if I'm the 1st Replica, then route direct (to prevent problem with consistency)  IE, I know he 
        // is the root for this message, even if he doesn't.  Make sure that if it is re-routed,
        // then the RM actually gets forwarded (in the first hop: NodeHandle is the target; in the second hop: nodeId is the next hop)
        
        // NOTE: this may have to be further thought throught and subdivided
        NodeHandleSet set = endpoint.replicaSet(msg.getTopic().getId(), 2);
        if (set.size() > 1 && set.getHandle(1) == localHandle) {
          endpoint.route(null, msg, nextHop);      
        } else {
          endpoint.route(msg.getTopic().getId(), msg, nextHop);                
        }
      }    
    } else {
      // use the hint 
      SubscribeMessage msg = new SubscribeMessage(localHandle, topics, theId, content);
      
      // see if hint is my direct neighbor, if so, route only directly, so that he will accept the connection even
      // though he may still be joining
      NodeHandleSet set = endpoint.replicaSet(msg.getTopic().getId(), 2);
      if (set.size() > 1 && set.getHandle(1) == localHandle) {
        endpoint.route(null, msg, hint);            
      } else {
        endpoint.route(msg.getTopic().getId(), msg, hint);                    
      }
    }
  }

  /**
   * This checks the underlieing overlay for the next hop for each of a 
   * number of topics and groups them accordingly.  Uses the Local NodeHandle 
   * for topics rooted here.
   * 
   * Maintains the order of the Topics
   * 
   * @return 
   */
  private HashMap<NodeHandle, List<Topic>> buildManifests(List<Topic> topics) {
    HashMap<NodeHandle, List<Topic>> manifest = new HashMap<NodeHandle, List<Topic>>();
    
    for (Topic topic : topics) {
      NodeHandleSet handleSet = endpoint.replicaSet(topic.getId(), 1);
      if (handleSet.size() == 0) {        
        handleSet = endpoint.localLookup(topic.getId(), 1, false);
      }
      
      // no next hop = we are the next hop
      NodeHandle handle = null;
      if (handleSet.size() > 0) handle = handleSet.getHandle(0);
      
      if (handle == null) {
        handle = this.localHandle;
        if (!isRoot(topic)) {
          if (logger.level <= Logger.WARNING) {            
            logger.log("buildManifests("+topics+") did not receive a next hop for topic "+topic+" but we are not the root of the topic. isRoot = "+isRoot(topic));
            // print handleSet
            logger.log("handle set:"+handleSet);
            // print leafset
            logger.log(node.printRouteState());
          }
        }
      }
      
      List<Topic> theTopics = manifest.get(handle);
      if (theTopics == null) {
        theTopics = new ArrayList<Topic>();
        manifest.put(handle, theTopics);
      }
      theTopics.add(topic);
    }
    
    if (logger.level <= Logger.FINEST) {
      logger.log("buildManifest()");
      for (NodeHandle node : manifest.keySet()) {
        logger.log("  "+node+" "+manifest.get(node));
      }
    }
    
    return manifest;    
  }
  
  /**
   * Internal method which processes an ack message
   *
   * @param message The ackMessage
   */
  protected void ackMessageReceived(SubscribeAckMessage message) {
    if (logger.level <= Logger.FINEST) logger.log("ackMessageReceived("+message+")");
//    ScribeClient client = (ScribeClient) outstanding.remove(new Integer(message.getId()));
    SubscribeLostMessage slm = subscribeLostMessages.get(message.getId());
    if (slm == null) {
      if (logger.level <= Logger.FINE) logger.log("ackMessageReceived("+message+") for unknown id");
    } else {
      ScribeMultiClient multiClient = slm.getClient();
      if (multiClient != null) multiClient.subscribeSuccess(message.getTopics());
      if (slm.topicsAcked(message.getTopics())) {      
        if (logger.level <= Logger.FINER) {
            logger.log("Removing client " + slm.getClient() + " from list of outstanding for ack " + message.getId());
        }
        subscribeLostMessages.remove(message.getId()).cancel();
      } else {
         if (logger.level <= Logger.FINER) {
           Collection<Topic> topics = slm.getTopics();
           int numTopics = topics.size();
           logger.log("Still waiting for SubscribeAck from "+
               (numTopics == 1 ? 
                   " topic "+topics.iterator().next() +"." : 
                   numTopics+" topics.")); 
         }
      }
    }
  }

  /**
   * Internal method which processes a subscribe failed message
   *
   * @param message THe lost message
   */
  private void failedMessageReceived(SubscribeFailedMessage message) {
    //ScribeClient client = (ScribeClient) outstanding.remove(new Integer(message.getId()));
    SubscribeLostMessage slm = subscribeLostMessages.get(message.getId());
    if (slm == null) {
      if (logger.level <= Logger.WARNING) logger.log("received unexpected subscribe failed message, ignoring:"+message);
      return;
    }
    // only remove it if this message covers everything
    if (slm.topicsAcked(message.getTopics())) {      
      subscribeLostMessages.remove(message.getId()).cancel();      
    }
    
    ScribeMultiClient client = slm.getClient();
    
    if (logger.level <= Logger.FINER) logger.log("Telling client " + client + " about FAILURE for outstanding ack " + message.getId());
    
    if (client != null) {
      client.subscribeFailed(message.getTopics());
    } else {
      maintenancePolicy.subscribeFailed(this, message.getTopics()); 
    }
  }

  /**
   * Internal method which processes a subscribe lost message
   *
   * @param message THe lost message
   */
  private void lostMessageReceived(SubscribeLostMessage message) {
//    ScribeClient client = (ScribeClient) outstanding.remove(message.getId());
    SubscribeLostMessage slm = subscribeLostMessages.remove(message.getId());
    ScribeMultiClient client = slm.getClient();
    
    if (logger.level <= Logger.FINER) logger.log("Telling client " + client + " about LOSS for outstanding ack " + message.getId());
    
    ArrayList<Topic> failedTopics = new ArrayList<Topic>();
    for (Topic topic : message.getTopics()) {
      NodeHandle parent = getParent(topic);
      if (!isRoot(topic) && (parent == null))
        failedTopics.add(topic);
    }
    
    if (!failedTopics.isEmpty()) {
      if (client != null) {
        client.subscribeFailed(failedTopics);
      } else {
        maintenancePolicy.subscribeFailed(this, failedTopics); 
      }
    }
  }

  // ----- SCRIBE METHODS -----
  public boolean containsChild(Topic topic, NodeHandle child) {
    TopicManager manager = (TopicManager) topicManagers.get(topic);
    if (manager == null) {
      return false;
    } else {
      return manager.containsChild(child);

    }
  }

  public void subscribe(Collection<Topic> topics) {
    doSubscribe(topics, null, null, null);
  }
  
  /**
   * Subscribes the given client to the provided topic. Any message published to the topic will be
   * delivered to the Client via the deliver() method.
   *
   * @param topic The topic to subscribe to
   * @param client The client to give messages to
   */
  public void subscribe(Topic topic, ScribeMultiClient client) {
    doSubscribe(Collections.singletonList(topic), client, null, null);
  }

  /**
   * Subscribes the given client to the provided topic. Any message published to the topic will be
   * delivered to the Client via the deliver() method.
   *
   * @param topic The topic to subscribe to
   * @param client The client to give messages to
   */
  // ************* deprecate this ***********
  public void subscribe(Topic topic, ScribeClient client) {
    doSubscribe(Collections.singletonList(topic), getMultiClient(client), null, null);
  }
  public void subscribe(Topic topic, ScribeClient client, ScribeContent content) {
    doSubscribe(Collections.singletonList(topic), getMultiClient(client), toRawScribeContent(content), null);
  }
  public void subscribe(Topic topic, ScribeClient client, ScribeContent content, NodeHandle hint) {
    doSubscribe(Collections.singletonList(topic), getMultiClient(client), toRawScribeContent(content), hint);
  }
  public void subscribe(Topic topic, ScribeClient client, RawScribeContent content) {
    doSubscribe(Collections.singletonList(topic), getMultiClient(client), content, null);    
  }
  public void subscribe(Topic topic, ScribeClient client, RawScribeContent content, NodeHandle hint) {
    doSubscribe(Collections.singletonList(topic), getMultiClient(client), content, hint);
  }  
  public void subscribe(Collection<Topic> theTopics, ScribeClient client, RawScribeContent content, NodeHandle hint) {
    doSubscribe(theTopics, getMultiClient(client), content, hint);       
  }  
  public void subscribe(Collection<Topic> theTopics, ScribeClient client, ScribeContent content, NodeHandle hint) {
    doSubscribe(theTopics, getMultiClient(client), toRawScribeContent(content), hint);       
  }  
  // ***************** end deprecation
  public void subscribe(Topic topic, ScribeMultiClient client, ScribeContent content, NodeHandle hint) {
    doSubscribe(Collections.singletonList(topic), client, toRawScribeContent(content), hint);
  }
  public void subscribe(Topic topic, ScribeMultiClient client, RawScribeContent content, NodeHandle hint) {
    doSubscribe(Collections.singletonList(topic), client, content, hint);
  }  
  public void subscribe(Collection<Topic> theTopics, ScribeMultiClient client, ScribeContent content, NodeHandle hint) {
    doSubscribe(theTopics, client, toRawScribeContent(content), hint);       
  }
  public void subscribe(Collection<Topic> theTopics, ScribeMultiClient client, RawScribeContent content, NodeHandle hint) {
    doSubscribe(theTopics, client, content, hint);         
  }
  
  protected RawScribeContent toRawScribeContent(ScribeContent content) {
    return content instanceof RawScribeContent ? (RawScribeContent)content : new JavaSerializedScribeContent(content);
  }
  
  /**
   * This method prevents re-subscription to topics you are already a part of.
   * However it allows subscription if you were the root, but there is now a root.
   * 
   * @param theTopics
   * @param client
   * @param content
   * @param hint
   */
  protected void doSubscribe(Collection<Topic> theTopics, ScribeMultiClient client, RawScribeContent content, NodeHandle hint) {
    if (logger.level <= Logger.FINER) logger.log(
        "Subscribing client " + client + " to " + theTopics +".");

    List<Topic> toSubscribe = new ArrayList<Topic>();
    List<Topic> alreadySubscribed = new ArrayList<Topic>();

    synchronized(topicManagers) {
      for (Topic topic : theTopics) {
        TopicManager manager = topicManagers.get(topic);
        
        // if we don't know about this topic, subscribe
        // otherwise, we simply add the client to the list
        if (manager == null) {
          manager = new TopicManager(topic);
          topicManagers.put(topic, manager);        
          toSubscribe.add(topic);
        } else {
          if ((manager.getParent() == null) && (! isRoot(topic))) {
            toSubscribe.add(topic);
          } else {
            // else, no need to subscribe
            alreadySubscribed.add(topic);
          }
        }
        manager.addClient(client);
      }
    }
    
    // we may need to make this call on the Selector thread for better consistency
    if (client != null) {
      if (!alreadySubscribed.isEmpty()) client.subscribeSuccess(alreadySubscribed);
    }
    
    if (toSubscribe.isEmpty()) return;
      
    sendSubscribe(toSubscribe, client, content, hint);
  }

  
  /**
   * Unsubscribes the given client from the provided topic.getId
   *
   * @param topic The topic to unsubscribe from
   * @param client The client to unsubscribe
   */
  public void unsubscribe(Topic topic, ScribeClient client) {
    unsubscribe(Collections.singletonList(topic), getMultiClient(client));
  }
  
  public void unsubscribe(Topic topic, ScribeMultiClient client) {
    unsubscribe(Collections.singletonList(topic), client);
  }
  /**
   * This code:
   * for each Topic:
   *   1) removes the client from the TopicManager
   *   2) 
   */
  public void unsubscribe(Collection<Topic> topicsToUnsubscribe, ScribeMultiClient client) {
    if (logger.level <= Logger.FINER) logger.log("Unsubscribing client " + client + " from topic " + topicManagers);
    HashMap<NodeHandle, List<Topic>> needToUnsubscribe = new HashMap<NodeHandle, List<Topic>>();

    synchronized(topicManagers) {
      for (Topic topic : topicsToUnsubscribe) {
        TopicManager manager = (TopicManager) topicManagers.get(topic);
        
        if (manager != null) {
    
          NodeHandle parent = manager.getParent();
          
          // if this is the last client and there are no children,
          // then we unsubscribe from the topic
          if (manager.removeClient(getMultiClient(client))) {
            if(logger.level <= Logger.INFO) logger.log("Removing TopicManager for topic: " + topic);
            
            topicManagers.remove(topic);
    
            // After we remove the topicManager we must call updateParents() to remove the parent from the parent dat structure
            removeFromAllParents(topic, parent);   
            
            if (parent != null) {
              List<Topic> theTopics = needToUnsubscribe.get(parent);
              if (theTopics == null) {
                theTopics = new ArrayList<Topic>();
                needToUnsubscribe.put(parent, theTopics);
              }
              theTopics.add(topic);
            }
          }
        } else {
          if (logger.level <= Logger.WARNING) logger.log("Attempt to unsubscribe client " + client + " from unknown topic " + topic);
        }
      }
    }
    
    for (NodeHandle parent : needToUnsubscribe.keySet()) {
      endpoint.route(null, new UnsubscribeMessage(localHandle, needToUnsubscribe.get(parent)), parent);
    }
  }

  /**
   * Publishes the given message to the topic.
   *
   * @param topic The topic to publish to
   * @param content The content to publish
   */
  public void publish(Topic topic, ScribeContent content) {
    publish(topic, content instanceof RawScribeContent ? (RawScribeContent)content : new JavaSerializedScribeContent(content));
  }
  
  public void publish(Topic topic, RawScribeContent content) {
    if (logger.level <= Logger.FINER) logger.log("Publishing content " + content + " to topic " + topic);

    endpoint.route(topic.getId(), new PublishRequestMessage(localHandle, topic, content), null);
  }

  /**
   * Anycasts the given content to a member of the given topic
   *
   * @param topic The topic to anycast to
   * @param content The content to anycast
   */
  public void anycast(Topic topic, ScribeContent content) {
    anycast(topic, content, null);     
  }
  
  public void anycast(Topic topic, ScribeContent content, NodeHandle hint) {
    if (content instanceof RawScribeContent) {
      anycast(topic, (RawScribeContent)content, hint);
    } else {
      anycast(topic, new JavaSerializedScribeContent(content), hint);
    }
  }
  
  public void anycast(Topic topic, RawScribeContent content) {
    anycast(topic, content, null);     
  }

  public void anycast(Topic topic, RawScribeContent content, NodeHandle hint) {
    if (logger.level <= Logger.FINER)
      logger.log("Anycasting content " + content
          + " to topic " + topic + " with hint " + hint);

    AnycastMessage aMsg = new AnycastMessage(localHandle, topic, content);
    
    policy.directAnycast(aMsg, getParent(topic), getChildrenOfTopic(topic));
    
    if (hint == null || localHandle.equals(hint)) {
      // There is a bug in Freepastry where if a message is routed with a hint
      // equal to itself then even if the node is not the destimation for the id
      // field, the deliver() method will be called

      endpoint.route(topic.getId(), aMsg, null);
    } else {

      endpoint.route(topic.getId(), aMsg, hint);
    }
  }
  
  /**
   * Adds a child to the given topic
   *
   * @param topic The topic to add the child to
   * @param child The child to add
   */
  public void addChild(Topic topic, NodeHandle child) {
    if (addChildHelper(topic, child)) {
      // a new TopicManager was created
      subscribe(Collections.singletonList(topic), null, maintenancePolicy.implicitSubscribe(Collections.singletonList(topic)), null); 
    }
    
    // note, there is a bit of a synchronization issue between this call and the addChildHelper call, but I don't want
    // to be holding a lock when calling addChildHelper, because it calls in to user code
    TopicManager manager = getTopicManager(topic);
    
    // we send a confirmation back to the child
    endpoint.route(null, new SubscribeAckMessage(localHandle, 
        Collections.singletonList(topic), 
        Collections.singletonList(manager.getPathToRoot()), MAINTENANCE_ID), child);
  }
   
  public void setParent(Topic topic, NodeHandle parent, List<Id> pathToRoot) {
    TopicManager manager = getTopicManager(topic);
    manager.setParent(parent, pathToRoot);
  }
  
  /**
   * Lazy constructor.  
   * @param topic
   * @return never null
   */
  public TopicManager getTopicManager(Topic topic) {
    synchronized(topicManagers) {
      TopicManager manager = (TopicManager) topicManagers.get(topic);
  
      // if we don't know about the topic, we subscribe, otherwise,
      // we simply add the child to the list
      if (manager == null) {
        manager = new TopicManager(topic);
        topicManagers.put(topic, manager);
      }
      return manager;    
    }
  }
  
  /**
   * Adds a child to the given topic, using the specified sequence number in the ack message
   * sent to the child.
   *
   * @param topic The topic
   * @param child THe child to add
   * @param id The seuqnce number
   * @return true if we need to subscribe to this topic because implicitly subscribing
   */
  protected boolean addChildHelper(Topic topic, NodeHandle child) {
    if (logger.level <= Logger.FINER) logger.log("addChild("+topic+","+child+","+id+")");

    boolean ret = false;
    
    TopicManager manager;
    
    List<ScribeMultiClient> clientList;
    synchronized(topicManagers) {
      // we can't use 
      manager = (TopicManager) topicManagers.get(topic);
  
      // if we don't know about the topic, we subscribe, otherwise,
      // we simply add the child to the list
      if (manager == null) {
        manager = new TopicManager(topic);
        topicManagers.put(topic, manager);
  
        if (logger.level <= Logger.FINER) logger.log("Implicitly subscribing to topic " + topic);
        ret = true;
      }
      
      // need to be holding topicManagers for the call to addToAllChildren
      manager.addChild(child);
      clientList = new ArrayList<ScribeMultiClient>(manager.getClients());
    }
    

    // and lastly notify the policy and all of the clients
    policy.childAdded(topic, child);

    for (ScribeMultiClient client : clientList) { 
      client.childAdded(topic, child);
    }
    
    return ret;
  }

  /**
   * Removes a child from the given topic
   *
   * @param topic The topic to remove the child from
   * @param child The child to remove
   */
  public void removeChild(Topic topic, NodeHandle child) {
    removeChild(topic, child, true);
  }

  /**
   * Removes a child from the given topic
   *
   * @param topic The topic to remove the child from
   * @param child The child to remove
   * @param sendDrop Whether or not to send a drop message to the chil
   */
  protected void removeChild(Topic topic, NodeHandle child, boolean sendDrop) {
    if (logger.level <= Logger.FINE) logger.log("Removing child " + child + " from topic " + topic);

    boolean sendUnsubscribe = false;
    
    TopicManager manager;
    
    synchronized(topicManagers) {
      manager = (TopicManager) topicManagers.get(topic);
      if (manager != null) {
        // if this is the last child and there are no clients, then
        // we unsubscribe, if we are not the root
        NodeHandle parent = manager.getParent();
        
        sendUnsubscribe = manager.removeChild(child); 
        if (sendUnsubscribe) {
          if (logger.level <= Logger.INFO)
            logger.log("Removing TopicManager for topic: " + topic);
          
          topicManagers.remove(topic);
          // Since we will be removing the topicManager which will also get rid of
          // the parent, we need to remove the stale parent from the global data
          // structure of parent -> topics
          removeFromAllParents(topic, parent);
        }
      }
    }
    
    if (manager != null) {
      NodeHandle parent = manager.getParent();
      if (sendUnsubscribe) {
        if (logger.level <= Logger.FINE) logger.log("We no longer need topic " + topic + " - unsubscribing from parent " + parent);

        if (parent != null) {
          endpoint.route(null, new UnsubscribeMessage(localHandle, Collections.singletonList(topic)), parent);
        }
      }

      if ((sendDrop) && (child.isAlive())) {
        if (logger.level <= Logger.FINE) logger.log("Informing child " + child + " that he has been dropped from topic " + topic);
        
        // now, we tell the child that he has been dropped
        endpoint.route(null, new DropMessage(localHandle, topic), child);
      }

      // and lastly notify the policy and all of the clients
      policy.childRemoved(topic, child);
            
      List<ScribeMultiClient> clientList;
      synchronized(topicManagers) {
        clientList = new ArrayList<ScribeMultiClient>(manager.getClients());
      }
      for (ScribeMultiClient client : clientList) { 
        client.childRemoved(topic, child);
      }
    } else {
      if (logger.level <= Logger.WARNING) logger.log("Unexpected attempt to remove child " + child + " from unknown topic " + topic);
    }
  }
  
  /**
   * Returns the list of topics the given client is subscribed
   * to.
   *
   * @param client The client in question
   * @return The list of topics
   */
  public Collection<Topic> getTopicsByClient(ScribeClient client) {    
    ArrayList<Topic> result = new ArrayList<Topic>();
    
    for (TopicManager topicManager : topicManagers.values()) {
      if (topicManager.containsClient(getMultiClient(client)))
        result.add(topicManager.getTopic());
    }
    
    return result;
  }

  public Collection<Topic> getTopicsByClient(ScribeMultiClient client) {    
    ArrayList<Topic> result = new ArrayList<Topic>();
    
    for (TopicManager topicManager : topicManagers.values()) {
      if (topicManager.containsClient(client))
        result.add(topicManager.getTopic());
    }
    
    return result;
  }
  
  public Topic[] getTopics(ScribeClient client) {
    return (Topic[])getTopicsByClient(client).toArray();
  }
  
  protected void recvAnycastFail(Topic topic, NodeHandle failedAtNode,
      ScribeContent content) {
    if (logger.level <= Logger.FINE)
      logger.log("received anycast failure message from " + failedAtNode
          + " for topic " + topic);
    policy.recvAnycastFail(topic, failedAtNode, content);
  }
  
  /**
   * This method should be invoked after the state change in the Topic Manager
   * has been made. This helps us to know the current state of the system and
   * thus generate WARNING messages only in cases of redundancy
   * 
   * Need to be holding lock: topicManagers
   * 
   * @param t
   * @param child
   */
  protected void addToAllChildren(Topic t, NodeHandle child) {
    if (logger.level <= Logger.INFO) logger.log("addToAllChildren("+t+","+child+")");
    
    // Child added
    Collection<Topic> topics = allChildren.get(child);
    
    if (topics == null) {
      if (child.isAlive()) {
        if (!allParents.containsKey(child)) child.addObserver(this);
      } else {    
        if (logger.level <= Logger.WARNING) logger.logException("addToAllChildren("+t+","+child+") child.isAlive() == false", new Exception("Stack Trace"));         
      }
      topics = new ArrayList<Topic>();
      allChildren.put(child, topics);
    }
    if (!topics.contains(t)) {
      topics.add(t);
    }
  }
  
  /**
   * Need to be holding lock: topicManagers
   * 
   * @param t
   * @param child
   */
  protected void removeFromAllChildren(Topic t, NodeHandle child) {
    if (logger.level <= Logger.INFO) logger.log("removeFromAllChildren("+t+","+child+")");
    
    // Child added
    Collection<Topic> topics = allChildren.get(child);
    
    if (topics == null) {
      return;
    }
    topics.remove(t);
    
    if (topics.isEmpty()) {
      allChildren.remove(child); 
      if (!allParents.containsKey(child)) child.deleteObserver(this);

    }
  }

  // This method should be invoked after the state change in the Topic Manager
  // has been made. This helps us to know the current state of the system and
  // thus generate WARNING messages only in cases of redundancy
  protected void addToAllParents(Topic t, NodeHandle parent) {
    if (logger.level <= Logger.INFO) logger.log("addToAllParents("+t+","+parent+")");
    
    if (parent == null || parent.equals(localHandle)) {
      if (isRoot(t)) {
        roots.add(t);
      }
      // This could very well be the case, but in such instances we need not do
      // anything
      return;
    }

    // Parent added
    Collection<Topic> topics = allParents.get(parent);
    if (topics == null) {
      if (parent.isAlive()) {
        if (!allChildren.containsKey(parent)) parent.addObserver(this);
      } else {
        if (logger.level <= Logger.WARNING) logger.logException("addToAllParents("+t+","+parent+") parent.isAlive() == false", new Exception("Stack Trace"));         
      }
      topics = new ArrayList<Topic>();
      allParents.put(parent, topics);
    }
    
    if (!topics.contains(t)) {
      topics.add(t);
    }
  }
  
  protected void removeFromAllParents(Topic t, NodeHandle parent) {
    if (logger.level <= Logger.INFO) logger.log("removeFromAllParents("+t+","+parent+")");
    
    if (parent == null || parent.equals(localHandle)) {
      // This could very well be the case, but in such instances we need not do
      // anything
      roots.remove(t);
      pending.remove(t);

      return;
    }

    // Parent removed
    Collection<Topic> topics = allParents.get(parent);
    
    if (topics == null) {
      return;
    }
    topics.remove(t);
    
    if (topics.isEmpty()) {
      allParents.remove(parent); 
      if (!allChildren.containsKey(parent)) parent.deleteObserver(this);
    }
  }

  public boolean allParentsContains(Topic t, NodeHandle parent) {
    if (parent == null) {
      return false;
    }
    if (allParents.containsKey(parent)) {
      Vector topics = (Vector) allParents.get(parent);
      if (topics.contains(t)) {
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  public boolean allParentsContainsParent(NodeHandle parent) {
    if (parent == null) {
      return false;
    }
    if (allParents.containsKey(parent)) {
      return true;
    } else {
      return false;
    }
  }

  public void printAllParentsDataStructure() {
    String s = "printAllParentsDataStructure()";
    for (NodeHandle parent: allParents.keySet()) {
      s+="\n  parent: " + parent + " (Topics,TopicExists,ActualParent) are as follows: ";
      for (Topic t : allParents.get(parent)) {
        boolean topicExists = containsTopic(t);
        NodeHandle actualParent = getParent(t);
          s+="\n    (" + t + ", " + topicExists + ", " + actualParent + ")";
      }
    }
  }

  public void printAllChildrenDataStructure() {
    String s = "printAllChildrenDataStructure()";
    for (NodeHandle child: allChildren.keySet()) {
      s+="\n  child: " + child + " (Topics,TopicExists, containsChild) are as follows: ";
      for (Topic t : allChildren.get(child)) {
        boolean topicExists = containsTopic(t);
        boolean containsChild = containsChild(t, child);
        s+="\n    (" + t + ", " + topicExists + ", " + containsChild + ")";
      }
    }
  }

  // This returns the topics for which the parameter 'parent' is a Scribe tree parent of the local node
  public Collection<Topic> getTopicsByParent(NodeHandle parent) {
    // handle local node as null or the localHandle
    if (parent == null) parent = localHandle;
    if (parent.equals(localHandle)) return roots;
    
    Collection<Topic> topic = allParents.get(parent);
    if (topic == null) {
      return Collections.emptyList(); 
    }
   
    return topic;
  }

  // This returns the topics for which the parameter 'child' is a Scribe tree child of the local node
  public Collection<Topic> getTopicsByChild(NodeHandle child) {
    if (child.equals(localHandle)) {
      if (logger.level <= Logger.WARNING) logger.log("ScribeImpl.getTopicsByChild() called with localHandle! Why would you do that?");
    }
    
    Collection<Topic> topic = allChildren.get(child);
    if (topic == null) {
      return Collections.emptyList();
    }
    return topic;
  }
  

  // ----- COMMON API METHODS -----

  /**
   * This method is invoked on applications when the underlying node is about to forward the given
   * message with the provided target to the specified next hop. Applications can change the
   * contents of the message, specify a different nextHop (through re-routing), or completely
   * terminate the message.
   *
   * @param message The message being sent, containing an internal message along with a destination
   *      key and nodeHandle next hop.
   * @return Whether or not to forward the message further
   */
  public boolean forward(final RouteMessage message) {
    
    Message internalMessage;
    try {
      internalMessage = message.getMessage(endpoint.getDeserializer());
    } catch (IOException ioe) {
      throw new RuntimeException(ioe); 
    }
    if (logger.level <= Logger.FINEST) logger.log("Forward called with "+internalMessage+" "+internalMessage.getClass().getName());
    if(internalMessage instanceof ScribeMessage) {
      policy.intermediateNode((ScribeMessage)internalMessage);
    }
    
    if (internalMessage instanceof AnycastMessage) {
      AnycastMessage aMessage = (AnycastMessage) internalMessage;
      
      // get the topic manager associated with this topic
      if (aMessage.getTopic() == null) {
        throw new RuntimeException("topic is null!");
      }
      TopicManager manager = (TopicManager) topicManagers.get(aMessage.getTopic());

      // if it's a subscribe message, we must handle it differently
      if (internalMessage instanceof SubscribeMessage) {
        SubscribeMessage sMessage = (SubscribeMessage) internalMessage;
        return handleForwardSubscribeMessage(sMessage);        
      } else {
        // Note that since forward() is called also on the outgoing path, it
        // could be that the last visited node of the anycast message is itself,
        // then in that case we return true
        if (logger.level <= Logger.FINER) logger.log("DEBUG: Anycast message.forward(1)");
        // There is a special case in the modified exhaustive anycast traversal
        // algorithm where the traversal ends at the node which happens to be
        // the best choice in the bag of prospectiveresponders. In this scenario
        // the local node's anycast() method will be visited again

        if (endpoint.getLocalNodeHandle().equals(aMessage.getLastVisited())
            && !endpoint.getLocalNodeHandle().equals(
                aMessage.getInitialRequestor())) {
          if (logger.level <= Logger.FINER) {
            logger.log("Bypassing forward logic of anycast message becuase local node is the last visited node "
                    + aMessage.getLastVisited() + " of in the anycast message ");
            if (isRoot(aMessage.getTopic())) {
                logger.log("Local node is the root of anycast group "
                    + aMessage.getTopic());
            }
          }
          return true;
        }
        
        // if we are not associated with this topic at all, let the
        // anycast continue
        if (manager == null) {
          if (logger.level <= Logger.FINER)
            logger.log("Manager of anycast group is null");
          return true;
        }

        Collection<ScribeMultiClient> clients = manager.getClients();

        // see if one of our clients will accept the anycast        
        for (ScribeMultiClient client : clients) {
          if (client.anycast(aMessage.getTopic(), aMessage.getContent())) {
            if (logger.level <= Logger.FINER) logger.log("Accepting anycast message from " +
              aMessage.getSource() + " for topic " + aMessage.getTopic());

            return false;
          }
        }

        // if we are the orginator for this anycast and it already has a destination,
        // we let it go ahead
        if (aMessage.getSource().getId().equals(endpoint.getId()) &&
            (message.getNextHopHandle() != null) &&
            (!localHandle.equals(message.getNextHopHandle()))) {
          if (logger.level <= Logger.FINER)
            logger.log("DEBUG: Anycast message.forward(2), before returning true");
          return true;
        }

        if (logger.level <= Logger.FINER) logger.log("Rejecting anycast message from " +
          aMessage.getSource() + " for topic " + aMessage.getTopic());
      }

      // add the local node to the visited list
      aMessage.addVisited(endpoint.getLocalNodeHandle());

      // allow the policy to select the order in which the nodes are visited
      policy.directAnycast(aMessage, manager.getParent(), manager.getChildren());

      // reset the source of the message to be us
      aMessage.setSource(endpoint.getLocalNodeHandle());

      // get the next hop
      NodeHandle handle = aMessage.getNext();

      // make sure that the next node is alive
      while ((handle != null) && (!handle.isAlive())) {
        handle = aMessage.getNext();
      }

      if (logger.level <= Logger.FINER) logger.log("Forwarding anycast message for topic " + aMessage.getTopic() + "on to " + handle);

      if (handle == null) {
        if (logger.level <= Logger.FINE) logger.log("Anycast " + aMessage + " failed.");

          if (logger.level <= INFO_2) {
            logger.log("Anycast failed at this intermediate node:" + aMessage+"\nAnycastMessage ANYCASTFAILEDHOPS "
                + aMessage.getVisitedSize() + " " + aMessage.getContent());
          }
          // We will send an anycast failure message
          // TODO: make this faster if using raw serialized message, use fast ctor
          AnycastFailureMessage aFailMsg = new AnycastFailureMessage(endpoint.getLocalNodeHandle(), aMessage.getTopic(), aMessage.getContent());
          endpoint.route(null, aFailMsg, aMessage.getInitialRequestor());
      } else {
        
        // What is going on here?
        if (logger.level <= Logger.FINEST) logger.log("forward() routing "+aMessage+" to "+handle);
        endpoint.route(null, aMessage, handle);
      }

      return false;
    }

    return true;
  }

  /**
   * This is complicated because the SubscribeMessage may have many topics to subscribe to at once.
   * Also, for every topic we don't accept, we must branch the SubscribeMessage based on the routing table.
   * 
   * 
   * @param sMessage
   * @return true if it needs to be forward, false if we handled it
   */
  protected boolean handleForwardSubscribeMessage(SubscribeMessage sMessage) {
    if (logger.level <= Logger.FINEST) logger.log("handleForwardScribeMessage("+sMessage+")");
    
    // if this is our own subscribe message, ignore it
    if (sMessage.getSource().getId().equals(endpoint.getId())) {
      if(logger.level <= Logger.INFO) logger.log(
          "Bypassing forward logic of subscribemessage "+sMessage+" becuase local node is the subscriber source.");
      return true;
    }
    
    // Note: deliver only gets called on the root
    // deliver() should only be called under the following circumstances
    // a) we are the root and the subscriber
    // b) nobody would take the node, so we return him a SubscribeFailedMessage
    // only return true if there are topics left in the SubscribeMessage that we aren't responsible for
    // c) we filled the request
    
    // first, we have to make sure that we don't create a loop, which would occur
    // if the subscribing node's previous parent is on our path to the root
    // also check to make sure that we have not already accepted the node
    // first, we'll distill the list into 3 categories
    // 1) topics that will cause a loop
    // 2) topics that are already subscribed
    // 3) topics that we are the root and we don't have any children yet, NOTE: this was added in FP 2.1 as a safety net so you can't write a policy that won't accept on the root
    // 4) topics that we need to ask the policy
    if (logger.level <= Logger.FINEST) logger.log("handleForwardSubscribeMessage() here 1 "+sMessage);
    
    // The first step is to break up the topics into 3 groups.  
    // forward: If we are in the subscriber's pathToRoot (to prevent loops)
    // dontForward: If we are already the parent
    // askPolicy: everyone else (after the policy is asked, these will be placed into forward/dontForward appropriately)
    
    // this is the list of topics that aren't a loop or already a child
    ArrayList<Topic> forward = new ArrayList<Topic>(); // leave these in the message
    ArrayList<Topic> dontForward = new ArrayList<Topic>(); // take these out, and maybe send a response message for these, the old policy just drops these, We should probably send a response as long as sMessage.getId() != NO_ACK_REQUIRED
//    ArrayList<Topic> isRoot = new ArrayList<Topic>(); // we are the root of a new topic, we must accept the child, and create the new topic
    ArrayList<Topic> askPolicy = new ArrayList<Topic>(); // ask the policy, and take these out if the policy accepts them
    
    for (Topic topic : sMessage.getTopics()) {
      TopicManager tmanager = topicManagers.get(topic);
      
//      if (isRoot(topic)) {
//        if (tmanager == null || tmanager.getChildren().isEmpty()) {
//          // we are the root of a new topic, or an empty topic
//          dontForward.add(topic);
////          isRoot.add(topic); 
////          continue;
//        }
//      }
        
      if (tmanager != null) {
        List<Id> path = tmanager.getPathToRoot();
  
        if (path.contains(sMessage.getSubscriber().getId())) {
          if (logger.level <= Logger.INFO) {
            String s = "Rejecting subscribe message from " +
                    sMessage.getSubscriber() + " for topic " + sMessage.getTopic() +
                    " because we are on the subscriber's path to the root:";
            for (Id id : path) {
              s+=id+",";
            }
            logger.log(s);
          }
          forward.add(topic);
          continue;
        }
  
        // check if child is already there
        if (tmanager.getChildren().contains(sMessage.getSubscriber())){
          dontForward.add(topic);
          continue;
        }
      }
      
      askPolicy.add(topic);
    }
    
//    for (Topic topic : isRoot) {
//      // don't need to subscribe, we are the root
//      addChildHelper(topic, sMessage.getSubscriber());
//    }

    List<Topic> accepted; // these are the messages that the policy accepted
    
//    logger.log("handleForwardScribeMessage("+sMessage+")"+
//        " forward:"+(forward.size() == 1 ? forward.iterator().next() : forward.size())+
//        " dontForward:"+(dontForward.size() == 1 ? dontForward.iterator().next() : dontForward.size())+
//        " askPolicy:"+(askPolicy.size() == 1 ? askPolicy.iterator().next() : askPolicy.size()));
    
    if (logger.level <= Logger.FINEST) logger.log("handleForwardSubscribeMessage() here 2 "+sMessage);
    if (!askPolicy.isEmpty()) {
      
      // see if the policy will allow us to take on this child
      accepted = policy.allowSubscribe(this, sMessage.getSubscriber(), new ArrayList<Topic>(askPolicy), sMessage.getContent()); 
      
      // askPolicy is now the rejected
      askPolicy.removeAll(accepted);        
    
//      logger.log("handleForwardScribeMessage("+sMessage+")"+
//          " accepted:"+(accepted.size() == 1 ? accepted.iterator().next() : accepted.size())+
//          " rejected:"+(askPolicy.size() == 1 ? askPolicy.iterator().next() : askPolicy.size()));
      
      dontForward.addAll(accepted);

      // we only acually add if the node is alive
      // Why is this the right policy?  Maybe we are incorrect about the liveness of the node, but 
      // we don't want anycasts to bounce all over the ring until they hit every node,
      // thus, if we were going to accept the node, then we drop that topic here, and forward the rest
      List<Topic> newTopics = new ArrayList<Topic>();
      if (sMessage.getSubscriber().isAlive()) {
        for (Topic topic : accepted) {
        
        // the ones that are returned: 
        //    a) call addChild()
        //    b) put into the SubscribeAckMessage
        // the rejected:
        //   leave in the sMessage
        
          if (logger.level <= Logger.FINER) logger.log("Hijacking subscribe message from " +
            sMessage.getSubscriber() + " for topic " + topic);
    
          // if so, add the child
          if (addChildHelper(topic, sMessage.getSubscriber())) {
            // the child implicitly created a topic, need to subscribe   
            newTopics.add(topic);
          }
        }
        
        // the topic is new to us, so we need to subscribe
        subscribe(newTopics, null, maintenancePolicy.implicitSubscribe(newTopics), null); 
      } else { // isAlive 
        if (logger.level <= Logger.WARNING) {
          logger.log("Dropping subscribe message for dead "+sMessage.getSubscriber()+" "+accepted);
        }
        accepted.clear(); 
      }
    } else {
      accepted = askPolicy; // also empty
    }
    
    // just a nicer name for it
    List<Topic> rejected = askPolicy;
    
    forward.addAll(rejected);
  
    List<Topic> toReturn; // which topics to include in the subscribeAck
    
    // this block chooses to set toReturn to only the new topics if this were due to maintenance,
    // because we don't need to ack the topics the node was already part of
    if (sMessage.getId() == MAINTENANCE_ID) {
      toReturn = accepted; // to update the rootToPath
    } else {
      toReturn = dontForward; // because the subscriber doesn't know he's already joined
    }
    
//    toReturn.addAll(isRoot);
    if (logger.level <= Logger.FINEST) logger.log("handleForwardSubscribeMessage() here 3 "+sMessage);

 
    // NOTE: We need the isAlive() check, otherwise the tmanager (below) may be null, if we
    // were to create teh tmanager just for this node.
    if (!toReturn.isEmpty() && sMessage.getSubscriber().isAlive()) {
      // we send a confirmation back to the child
      
      // build all of the pathToRoot[]
      List<List<Id>> paths = new ArrayList<List<Id>>(toReturn.size());
      for (Topic topic : toReturn) {
        TopicManager tmanager = topicManagers.get(topic);
        paths.add(tmanager.getPathToRoot());
      }
        
      endpoint.route(null, new SubscribeAckMessage(localHandle, toReturn, paths, sMessage.getId()), sMessage.getSubscriber());
    }
    
    // otherwise, we are effectively rejecting the child
    sMessage.removeTopics(dontForward);
    
    if (logger.level <= Logger.FINER) logger.log("Rejecting subscribe message from " +
        sMessage.getSubscriber() + " for topic(s) " + sMessage.getTopics());

    if (sMessage.isEmpty()) {
      // there are no more topics in the message, we handled them all
      if (logger.level <= Logger.FINEST) logger.log("handleForwardSubscribeMessage() returning false here 85");
      return false; // the buck stops here, cause all requests are filled
    }
    
    // add the local node to the visited list
    sMessage.addVisited(endpoint.getLocalNodeHandle());
    
    if (logger.level <= Logger.FINEST) logger.log("handleForwardSubscribeMessage() here 4 "+sMessage);

    // there are Topics left that we didn't accept:
    // a) for each that we have a manager, call directAnycast, this is the old way to have good management of trees
    // b) for topics that we don't have a manager, split them up based on the underlieing overlay's router and route them that way, this is the new version for multi-subscription
    
    List<Topic> noManager = new ArrayList<Topic>(); // these are the topics we don't have a manager for
    List<Topic> failed = new ArrayList<Topic>(); // these are the topics that have exhausted the tree (they visited everyone and were rejected)
    
    Iterator<Topic> topicIterator = sMessage.getTopics().iterator();
    while (topicIterator.hasNext()) {
      Topic topic = topicIterator.next();
      
      // the topic can change on the SubscribeMessage when we accept some of the items
      TopicManager manager = (TopicManager) topicManagers.get(topic);
    
      // we have nothing to do with this message
      if (manager == null) {
        noManager.add(topic);
      } else {
        topicIterator.remove(); // in case we just forward the message
        AnycastMessage aMessage = sMessage.copy(Collections.singletonList(topic), sMessage.getRawContent());// clone but with just the 1 topic, make sure to copy toVisit/visited/content
            
        // allow the policy to select the order in which the nodes are visited
        policy.directAnycast(aMessage, manager.getParent(), manager.getChildren());

        // reset the source of the message to be us
        aMessage.setSource(endpoint.getLocalNodeHandle());
        
        // get the next hop
        NodeHandle handle = aMessage.getNext();

        // make sure that the next node is alive
        while ((handle != null) && (!handle.isAlive())) {
          handle = aMessage.getNext();
        }

        if (handle == null || !handle.isAlive()) handle = null;
        
        if (logger.level <= Logger.FINER) logger.log("Forwarding anycast message for topic " + aMessage.getTopic() + "on to " + handle);

        if (handle == null) {
          if (logger.level <= Logger.FINE) logger.log("Anycast " + aMessage + " failed.");

          // if it's a subscribe message, send a subscribe failed message back
          // as a courtesy
          if (logger.level <= Logger.FINER) logger.log("Sending SubscribeFailedMessage to " + sMessage.getSubscriber() +" for topic "+topic);

          failed.add(topic);
            
            // XXX - For the centralized policy we had done this earlier
            // XXX - We let the subscribe proceed to the root, the root will send
            // the SubscribeFailedMsg if it reaches there
            // XXX - return true;
        } else {
          if (logger.level <= Logger.FINEST) logger.log("handleForwardSubscribeMessage() routing "+aMessage+" to "+handle);
          endpoint.route(aMessage.getTopic().getId(), aMessage, handle);
        }
      }
    }
    if (logger.level <= Logger.FINEST) logger.log("handleForwardSubscribeMessage() here 5 "+sMessage);
    
    // handle the multi-subscription
    HashMap<NodeHandle, List<Topic>> manifest = buildManifests(noManager);
    if (manifest.containsKey(localHandle)) {
      // we are the root, but the policy would let us accept him
      List<Topic> theTopics = manifest.remove(localHandle);
      sMessage.removeTopics(theTopics);
      for (Topic topic : theTopics) {
        failed.add(topic); 
//        if (logger.level <= Logger.WARNING) logger.log("No next hop for topic "+topic+" isRoot = "+isRoot(topic));
      }
    }
    if (logger.level <= Logger.FINEST) logger.log("handleForwardSubscribeMessage() here 6 "+sMessage);

    endpoint.route(null, 
        new SubscribeFailedMessage(localHandle, failed, sMessage.getId()),
        sMessage.getSubscriber());
    
    if (manifest.keySet().size() == 1) {
      if (logger.level <= Logger.FINEST) logger.log("handleForwardSubscribeMessage() returning true at this location!!! "+sMessage);
      return true; // forward the message 
    }
    for (NodeHandle nextHop : manifest.keySet()) {
      List<Topic> theTopics = manifest.get(nextHop);
      if (theTopics != null) { // this should probably never happen
        AnycastMessage aMessage = sMessage.copy(theTopics, convert(policy.divideContent(theTopics, sMessage.getContent()))); // use the copy constructor again
        endpoint.route(aMessage.getTopic().getId(), aMessage, nextHop);
      }
    }
    
    // TODO: handle this
    
    
    
    return false;
  }        
  
  /**
   * This method is called on the application at the destination node for the given id.
   *
   * @param id The destination id of the message
   * @param message The message being sent
   */
  public void deliver(Id id, Message message) {
    if (logger.level <= Logger.FINEST) logger.log("Deliver called with " + id + " " + message);
    
    if (message instanceof AnycastMessage) {
      AnycastMessage aMessage = (AnycastMessage) message;

      // if we are the recipient to someone else's subscribe, then we should have processed
      // this message in the forward() method.
      // Otherwise, we received our own subscribe message, which means that we are
      // the root
      if (aMessage.getSource().equals(localHandle)) {
        if (aMessage instanceof SubscribeMessage) {
          SubscribeMessage sMessage = (SubscribeMessage) message;

          if (sMessage.isEmpty()) return; // we already handled the subscribe message
          
          SubscribeLostMessage slm = subscribeLostMessages.get(sMessage.getId());
          if (slm != null) {
            ScribeMultiClient multiClient = slm.getClient();
            if (multiClient != null) multiClient.subscribeSuccess(sMessage.getTopics());
            // I'm not sure what to do here, because we should be the root, but if we aren't?  I suspect it is a bug.
            if (slm.topicsAcked(sMessage.getTopics())) {      
              if (logger.level <= Logger.FINER) {
                  logger.log("Removing client " + slm.getClient() + " from list of outstanding for ack " + sMessage.getId());
              }
              subscribeLostMessages.remove(sMessage.getId()).cancel();
            }
          }
          for (Topic topic : sMessage.getTopics()) {
            if (!isRoot(topic)) {
              if (logger.level <= Logger.WARNING) 
                logger.log("Received our own subscribe message " + aMessage + " for topic " + aMessage.getTopic() + " - we are not the root.");               
            }
          }
          if (logger.level <= Logger.FINE) 
            logger.log("Received our own subscribe message " + aMessage + " for topic " + aMessage.getTopic() + " - we are the root.");
        } else {
          if(logger.level <= Logger.WARNING) logger.log("WARNING : Anycast failed at Root for Topic " + aMessage.getTopic() + " was generated by us " + " msg= " + aMessage);
          if(logger.level <= INFO_2) logger.log(endpoint.getId() + ": AnycastMessage ANYCASTFAILEDHOPS " + aMessage.getVisitedSize() + " " + aMessage.getContent());   
//          if (logger.level <= Logger.WARNING) logger.log("Received unexpected delivered anycast message " + aMessage + " for topic " +
//            aMessage.getTopic() + " - was generated by us.");
          
          // We send a anycast failure message
          // TODO: make this faster if using raw serialized message, use fast ctor
          AnycastFailureMessage aFailMsg = new AnycastFailureMessage(endpoint.getLocalNodeHandle(), aMessage.getTopic(), aMessage.getContent());
          endpoint.route(null, aFailMsg, aMessage.getInitialRequestor()); 
        }
      } else {
        // here, we have had a subscribe message delivered, which means that we are the root, but
        // our policy says that we cannot take on this child
        if (aMessage instanceof SubscribeMessage) {
          SubscribeMessage sMessage = (SubscribeMessage) aMessage;
          if (logger.level <= Logger.FINE) logger.log("Sending SubscribeFailedMessage (at root) to " + sMessage.getSubscriber());

          endpoint.route(null,
                         new SubscribeFailedMessage(localHandle, sMessage.getTopics(), sMessage.getId()),
                         sMessage.getSubscriber());
        } else {
          if(logger.level <= Logger.WARNING) logger.log("WARNING : Anycast failed at Root for Topic " + aMessage.getTopic() + " not generated by us " +" msg= " + aMessage);
          if(logger.level <= INFO_2) logger.log(endpoint.getId() + ": AnycastMessage ANYCASTFAILEDHOPS " + aMessage.getVisitedSize() + " " + aMessage.getContent());  
//          if (logger.level <= Logger.WARNING) logger.log("Received unexpected delivered anycast message " + aMessage + " for topic " +
//                      aMessage.getTopic() + " - not generated by us, but was expected to be.");
          
          // We send an anycast failure message
          AnycastFailureMessage aFailMsg = new AnycastFailureMessage(endpoint.getLocalNodeHandle(), aMessage.getTopic(), aMessage.getContent());
          endpoint.route(null, aFailMsg, aMessage.getInitialRequestor()); 
        }
      }
    } else if (message instanceof SubscribeAckMessage) { 
      // store the former parents/topics here so we can send Unsubscribe messages
      HashMap<NodeHandle, List<Topic>> needToUnsubscribe = new HashMap<NodeHandle, List<Topic>>();
      SubscribeAckMessage saMessage = (SubscribeAckMessage) message;

      if (! saMessage.getSource().isAlive()) {
        if (logger.level <= Logger.WARNING) logger.log("Dropping subscribe ack message from dead node:" + saMessage.getSource() + " for topics " + saMessage.getTopics());
        return;
      }
      
      Iterator<List<Id>> i = saMessage.getPathsToRoot().iterator();
      for (Topic topic : saMessage.getTopics()) {
        List<Id> pathToRoot = i.next();
        TopicManager manager = (TopicManager) topicManagers.get(topic);
  
        if (logger.level <= Logger.FINE) logger.log("Received subscribe ack message from " + saMessage.getSource() + " for topic " + topic);
  
        ackMessageReceived(saMessage);
  
        // if we're the root, reject the ack message, except for the hack to implement a centralized solution with self overlays for each node (i.e everyone is a root)
        if (isRoot(topic)) {
          if (logger.level <= Logger.FINE) logger.log("Received unexpected subscribe ack message (we are the root) from " +
                   saMessage.getSource() + " for topic " + topic);
          List<Topic> topics = needToUnsubscribe.get(saMessage.getSource());
          if (topics == null) topics = new ArrayList<Topic>();
          topics.add(topic);
        } else {
          // if we don't know about this topic, then we unsubscribe
          // if we already have a parent, then this is either an errorous
          // subscribe ack, or our path to the root has changed.
          if (manager == null) {
            if (logger.level <= Logger.WARNING) logger.log("Received unexpected subscribe ack message from " +
                saMessage.getSource() + " for unknown topic " + topic);
            List<Topic> topics = needToUnsubscribe.get(saMessage.getSource());
            if (topics == null) topics = new ArrayList<Topic>();
            topics.add(topic);
          } else {
            if (manager.getParent() == null) {
              setParent(topic, saMessage.getSource(), pathToRoot);
            }
  
            if (!manager.getParent().equals(saMessage.getSource())) {
              if (logger.level <= Logger.WARNING) logger.log("Received somewhat unexpected subscribe ack message (already have parent " + manager.getParent() +
                          ") from " + saMessage.getSource() + " for topic " + topic + " - the new policy is now to accept the message");
    
              
              NodeHandle parent = manager.getParent();
              setParent(topic, saMessage.getSource(), pathToRoot);
  
              List<Topic> topics = needToUnsubscribe.get(parent);
              if (topics == null) {
                topics = new ArrayList<Topic>();
                needToUnsubscribe.put(parent, topics);
              }
              topics.add(topic);
            }
//
//          if (manager != null) {            
//            if (manager.getParent() != null && !manager.getParent().equals(saMessage.getSource())) {
//              // parent changed for some reason
//              if (logger.level <= Logger.WARNING) logger.log("Received somewhat unexpected subscribe ack message (already have parent " + manager.getParent() +
//                  ") from " + saMessage.getSource() + " for topic " + topic + " - the new policy is now to accept the message");
//      
//              NodeHandle parent = manager.getParent();
//              endpoint.route(null, new UnsubscribeMessage(localHandle, topic), parent);
//            }
//            setParent(topic, saMessage.getSource(), pathToRoot);
//           
          }
        }
        for(NodeHandle source : needToUnsubscribe.keySet()) {
          endpoint.route(null, new UnsubscribeMessage(localHandle, needToUnsubscribe.get(source)), source);
        }
      }
    } else if (message instanceof SubscribeLostMessage) {
      SubscribeLostMessage slMessage = (SubscribeLostMessage) message;
      
      lostMessageReceived(slMessage);
    } else if (message instanceof SubscribeFailedMessage) {
      SubscribeFailedMessage sfMessage = (SubscribeFailedMessage) message;

      failedMessageReceived(sfMessage);
    } else if (message instanceof PublishRequestMessage) {
      PublishRequestMessage prMessage = (PublishRequestMessage) message;
      TopicManager manager = (TopicManager) topicManagers.get(prMessage.getTopic());

      if (logger.level <= Logger.FINER) logger.log("Received publish request message with data " +
        prMessage.getContent() + " for topic " + prMessage.getTopic());

      // if message is for a non-existant topic, drop it on the floor (we are the root, after all)
      // otherwise, turn it into a publish message, and forward it on
      if (manager == null) {
        if (logger.level <= Logger.FINE) logger.log("Received publish request message for non-existent topic " +
          prMessage.getTopic() + " - dropping on floor.");
      } else {
        deliver(prMessage.getTopic().getId(), new PublishMessage(prMessage.getSource(), prMessage.getTopic(), prMessage.getContent()));
      }
    } else if (message instanceof PublishMessage) {
      PublishMessage pMessage = (PublishMessage) message;
      TopicManager manager = (TopicManager) topicManagers.get(pMessage.getTopic());

      if (logger.level <= Logger.FINER) logger.log("Received publish message with data " + pMessage.getContent() + " for topic " + pMessage.getTopic());

      // if we don't know about this topic, or this message did
      // not come from our parent, send an unsubscribe message
      // otherwise, we deliver the message to all clients and forward the
      // message to all children
      if ((manager != null) && ((manager.getParent() == null) || (manager.getParent().equals(pMessage.getSource())))) {
        pMessage.setSource(localHandle);

        Collection<ScribeMultiClient> clients = manager.getClients();
        
        // We also do an upcall intermediateNode() to allow implicit subscribers to change state in the message
        policy.intermediateNode(pMessage);

        for (ScribeMultiClient client : clients) {
          if (logger.level <= Logger.FINER) logger.log("Delivering publish message with data " + pMessage.getContent() + " for topic " +
            pMessage.getTopic() + " to client " + client);
          client.deliver(pMessage.getTopic(), pMessage.getContent());
        }

        Collection<NodeHandle> handles = new ArrayList<NodeHandle>(manager.getChildren());

        for (NodeHandle handle : handles) {
          if (logger.level <= Logger.FINER) logger.log("Forwarding publish message with data " + pMessage.getContent() + " for topic " +
            pMessage.getTopic() + " to child " + handle);
          endpoint.route(null, new PublishMessage(endpoint.getLocalNodeHandle(), pMessage.getTopic(), pMessage.getContent()), handle);
        }
      } else {
        if (logger.level <= Logger.WARNING) logger.log("Received unexpected publish message from " +
          pMessage.getSource() + " for unknown topic " + pMessage.getTopic());

        endpoint.route(null, new UnsubscribeMessage(localHandle, Collections.singletonList(pMessage.getTopic())), pMessage.getSource());
      }
    } else if (message instanceof UnsubscribeMessage) {
      UnsubscribeMessage uMessage = (UnsubscribeMessage) message;
      List<Topic> topics = uMessage.getTopics();
      NodeHandle source = uMessage.getSource();
      
      if (logger.level <= Logger.FINE) {
        String s;
        if (topics.size() == 1) {
          s = " for topic " + topics.get(0).toString(); 
        } else {
          s = " for "+topics.size()+" topics."; 
        }
        logger.log("Received unsubscribe message from " +
            source + s);
      }
      
      for (Topic topic : topics) {
        removeChild(topic, source, false);
      }
    } else if (message instanceof DropMessage) {
      DropMessage dMessage = (DropMessage) message;
      if (logger.level <= Logger.FINE) logger.log("Received drop message from " + dMessage.getSource() + " for topic " + dMessage.getTopic());
      
      TopicManager manager = (TopicManager) topicManagers.get(dMessage.getTopic());

      if (manager != null) {
        if ((manager.getParent() != null) && manager.getParent().equals(dMessage.getSource())) {
          // we set the parent to be null, and then send out another subscribe message
          setParent(dMessage.getTopic(), null, null);
          Collection<ScribeMultiClient> clients = manager.getClients();

          sendSubscribe(dMessage.getTopic(), null, 
              maintenancePolicy.implicitSubscribe(Collections.singletonList(dMessage.getTopic())), 
              null);
        } else {
          if (logger.level <= Logger.WARNING) logger.log("Received unexpected drop message from non-parent " +
                      dMessage.getSource() + " for topic " + dMessage.getTopic() + " - ignoring");
        }
      } else {
        if (logger.level <= Logger.WARNING) logger.log("Received unexpected drop message from " +
                    dMessage.getSource() + " for unknown topic " + dMessage.getTopic() + " - ignoring");
      }
    } else if (message instanceof MaintenanceMessage) {
      if (logger.level <= Logger.FINE) logger.log("Received maintenance message");
      maintenancePolicy.doMaintenance(this);
    } else if (message instanceof AnycastFailureMessage) {
      AnycastFailureMessage aFailMsg = (AnycastFailureMessage) message;
      if (logger.level <= Logger.FINE)
        logger.log("Received anycast failure message from " + aFailMsg.getSource()+ " for topic " + aFailMsg.getTopic());
      recvAnycastFail(aFailMsg.getTopic(), aFailMsg.getSource(), aFailMsg.getContent());
    } else {
      if (logger.level <= Logger.WARNING) logger.log("Received unknown message " + message + " - dropping on floor.");
    }
  }
  
  protected RawScribeContent convert(ScribeContent content) {
    if (content == null) return null;
    if (content instanceof RawScribeContent) return (RawScribeContent)content;
    return new JavaSerializedScribeContent(content);
  }
  
  /**
   * Called when a Node's liveness changes
   *
   */
  public void update(Observable o, Object arg) {
    if (arg.equals(NodeHandle.DECLARED_DEAD)) {
      NodeHandle handle = (NodeHandle)o;      
      
      ArrayList<Topic> wasChildOfTopics = new ArrayList<Topic>(getTopicsByChild(handle));
      for (Topic topic : wasChildOfTopics) {
        removeChild(topic,handle); // TODO: see what messages this dispatches
        if (logger.level <= Logger.FINE) logger.log("Child " + o + " for topic " + topic + " has died - removing.");
      }
      ArrayList<Topic> wasParentOfTopics = new ArrayList<Topic>(getTopicsByParent(handle));
      for (Topic topic : wasParentOfTopics) {
        if (logger.level <= Logger.FINE) logger.log("Parent " + handle + " for topic " + topic + " has died - removing.");
        setParent(topic, null, null);
      }
      
      maintenancePolicy.nodeFaulty(this, handle, 
          wasParentOfTopics, 
          wasChildOfTopics);
              
//      o.deleteObserver(this); // this should be done automatically when the maintenance policy gets rid of the parents/children
    }
  }

  /**
   * This method is invoked to inform the application that the given node has either joined or left
   * the neighbor set of the local node, as the set would be returned by the neighborSet call.
   *
   * @param handle The handle that has joined/left
   * @param joined Whether the node has joined or left
   */
  public void update(NodeHandle handle, boolean joined) {
    if(logger.level <= Logger.INFO) logger.log("update(" + handle + ", " + joined + ")");
        
    if (joined) {
      // NOTE: we could clean this up to use roots/pending, but this impl seems pretty safe
      List<Topic> notRoot = new ArrayList<Topic>();
      for(TopicManager manager : (new ArrayList<TopicManager>(topicManagers.values()))) {
//      TopicManager manager = (TopicManager) topics.get(topic);
        Topic topic = manager.topic;      
        // check if new guy is root, we were old root, then subscribe
        if (!isRoot(topic) && manager.getParent() == null){ // maybe we are already subscribing?
          // send subscribe message
//          sendSubscribe(topic, null, null, null);
          notRoot.add(topic);
        }
//      } else {
//        // if you should be the root, but you are only a member
//        // unsubscribe from previous root to prevent loops (this guy is probably faulty anyway)
//        // *** this is already handled in the TopicManager.update() method for when the node is faulty
//        if (isRoot(topic) && (manager.getParent() != null)) {
//          endpoint.route(null, new UnsubscribeMessage(endpoint.getLocalNodeHandle(), topic), manager.getParent());
//          manager.setParent(null);
//        }
      }
      if (!notRoot.isEmpty()) {
        maintenancePolicy.noLongerRoot(this, notRoot);
      }
    }    
  }

  /**
   * Class which keeps track of a given topic
   *
   * @version $Id: ScribeImpl.java 4495 2008-09-26 14:48:02Z jeffh $
   * @author amislove
   */
  public class TopicManager {

    /**
     * DESCRIBE THE FIELD
     */
    protected Topic topic;

    /**
     * The current path to the root for this node
     */
    protected List<Id> pathToRoot;

    /**
     * DESCRIBE THE FIELD
     */
    protected ArrayList<ScribeMultiClient> clients;

    /**
     * DESCRIBE THE FIELD
     */
    protected ArrayList<NodeHandle> children;

    /**
     * DESCRIBE THE FIELD
     */
    protected NodeHandle parent;

    /**
     * Constructor for TopicManager.
     *
     * @param topic DESCRIBE THE PARAMETER
     * @param client DESCRIBE THE PARAMETER
     */
//    public TopicManager(Topic topic, ScribeClient client) {
//      this(topic);
//
//      addClient(client);
//      if(logger.level <= Logger.INFO) logger.log("Creating TopicManager for topic: " + topic + ", client: " + client);
//    }
//
//    /**
//     * Constructor for TopicManager.
//     *
//     * @param topic DESCRIBE THE PARAMETER
//     * @param child DESCRIBE THE PARAMETER
//     */
//    public TopicManager(Topic topic, NodeHandle child) {
//      this(topic);
//
//      addChild(child);
//      if(logger.level <= Logger.INFO) logger.log("Creating TopicManager for topic: " + topic + ", child: " + child);
//    }

    /**
     * Constructor for TopicManager.
     *
     * @param topic DESCRIBE THE PARAMETER
     */
    private TopicManager(Topic topic) {
      this.topic = topic;
      this.clients = new ArrayList<ScribeMultiClient>();
      this.children = new ArrayList<NodeHandle>();

      // this is a bit ugly: normally we can't be holding the topicManagers lock when calling setPathToRoot()
      // because it sends messages, but it is ok in this case because we have no children
      setPathToRoot(null);
      
//      if(logger.level <= Logger.INFO) logger.log("Creating TopicManager for topic: " + topic);
    }
    
    /**
     * Gets the topic of the TopicManager object
     *
     * @return The Parent value
     */
    public Topic getTopic() {
      return topic;
    }

    /**
     * Gets the Parent attribute of the TopicManager object
     *
     * @return The Parent value
     */
    public NodeHandle getParent() {
      return parent;
    }

    /**
     * Gets the Clients attribute of the TopicManager object
     *
     * @return The Clients value
     */
    public Collection<ScribeMultiClient> getClients() {
      return Collections.unmodifiableCollection(clients);
    }
    
//    public ScribeClient[] getClientsAsArray() {
//      return (ScribeClient[]) clients.toArray(new ScribeClient[0]);
//    }
    
    /**
     * Returns whether or not this topic manager contains the given
     * client.
     *
     * @param client The client in question
     * @return Whether or not this manager contains the client
     */
    public boolean containsClient(ScribeMultiClient client) {
      return clients.contains(client);
    }

    /**
     * Gets the Children attribute of the TopicManager object
     *
     * @return The Children value
     */
    public Collection<NodeHandle> getChildren() {
      return Collections.unmodifiableCollection(children);
    }
    
    public int numChildren() {
      return children.size();
    }
    
    /**
     * Gets the PathToRoot attribute of the TopicManager object
     *
     * @return The PathToRoot value
     */
    public List<Id> getPathToRoot() {
      return pathToRoot;
    }

    /**
     * Sets the PathToRoot attribute of the TopicManager object
     *
     * Don't hold the topicManagers lock, this method sends messages
     *
     * @param pathToRoot The new PathToRoot value
     */
    public void setPathToRoot(List<Id> pathToRoot) {
      // make sure this returns if it didn't change
      
      // build the path to the root for the new node
      if (pathToRoot == null) {
        this.pathToRoot = new ArrayList<Id>();
      } else {
        this.pathToRoot = new ArrayList<Id>(pathToRoot);
      }
      this.pathToRoot.add(endpoint.getId());

      if (!children.isEmpty()) {
        List<NodeHandle> sendDrop = new ArrayList<NodeHandle>();
        List<NodeHandle> sendUpdate = new ArrayList<NodeHandle>();
        
        // now send the information out to our children, prevent routing loops
        synchronized(topicManagers) {
          Collection<NodeHandle> children = getChildren();
          for (NodeHandle child : children) {
            if (this.pathToRoot.contains(child.getId())) {
              sendDrop.add(child);
              
              // Can't call removeChild() here because you will get a ConcurrentModificationException
//              removeChild(child);
            } else {
              sendUpdate.add(child);
            }
          }
          
          for (NodeHandle child : sendDrop) {
            removeChild(child);
          }
        }

        for (NodeHandle child : sendDrop) {
          endpoint.route(null, new DropMessage(localHandle, topic), child);        
        }
        for (NodeHandle child : sendUpdate) {
          // todo: make this an update message
          endpoint.route(null, new SubscribeAckMessage(localHandle, Collections.singletonList(topic), Collections.singletonList(getPathToRoot()), MAINTENANCE_ID), child);        
        }      
      }
    }

    /**
     * Sets the Parent attribute of the TopicManager object
     *
     * @param handle The new Parent value
     */
    public void setParent(NodeHandle handle, List<Id> pathToRoot) {
      if (logger.level <= Logger.INFO) logger.log(this+"setParent("+handle+","+pathToRoot+") prev:"+parent);        
      
      if ((handle != null) && !handle.isAlive()) {
        if (logger.level <= Logger.WARNING) logger.log("Setting dead parent "+handle+" for " + topic);
      }
      
      if ((handle != null) && (parent != null)) {
        if (handle.equals(parent)) {
          // not a real change
          setPathToRoot(pathToRoot);
          return;
        }
        if (logger.level <= Logger.FINE) logger.log("Unexpectedly changing parents for topic " + topic+":"+parent+"=>"+handle);
      }
      
      NodeHandle prevParent = parent;
      
//      if (parent != null) {
//        parent.deleteObserver(this);
//      }
      
      parent = handle;
      setPathToRoot(pathToRoot);
      
//      if ((parent != null) && parent.isAlive()) {
//        parent.addObserver(this);
//      }
      
      synchronized(topicManagers) {
        // We remove the stale parent from global data structure of parent ->
        // topics
        removeFromAllParents(topic, prevParent);
  
        // We add the new parent from the global data structure of parent ->
        // topics
        addToAllParents(topic, parent);
      }
    }

    public String toString() {
      return topic.toString();
    }
    
    /**
     * Called when a Node's liveness changes
     *
     */
    /*
    public void update(Observable o, Object arg) {
      if (arg.equals(NodeHandle.DECLARED_DEAD)) {
//        logger.log("update("+o+","+arg+")");
        // print a warning if we get an update we don't expect
        boolean expected = false;
        if (children.contains(o)) {
          if (logger.level <= Logger.FINE) logger.log("Child " + o + " for topic " + topic + " has died - removing.");

          ScribeImpl.this.removeChild(topic, (NodeHandle) o);
          expected = true;
        } 
        
        if (o.equals(parent)) {
          expected = true;
          // if our parent has died, then we must resubscribe to the topic
          if (logger.level <= Logger.FINE) logger.log("Parent " + parent + " for topic " + topic + " has died - resubscribing.");
          
          setParent(null);

          sendSubscribe(topic, null, null, null);
        }
        
        if (!expected) {
          if (logger.level <= Logger.WARNING) logger.log(this+" Received unexpected update from " + o);
        }
        
        o.deleteObserver(this);
      }
    }
      */

    /**
     * Adds a feature to the Client attribute of the TopicManager object
     *
     * @param client The feature to be added to the Client attribute
     */
    public void addClient(ScribeMultiClient client) {
//      logger.log(this+"addClient("+client+")");
      if (client == null) return;
      
      if (!clients.contains(client)) {
        clients.add(client);
      }
    }

    /**
     * @param client the client to remove
     * @return true if there are no children/clients remaining (you can unsubscribe from the parent)
     */
    public boolean removeClient(ScribeMultiClient client) {
      clients.remove(client);

      boolean unsub = ((clients.size() == 0) && (children.size() == 0));

      // if we're going to unsubscribe, then we remove ourself as
      // as observer to keep from having memory problems
      // TODO: make this part of the destructable pattern
      // and get rid of all observers and remove from topics list too
//      if (unsub && (parent != null)) {
//        parent.deleteObserver(this);
//      }

      return unsub;
    }

    public boolean containsChild(NodeHandle child) {
      if (children.contains(child)) {
        return true;
      } else {
        return false;
      }
    }
    
    /**
     * Adds a feature to the Child attribute of the TopicManager object
     *
     * Need to be holding lock: topicManagers
     *
     * @param child The feature to be added to the Child attribute
     */
    public void addChild(NodeHandle child) {
      if (logger.level <= Logger.INFO)
        logger.log("addChild( " + topic + ", " + child + ")");

      if (!children.contains(child)) {
        if (child.isAlive()) {
          children.add(child);
//          child.addObserver(this);
          // We update this information to the global data structure of children
          // -> topics
          addToAllChildren(topic, child);
        } else {
          if (logger.level <= Logger.WARNING)
            logger.logException("WARNING: addChild("+topic+", "+child+") did not add child since the child.isAlive() failed",new Exception("Stack Trace"));
        }
      }
    }

    /**
     * Removes the child from the topic.
     *
     * Need to be holding lock: topicManagers
     * 
     * @param child the child to be removed
     * @return true if we can unsubscribe (IE, no clients nor children)
     */
    public boolean removeChild(NodeHandle child) {
      if (logger.level <= Logger.INFO)
        logger.log("removeChild( " + topic + ", " + child + ")");
      
      children.remove(child);
      
//      child.deleteObserver(this);

      boolean unsub = ((clients.size() == 0) && (children.size() == 0));

      // if we're going to unsubscribe, then we remove ourself as
      // as observer to keep from having memory problems
      // TODO: make this part of the destructable pattern
      // and get rid of all observers and remove from topics list too
//      if (unsub && (parent != null)) {
//        parent.deleteObserver(this);
//      }

      // We update this information to the global data structure of children ->
      // topics
      removeFromAllChildren(topic,child);

      return unsub;
    }
  }

  @Override
  public String toString() {
    return "ScribeImpl["+localHandle+"]";
  }

  public void destroy() {
    if (environment.getSelectorManager().isSelectorThread()) {
      if (logger.level <= Logger.INFO) logger.log("Destroying "+this);
      ArrayList<TopicManager> managers = new ArrayList<TopicManager>(topicManagers.values());
      topicManagers.clear();
      
      for (NodeHandle handle : allChildren.keySet()) {
        handle.deleteObserver(this); 
      }
      for (NodeHandle handle : allParents.keySet()) {
        handle.deleteObserver(this); 
      }
    } else {
      environment.getSelectorManager().invoke(new Runnable() {      
        public void run() {
          destroy();
        }     
      });
    }
  }

  public Endpoint getEndpoint() {
    return endpoint; 
  }
  
  public void setContentDeserializer(ScribeContentDeserializer deserializer) {
    contentDeserializer = deserializer;
  }
  
  public ScribeContentDeserializer getContentDeserializer() {
    return contentDeserializer;
  }

  public Collection<Topic> getTopics() {
    // it would be safer to return a copy, but faster to do it this way
    return topicManagers.keySet();
  }

  public List<Id> getPathToRoot(Topic topic) {
    TopicManager manager = (TopicManager) topicManagers.get(topic);
    if (manager == null) return null;
    return manager.getPathToRoot();
  }
}
