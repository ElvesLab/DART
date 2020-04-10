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
package rice.p2p.aggregation;

import java.io.IOException;
import java.io.Serializable;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.Vector;
import java.util.Arrays;

import rice.Continuation;
import rice.Executable;
import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.environment.params.Parameters;
import rice.p2p.aggregation.messaging.*;
import rice.p2p.aggregation.raw.*;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.glacier.VersionKey;
import rice.p2p.glacier.VersioningPast;
import rice.p2p.glacier.v2.DebugContent;
import rice.p2p.past.Past;
import rice.p2p.past.PastImpl;
import rice.p2p.past.PastContent;
import rice.p2p.past.PastContentHandle;
import rice.p2p.past.gc.GCPast;
import rice.p2p.past.gc.GCPastContent;
import rice.p2p.past.gc.GCPastContentHandle;
import rice.p2p.past.gc.rawserialization.RawGCPastContent;
import rice.p2p.past.rawserialization.*;
import rice.p2p.util.DebugCommandHandler;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.persistence.StorageManager;
import rice.p2p.glacier.v2.GlacierContentHandle;

@SuppressWarnings("unchecked")
public class AggregationImpl implements GCPast, VersioningPast, Aggregation, Application, DebugCommandHandler {
  protected final Past aggregateStore;
  protected final StorageManager waitingList;
  protected final AggregationPolicy policy;
  protected final AggregateList aggregateList;
  protected final Endpoint endpoint;
  protected final Past objectStore;
  protected final String instance;
  protected final IdFactory factory;
  protected final Node node;

  private final char tiFlush = 1;
  private final char tiMonitor = 2;
  private final char tiConsolidate = 3;
  private final char tiStatistics = 4;
  private final char tiExpire = 5;
  protected Hashtable timers;
  protected Continuation flushWait;
  protected boolean rebuildInProgress;
  protected Vector monitorIDs;
  protected AggregationStatistics stats;

  private static final long SECONDS = 1000;
  private static final long MINUTES = 60 * SECONDS;
  private static final long HOURS = 60 * MINUTES;
  private static final long DAYS = 24 * HOURS;
  private static final long WEEKS = 7 * DAYS;

  private final boolean logStatistics;

  private final long flushDelayAfterJoin; 
  private final long flushStressInterval;
  private long flushInterval;

  private int maxAggregateSize;
  private int maxObjectsInAggregate;
  private int maxAggregatesPerRun;
  
  private final boolean addMissingAfterRefresh;
  private final int maxReaggregationPerRefresh;
  private final int nominalReferenceCount;
  private final int maxPointersPerAggregate;
  private final long pointerArrayLifetime;
  private final long aggregateGracePeriod;

  private final long aggrRefreshInterval;
  private final long aggrRefreshDelayAfterJoin;
  private long expirationRenewThreshold;

  private final boolean monitorEnabled;
  private final long monitorRefreshInterval;

  private final long consolidationDelayAfterJoin;
  private long consolidationInterval;
  private long consolidationThreshold;
  private int consolidationMinObjectsInAggregate;
  private double consolidationMinComponentsAlive;

  private int reconstructionMaxConcurrentLookups;

  private final boolean aggregateLogEnabled;

  private final long statsGranularity;
  private final long statsRange;
  private final long statsInterval;

  private final double jitterRange;

  private Environment environment;
  protected Logger logger;
  
  protected PastContentDeserializer contentDeserializer;
  protected PastContentHandleDeserializer contentHandleDeserializer;
  
  protected AggregateFactory aggregateFactory;
  
  public AggregationImpl(Node node, Past aggregateStore, Past objectStore, StorageManager waitingList, String configFileName, IdFactory factory, String instance) throws IOException {
    this(node, aggregateStore, objectStore, waitingList, configFileName, factory, instance, null, null);
  }

  public AggregationImpl(Node node, Past aggregateStore, Past objectStore, StorageManager waitingList, String configFileName, IdFactory factory, String instance, AggregationPolicy policy, AggregateFactory aggregateFactory) throws IOException {
    this.environment = node.getEnvironment();
    logger = environment.getLogManager().getLogger(AggregationImpl.class, instance);
    
    Parameters p = environment.getParameters();
    
    logStatistics = p.getBoolean("p2p_aggregation_logStatistics");

    flushDelayAfterJoin = p.getLong("p2p_aggregation_flushDelayAfterJoin");
    flushStressInterval = p.getLong("p2p_aggregation_flushStressInterval");
    flushInterval = p.getLong("p2p_aggregation_flushInterval");

    maxAggregateSize = p.getInt("p2p_aggregation_maxAggregateSize");
    maxObjectsInAggregate = p.getInt("p2p_aggregation_maxObjectsInAggregate");
    maxAggregatesPerRun = p.getInt("p2p_aggregation_maxAggregatesPerRun");
    
    addMissingAfterRefresh = p.getBoolean("p2p_aggregation_addMissingAfterRefresh");
    maxReaggregationPerRefresh = p.getInt("p2p_aggregation_maxReaggregationPerRefresh");
    nominalReferenceCount = p.getInt("p2p_aggregation_nominalReferenceCount");
    maxPointersPerAggregate = p.getInt("p2p_aggregation_maxPointersPerAggregate");
    pointerArrayLifetime = p.getLong("p2p_aggregation_pointerArrayLifetime");
    aggregateGracePeriod = p.getLong("p2p_aggregation_aggregateGracePeriod");

    aggrRefreshInterval = p.getLong("p2p_aggregation_aggrRefreshInterval");
    aggrRefreshDelayAfterJoin = p.getLong("p2p_aggregation_aggrRefreshDelayAfterJoin");
    expirationRenewThreshold = p.getLong("p2p_aggregation_expirationRenewThreshold");

    monitorEnabled = p.getBoolean("p2p_aggregation_monitorEnabled");
    monitorRefreshInterval = p.getLong("p2p_aggregation_monitorRefreshInterval");

    consolidationDelayAfterJoin = p.getLong("p2p_aggregation_consolidationDelayAfterJoin");
    consolidationInterval = p.getLong("p2p_aggregation_consolidationInterval");
    consolidationThreshold = p.getLong("p2p_aggregation_consolidationThreshold");
    consolidationMinObjectsInAggregate = p.getInt("p2p_aggregation_consolidationMinObjectsInAggregate");
    consolidationMinComponentsAlive = p.getDouble("p2p_aggregation_consolidationMinComponentsAlive");

    reconstructionMaxConcurrentLookups = p.getInt("p2p_aggregation_reconstructionMaxConcurrentLookups");

    aggregateLogEnabled = p.getBoolean("p2p_aggregation_aggregateLogEnabled");

    statsGranularity = p.getLong("p2p_aggregation_statsGranularity");
    statsRange = p.getLong("p2p_aggregation_statsRange");
    statsInterval = p.getLong("p2p_aggregation_statsInterval");

    jitterRange = p.getDouble("p2p_aggregation_jitterRange");

    this.aggregateFactory = aggregateFactory;
    if (this.aggregateFactory == null) {
      this.aggregateFactory = getDefaultAggregateFactory();
    }

    this.endpoint = node.buildEndpoint(this, instance);
    this.endpoint.setDeserializer(new MessageDeserializer() {    
      public Message deserialize(InputBuffer buf, short type, int priority,
          NodeHandle sender) throws IOException {
        switch(type) {
//          case AggregationTimeoutMessage.TYPE:
//            return new AggregationTimeoutMessage(buf, endpoint);
        }
        return null;
      }    
    });
    
    this.waitingList = waitingList;
    this.instance = instance;
    
    this.contentDeserializer = new JavaPastContentDeserializer();
    this.contentHandleDeserializer = new JavaPastContentHandleDeserializer();

    this.aggregateStore = aggregateStore;
//    this.aggregateStore.setContentDeserializer(new JavaPastContentDeserializer() {    
    this.aggregateStore.setContentDeserializer(new PastContentDeserializer() {    
      public PastContent deserializePastContent(InputBuffer buf, Endpoint endpoint,
          short contentType) throws IOException {
        switch(contentType) {
          case RawAggregate.TYPE:
            return new RawAggregate(buf, endpoint, contentDeserializer);  
          case NonAggregate.TYPE:
            short subType = buf.readShort();
            return contentDeserializer.deserializePastContent(buf, endpoint, subType);
//            
//          case 0:
//            PastContent content = super.deserializePastContent(buf, endpoint, (short)0);
//            throw new IllegalArgumentException("Unknown Type:"+contentType+" content:"+content.getClass().getName()+" "+content);            
        }
        throw new IllegalArgumentException("Unknown Type:"+contentType);
      }    
    });
    
    this.aggregateStore.setContentHandleDeserializer(new PastContentHandleDeserializer() {    
      public PastContentHandle deserializePastContentHandle(InputBuffer buf, Endpoint endpoint,
          short contentType) throws IOException {
        switch(contentType) {
          case AggregateHandle.TYPE:
            return new AggregateHandle(buf, endpoint);          
        }
        throw new IllegalArgumentException("Unknown Type:"+contentType);
      }    
    });
    
    this.objectStore = objectStore;
    this.node = node;
    this.timers = new Hashtable();
    this.aggregateList = new AggregateList(configFileName, getLocalNodeHandle().getId().toString(), factory, aggregateLogEnabled, instance, environment);
    this.stats = aggregateList.getStatistics(statsGranularity, statsRange, nominalReferenceCount);
    if (policy == null) {
      this.policy = getDefaultPolicy();      
    } else {
      this.policy = policy;
    }
    
    this.factory = factory;
    this.flushWait = null;
    this.rebuildInProgress = false;
    this.monitorIDs = new Vector();

    if (!aggregateList.readOK())
      if (logger.level <= Logger.WARNING) logger.log("Failed to read configuration file; aggregate list must be rebuilt!");
    else 
      if (logger.level <= Logger.INFO) logger.log( "Aggregate list read OK -- current root: " + ((aggregateList.getRoot() == null) ? "null" : aggregateList.getRoot().toStringFull()));

    removeDeadAggregates();

    addTimer(jitterTerm(flushDelayAfterJoin), tiFlush);
    addTimer(jitterTerm(aggrRefreshDelayAfterJoin), tiExpire);
    addTimer(jitterTerm(consolidationDelayAfterJoin), tiConsolidate);
    addTimer(statsInterval, tiStatistics);
    if (monitorEnabled)
      addTimer(monitorRefreshInterval, tiMonitor);
    endpoint.register();
  }

  private static AggregationPolicy getDefaultPolicy() {
    return new AggregationDefaultPolicy();
  }

  private static AggregateFactory getDefaultAggregateFactory() {
    return new RawAggregateFactory(); 
  }
  
  private long jitterTerm(long basis) {
    return (long)((1-jitterRange)*basis) + environment.getRandomSource().nextInt((int)(2*jitterRange*basis));
  }

  /**
   * Schedule a timer event
   *
   * @param timeoutMsec Length of the delay (in milliseconds)
   * @param timeoutID Identifier (to distinguish between multiple timers)
   */
  private void addTimer(long timeoutMsec, char timeoutID) {
    /*
     *  We schedule a GlacierTimeoutMessage with the ID of the
     *  requested timer. This message will be delivered if the
     *  pires and it has not been removed in the meantime.
     */
    CancellableTask timer = endpoint.scheduleMessage(new AggregationTimeoutMessage(timeoutID, getLocalNodeHandle()), timeoutMsec);
    timers.put(new Integer(timeoutID), timer);
  }

  /**
   * Cancel a timer event that has not yet occurred
   *
   * @param timeoutID Identifier of the timer event to be cancelled
   */
  private void removeTimer(int timeoutID) {
    CancellableTask timer = (CancellableTask) timers.remove(new Integer(timeoutID));

    if (timer != null) {
      timer.cancel();
    }
  }

  private void panic(String s) throws Error {
    Error err = new Error("Panic "+s);
    if (logger.level <= Logger.SEVERE) logger.logException( "PANIC: " + s, err);
    throw err;
  }

  public String handleDebugCommand(String command) {
    if (command.indexOf(" ") < 0)
      return null;
  
    String requestedInstance = command.substring(0, command.indexOf(" "));
    String myInstance = "aggr."+instance.substring(instance.lastIndexOf("-") + 1);
    String cmd = command.substring(requestedInstance.length() + 1);
    
    if (!requestedInstance.equals(myInstance) && !requestedInstance.equals("a")) {
      String subResult = null;

      if ((subResult == null) && (aggregateStore instanceof DebugCommandHandler))
        subResult = ((DebugCommandHandler)aggregateStore).handleDebugCommand(command);
      if ((subResult == null) && (objectStore instanceof DebugCommandHandler))
        subResult = ((DebugCommandHandler)objectStore).handleDebugCommand(command);

      return subResult;
    }
  
    if (logger.level <= Logger.INFO) logger.log( "Debug command: "+cmd);
  
    if (cmd.startsWith("status")) {
      return stats.numObjectsTotal+" objects total\n"+stats.numObjectsAlive+" objects alive\n"+
             stats.numAggregatesTotal+" aggregates total\n"+stats.numPointerArrays+" pointer arrays\n"+
             stats.criticalAggregates+" critical aggregates\n"+stats.orphanedAggregates+" orphaned aggregates\n";
    }

    if (cmd.startsWith("insert")) {
      int numObjects = Integer.parseInt(cmd.substring(7));
      String result = "";
      
      for (int i=0; i<numObjects; i++) {
        Id randomID = factory.buildRandomId(environment.getRandomSource());
        result = result + randomID.toStringFull() + "\n";
        insert(
          new DebugContent(randomID, false, 0, new byte[] {}),
          environment.getTimeSource().currentTimeMillis() + 120*SECONDS,
          new Continuation() {
            public void receiveResult(Object o) {
            }
            public void receiveException(Exception e) {
            }
          }
        );
      }
      
      return result + numObjects + " object(s) created\n";
    }

    if (cmd.startsWith("show config")) {
      return 
        "flushDelayAfterJoin = " + (int)(flushDelayAfterJoin / SECONDS) + " sec\n" +
        "flushInterval = " + (int)(flushInterval / SECONDS) + " sec\n" +
        "maxAggregateSize = " + maxAggregateSize + " bytes\n" +
        "maxObjectsInAggregate = " + maxObjectsInAggregate + " objects\n" +
        "maxAggregatesPerRun = " + maxAggregatesPerRun + " aggregates\n" +
        "addMissingAfterRefresh = " + addMissingAfterRefresh + "\n" +
        "nominalReferenceCount = " + nominalReferenceCount + "\n" +
        "maxPointersPerAggregate = " + maxPointersPerAggregate + "\n" +
        "pointerArrayLifetime = " + (int)(pointerArrayLifetime / DAYS) + " days\n" +
        "aggrRefreshInterval = " + (int)(aggrRefreshInterval / SECONDS) + " sec\n" +
        "aggrRefreshDelayAfterJoin = " + (int)(aggrRefreshDelayAfterJoin / SECONDS) + " sec\n" +
        "expirationRenewThreshold = " + (int)(expirationRenewThreshold / HOURS) + " hrs\n" +
        "consolidationDelayAfterJoin = " + (int)(consolidationDelayAfterJoin / SECONDS) + " sec\n" +
        "consolidationInterval = " + (int)(consolidationInterval / SECONDS) + " sec\n" +
        "consolidationThreshold = " + (int)(consolidationThreshold / HOURS) + " hrs\n" +
        "consolidationMinObjectsInAggregate = " + consolidationMinObjectsInAggregate + "\n" +
        "consolidationMinComponentsAlive = " + consolidationMinComponentsAlive + "\n";
    }    

    if (cmd.startsWith("ls")) {
      Enumeration enumeration = aggregateList.elements();
      StringBuffer result = new StringBuffer();
      int numAggr = 0, numObj = 0;

      long now = environment.getTimeSource().currentTimeMillis();
      if (cmd.indexOf("-r") < 0)
        now = 0;

      aggregateList.recalculateReferenceCounts(null);
      aggregateList.resetMarkers();
      while (enumeration.hasMoreElements()) {
        AggregateDescriptor aggr = (AggregateDescriptor) enumeration.nextElement();
        if (!aggr.marker) {
          result.append("***" + aggr.key.toStringFull() + " (" + aggr.objects.length + " obj, " + 
                   aggr.pointers.length + " ptr, " + aggr.referenceCount + " ref, exp=" + 
                   (aggr.currentLifetime - now) + ")\n");
          for (int i=0; i<aggr.objects.length; i++)
            result.append("    #"+i+" "+
              aggr.objects[i].key.toStringFull()+"v"+aggr.objects[i].version +
              ", lt=" + (aggr.objects[i].currentLifetime-now) +
              ", rt=" + (aggr.objects[i].refreshedLifetime-now) +
              ", size=" + aggr.objects[i].size + " bytes\n");
          for (int i=0; i<aggr.pointers.length; i++) 
            result.append("    Ref "+aggr.pointers[i].toStringFull()+"\n");
          result.append("\n");
          aggr.marker = true;
          numAggr ++;
          numObj += aggr.objects.length;
        }
      }

      result.append(numAggr + " aggregate(s), " + numObj + " object(s)");
      
      return result.toString();
    }

    if (cmd.startsWith("write list")) {
      aggregateList.writeToDisk();
      return "Done, new root is "+((aggregateList.getRoot()==null) ? "null" : aggregateList.getRoot().toStringFull());
    }

    if ((cmd.length() >= 5) && cmd.substring(0, 5).equals("reset")) {
      final String[] ret = new String[] { null };

      reset(new Continuation() {
        public void receiveResult(Object o) {
          ret[0] = "result("+o+")";
        }
        public void receiveException(Exception e) {
          ret[0] = "exception("+e+")";
        }
      });
        
      while (ret[0] == null)
        Thread.yield();
      
      return ret[0];
    }

    if (cmd.startsWith("flush")) {
      final String[] ret = new String[] { null };

      flush(new Continuation() {
        public void receiveResult(Object o) {
          ret[0] = "result("+o+")";
        }
        public void receiveException(Exception e) {
          ret[0] = "exception("+e+")";
        }
      });
        
      while (ret[0] == null)
        Thread.yield();
      
      return ret[0];
    }
    
    if (cmd.startsWith("get root")) {
      return "root="+((aggregateList.getRoot()==null) ? "null" : aggregateList.getRoot().toStringFull());
    }

    if (cmd.startsWith("set root")) {
      final String[] ret = new String[] { null };
      setHandle(factory.buildIdFromToString(cmd.substring(9)), new Continuation() {
        public void receiveResult(Object o) {
          ret[0] = "result("+o+")";
        }
        public void receiveException(Exception e) {
          ret[0] = "exception("+e+")";
        }
      });
        
      while (ret[0] == null)
        Thread.yield();
      
      return ret[0];
    }

    if (cmd.startsWith("lookup")) {
      Id id = factory.buildIdFromToString(cmd.substring(7));

      final String[] ret = new String[] { null };
      lookup(id, false, new Continuation() {
        public void receiveResult(Object o) {
          ret[0] = "result("+o+")";
        }
        public void receiveException(Exception e) {
          ret[0] = "exception("+e+")";
        }
      });
      
      while (ret[0] == null)
        Thread.yield();
      
      return "lookup("+id+")="+ret[0];
    }

    if (cmd.startsWith("handles")) {
      String args = cmd.substring(8);
      Id id = factory.buildIdFromToString(args.substring(args.indexOf(' ') + 1));
      int max = Integer.parseInt(args.substring(0, args.indexOf(' ')));

      final String[] ret = new String[] { null };
      lookupHandles(id, max, new Continuation() {
        public void receiveResult(Object o) {
          if (o instanceof PastContentHandle[]) {
            PastContentHandle[] oA = (PastContentHandle[]) o;
            ret[0] = "";
            for (int i=0; i<oA.length; i++)
              ret[0] = ret[0] + "#"+i+" "+oA[i]+"\n";
            ret[0] = ret[0] + oA.length + " handle(s) returned\n";
          } else ret[0] = "result("+o+") -- no handles returned!";
        }
        public void receiveException(Exception e) {
          ret[0] = "exception("+e+")";
        }
      });
      
      while (ret[0] == null)
        Thread.yield();
      
      return "Handles("+max+","+id+"):\n"+ret[0];
    }
      
    if (cmd.startsWith("refresh all")) {
      long expiration = environment.getTimeSource().currentTimeMillis() + Long.parseLong(cmd.substring(12));
      TreeSet ids = new TreeSet();
      String result;
      
      aggregateList.resetMarkers();

      Enumeration enumeration = aggregateList.elements();
      while (enumeration.hasMoreElements()) {
        AggregateDescriptor aggr = (AggregateDescriptor) enumeration.nextElement();
        if (!aggr.marker) {
          aggr.marker = true;
          for (int i=0; i<aggr.objects.length; i++)
            ids.add(aggr.objects[i].key);
        }
      }
      
      if (!ids.isEmpty()) {
        Id[] allIds = (Id[]) ids.toArray(new Id[] {});
        result = "Refreshing " + allIds.length + " keys...\n";

        for (int i=0; i<allIds.length; i++)
          result = result + "#" + i + " " + allIds[i].toStringFull() + "\n";
      
        final String[] ret = new String[] { null };
        refresh(allIds, expiration, new Continuation() {
          public void receiveResult(Object o) {
            ret[0] = "result("+o+")";
          };
          public void receiveException(Exception e) {
            ret[0] = "exception("+e+")";
          }
        });

        while (ret[0] == null)
          Thread.yield();
      
        result = result + ret[0];
      } else result = "Aggregate list is empty; nothing to refresh!";
      
      return result;
    }
      
    if (cmd.startsWith("refresh")) {
      String args = cmd.substring(8);
      String expirationArg = args.substring(args.lastIndexOf(' ') + 1);
      String keyArg = args.substring(0, args.lastIndexOf(' '));

      Id id = factory.buildIdFromToString(keyArg);
      long expiration = environment.getTimeSource().currentTimeMillis() + Long.parseLong(expirationArg);

      final String[] ret = new String[] { null };
      refresh(new Id[] { id }, expiration, new Continuation() {
        public void receiveResult(Object o) {
          ret[0] = "result("+o+")";
        }
        public void receiveException(Exception e) {
          ret[0] = "exception("+e+")";
        }
      });
      
      while (ret[0] == null)
        Thread.yield();
      
      return "refresh("+id+", "+expiration+")="+ret[0];
    }

    if (cmd.startsWith("monitor remove") && monitorEnabled) {
      String[] args = cmd.substring(15).split(" ");
      if (args.length == 1) {        
        int howMany = Integer.parseInt(args[0]);
        
        if (howMany > monitorIDs.size())
          howMany = monitorIDs.size();
        
        for (int i=0; i<howMany; i++)
          monitorIDs.removeElementAt(environment.getRandomSource().nextInt(monitorIDs.size()));
          
        return "Removed "+howMany+" elements; "+monitorIDs.size()+" elements left";
      } else return "Syntax: monitor remove <howMany>";
    }

    if (cmd.startsWith("monitor status") && monitorEnabled) {
      return "Monitor is "+(monitorEnabled ? ("enabled, monitoring "+monitorIDs.size()+" objects") : "disabled");
    }

    if (cmd.startsWith("monitor ls") && monitorEnabled) {
      StringBuffer result = new StringBuffer();
      Enumeration enumeration = monitorIDs.elements();
      
      while (enumeration.hasMoreElements())
        result.append(((Id)enumeration.nextElement()).toStringFull() + "\n");
        
      result.append(monitorIDs.size() + " object(s)");
      return result.toString();
    }

    if (cmd.startsWith("monitor check") && monitorEnabled) {
      final StringBuffer result = new StringBuffer();
      final String[] ret = new String[] { null };
      
      if (monitorIDs.isEmpty())
        return "Add objects first!";

      final long now = environment.getTimeSource().currentTimeMillis();
            
      Continuation c = new Continuation() {
        int currentLookup = 0;
        boolean lookupInAggrStore = false;
        
        public void receiveResult(Object o) {
          if (logger.level <= Logger.FINE) logger.log( "Monitor: Retr "+currentLookup+" a="+lookupInAggrStore+" got "+o);
          Id currentId = (Id) monitorIDs.elementAt(currentLookup);
          PastContentHandle[] handles = (PastContentHandle[]) o;
          GCPastContentHandle handle = null;
          boolean skipToNext = true;
          
          for (int i=0; i<handles.length; i++)
            if (handles[i] != null)
              handle = (GCPastContentHandle) handles[i];
          
          if (!lookupInAggrStore) {
            result.append(currentId.toStringFull() + " - OS ");
            result.append((handle==null) ? "--" : ""+(handle.getExpiration()-now));
            
            AggregateDescriptor adc = (AggregateDescriptor) aggregateList.getADC(currentId);
            if (adc != null) {
              result.append(" AD " + (adc.currentLifetime - now));
              
              int objDescIndex = adc.lookupNewest(currentId);
              if (objDescIndex >= 0) {
                ObjectDescriptor odc = adc.objects[objDescIndex];
                result.append(" OD " + (odc.currentLifetime - now));
                lookupInAggrStore = true;
                skipToNext = false;
                aggregateStore.lookupHandles(adc.key, 1, this);
              } else {
                result.append(" OD ??\n");
              }
            } else {
              result.append(" AD ??\n");
            }
          } else {
            result.append(" AS " + ((handle==null) ? "--\n" : ""+(handle.getExpiration()-now) + "\n"));
            lookupInAggrStore = false;
          }
          
          if (skipToNext) {
            currentLookup++;
            if (currentLookup < monitorIDs.size()) {
              if (logger.level <= Logger.FINE) logger.log( "Monitor: Continuing with element "+currentLookup);
              objectStore.lookupHandles((Id) monitorIDs.elementAt(currentLookup), 1, this);
            } else {
              if (logger.level <= Logger.FINE) logger.log( "Monitor: Done");
              ret[0] = "done";
            }
          }
        }
        public void receiveException(Exception e) {
          if (logger.level <= Logger.WARNING) logger.logException("Montior: Failed, e=",e);
          ret[0] = "done";
        }
      };
      
      objectStore.lookupHandles((Id) monitorIDs.elementAt(0), 1, c);
      while (ret[0] == null)
        Thread.yield();

      return result.toString();
    }

    if (cmd.startsWith("monitor add") && monitorEnabled) {
      String[] args = cmd.substring(12).split(" ");
      if (args.length == 6) {
        final int numFiles = Integer.parseInt(args[0]);
        final int avgBurstSize = Integer.parseInt(args[1]);
        final double sizeSkew = Double.parseDouble(args[2]);
        final int smallSize = Integer.parseInt(args[3]);
        final int largeSize = Integer.parseInt(args[4]);
        final long expiration = environment.getTimeSource().currentTimeMillis() + Long.parseLong(args[5]);
        
        Continuation c = new Continuation() {
          int remainingTotal = numFiles;
          public void receiveResult(Object o) {
            if (remainingTotal > 0) {
              final int burstSize = Math.min((int)((avgBurstSize*0.3) + environment.getRandomSource().nextInt((int)(1.4*avgBurstSize))), remainingTotal);
              final Continuation outerContinuation = this;
              remainingTotal -= burstSize;
              if (logger.level <= Logger.FINE) logger.log( "Inserting burst of size "+burstSize+", remaining objects: "+remainingTotal);
              Continuation c2 = new Continuation() {
                long remainingHere = burstSize;
                public void receiveResult(Object o) {
                  if (remainingHere > 0) {
                    if (logger.level <= Logger.FINE) logger.log( "Continuing burst insert, "+remainingHere+" remaining");
                    int thisAvgSize = ((0.001*environment.getRandomSource().nextInt(1000)) < sizeSkew) ? smallSize : largeSize;
                    int thisSize = (int)(0.3*thisAvgSize + environment.getRandomSource().nextInt((int)(1.4*thisAvgSize)));
                    Id randomID = factory.buildRandomId(environment.getRandomSource());
                    remainingHere --;
                    monitorIDs.add(randomID);
                    insert(new DebugContent(randomID, false, 0, new byte[thisSize]), expiration, this);
                  } else {
                    if (logger.level <= Logger.FINE) logger.log( "Burst insertion complete, flushing...");
                    flush(outerContinuation);
                  }
                }
                public void receiveException(Exception e) {
                  if (logger.level <= Logger.WARNING) logger.logException("Monitor.add component insertion failed: ",e);
                  receiveResult(e);
                }
              };
              
              c2.receiveResult(new Boolean(true));
            } else {
              if (logger.level <= Logger.INFO) logger.log( "Monitor add completed, "+numFiles+" objects created successfully");
            }            
          }
          public void receiveException(Exception e) {
            if (logger.level <= Logger.WARNING) logger.logException("Monitor.add aggregate insertion failed: ",e);
            receiveResult(e);
          }
        };
        
        c.receiveResult(new Boolean(true));
        return "In progress...";
      }
      
      return "Syntax: monitor add <#files> <avgBurstSize> <sizeSkew> <smallSize> <largeSize> <lifetime>";
    }
      
    if (cmd.startsWith("killall")) {
      String args = cmd.substring(8);
      String expirationArg = args.substring(args.lastIndexOf(' ') + 1);
      String keyArg = args.substring(0, args.lastIndexOf(' '));

      Id id = factory.buildIdFromToString(keyArg);
      long expiration = environment.getTimeSource().currentTimeMillis() + Long.parseLong(expirationArg);

      AggregateDescriptor aggr = (AggregateDescriptor) aggregateList.getADC(id);
      if (aggr != null) {
        aggregateList.setAggregateLifetime(aggr, Math.min(aggr.currentLifetime, expiration));
        for (int i=0; i<aggr.objects.length; i++) {
          aggregateList.setObjectCurrentLifetime(aggr, i, Math.min(aggr.objects[i].currentLifetime, expiration));
          aggregateList.setObjectRefreshedLifetime(aggr, i, Math.min(aggr.objects[i].refreshedLifetime, expiration));
        }
        return "OK";
      }
      
      return "Aggregate "+id+" not found in aggregate list";
    }

    if (cmd.startsWith("waiting")) {
      Iterator iter = waitingList.scan().getIterator();
      String result = "";

      result = result + waitingList.scan().numElements()+ " object(s) waiting\n";
      
      while (iter.hasNext()) {
        Id thisId = (Id) iter.next();
        result = result + thisId.toStringFull()+" "+waitingList.getMetadata(thisId)+"\n";
      }
        
      return result;
    }
      
    if (cmd.startsWith("vlookup")) {
      String[] vkeyS = cmd.substring(8).split("v");
      Id key = factory.buildIdFromToString(vkeyS[0]);
      long version = Long.parseLong(vkeyS[1]);

      final String[] ret = new String[] { null };
      lookup(key, version, new Continuation() {
        public void receiveResult(Object o) {
          ret[0] = "result("+o+")";
        }
        public void receiveException(Exception e) {
          ret[0] = "exception("+e+")";
        }
      });
      
      while (ret[0] == null)
        Thread.yield();
      
      return "vlookup("+key+"v"+version+")="+ret[0];
    }
    
    return null;
  }

  private void removeDeadAggregates() {
    Vector toRemove = new Vector();
    Enumeration enumeration = aggregateList.elements();
    long now = environment.getTimeSource().currentTimeMillis();
  
    /* Note: Multiple keys are mapped to a single aggregate descriptor, so the same ADC
       may be returned multiple times - but we must delete it only once! */
    
    while (enumeration.hasMoreElements()) {
      AggregateDescriptor adc = (AggregateDescriptor) enumeration.nextElement();
      if (adc.currentLifetime < (now - aggregateGracePeriod)) {
        if (!toRemove.contains(adc))
          toRemove.add(adc);
        if (logger.level <= Logger.WARNING) logger.log("Scheduling dead aggregate for removal: "+adc.key.toStringFull()+"(expired "+adc.currentLifetime+")");
      }
    }
    
    if (toRemove.size() > 0) {
      if (logger.level <= Logger.INFO) logger.log( "Removing "+toRemove.size()+" dead aggregates...");
      
      Enumeration rem = toRemove.elements();
      while (rem.hasMoreElements())
        aggregateList.removeAggregateDescriptor((AggregateDescriptor) rem.nextElement());
    }
  }

  private void storeAggregate(final Aggregate aggr, final long expiration, final ObjectDescriptor[] desc, final Id[] pointers, final Continuation command) {
    if (logger.level <= Logger.FINE) logger.log( "storeAggregate() schedules content hash computation...");
    endpoint.process(new Executable() {
      public Object execute() {
        if (logger.level <= Logger.FINE) logger.log( "storeAggregate() starts working on content hash...");
        return factory.buildId(aggr.getContentHash());
      }
    }, new Continuation() {
      public void receiveResult(Object o) {
        if (o instanceof Id) {
          aggr.setId((Id) o);
          if (logger.level <= Logger.INFO) logger.log( "Storing aggregate, CH="+aggr.getId()+", expiration="+expiration+" (rel "+(expiration-environment.getTimeSource().currentTimeMillis())+") with "+desc.length+" objects:");
          for (int j=0; j<desc.length; j++)
            if (logger.level <= Logger.INFO) logger.log( "#"+j+": "+desc[j]);

          Continuation c = new Continuation() {
            public void receiveResult(Object o) {
              AggregateDescriptor adc = new AggregateDescriptor(
                aggr.getId(),
                expiration,
                desc,
                pointers
              );

              if (o instanceof Boolean[]) {
                aggregateList.addAggregateDescriptor(adc);
                aggregateList.setRoot(aggr.getId());
                aggregateList.writeToDisk();
                if (logger.level <= Logger.FINE) logger.log( "Aggregate inserted successfully");
                command.receiveResult(new Boolean(true));
              } else {
                if (logger.level <= Logger.WARNING) logger.log("Unexpected result in aggregate insert (commit): "+o);
                command.receiveException(new AggregationException("Unexpected result (commit): "+o));
              }
            }
            public void receiveException(Exception e) {
              command.receiveException(e);
            }
          };
    
          if (aggregateStore instanceof GCPast) 
            ((GCPast)aggregateStore).insert(aggr, expiration, c);
          else
            aggregateStore.insert(aggr, c);
        } else {
          if (logger.level <= Logger.WARNING) logger.log("storeAggregate() cannot determine content hash, received "+o);
          command.receiveException(new AggregationException("storeAggregate() cannot determine content hash"));
        }
      }
      public void receiveException(Exception e) {
        if (logger.level <= Logger.WARNING) logger.logException("storeAggregate() cannot determine content hash, exception ",e);
        command.receiveException(e);
      }
    });
  }

  private void flushComplete(Object o) {
    if (flushWait != null) {
      Continuation c = flushWait;
      flushWait = null;
      if (o instanceof Exception) 
        c.receiveException((Exception) o);
      else 
        c.receiveResult(o);
    }
  }

  private void formAggregates(final Continuation command) {
    if (flushWait != null) {
      if (logger.level <= Logger.INFO) logger.log( "Flush in progress... daisy-chaining continuation");
      final Continuation parent = flushWait;
      flushWait = new Continuation() {
        public void receiveResult(Object o) {
          if (logger.level <= Logger.INFO) logger.log( "Daisy-chain receiveResult(), restarting "+command);
          parent.receiveResult(o);
          formAggregates(command);
        }
        public void receiveException(Exception e) {
          if (logger.level <= Logger.INFO) logger.log( "Daisy-chain receiveException(), restarting "+command);
          parent.receiveException(e);
          formAggregates(command);
        }
      };
      return;
    }

    flushWait = command;
      
    IdSet waitingKeys = waitingList.scan();
    if (waitingKeys.numElements() == 0) {
      if (logger.level <= Logger.INFO) logger.log( "NO BINS TO PACK");
      flushComplete(new Boolean(true));
      return;
    }
  
    if (logger.level <= Logger.INFO) logger.log( "BIN PACKING STARTED");

    Vector currentAggregate = new Vector();
    Vector aggregates = new Vector();
    Vector deletionVector = new Vector();
    Iterator iter = waitingKeys.getIterator();
    long currentAggregateSize = 0;
    int currentObjectsInAggregate = 0;
    
    while (true) {
      ObjectDescriptor thisObject = null;
      boolean mustAddObject = false;

      if (aggregates.size() >= maxAggregatesPerRun)
        break;
      
      while (iter.hasNext()) {
        Id thisId = (Id) iter.next();
        thisObject = (ObjectDescriptor) waitingList.getMetadata(thisId);
        if (thisObject != null) {
          thisObject = new ObjectDescriptor(
            thisObject.key, thisObject.version, thisObject.currentLifetime,
            thisObject.refreshedLifetime, thisObject.size
          );
          if ((((currentAggregateSize + thisObject.size) <= maxAggregateSize) || currentAggregate.isEmpty()) && (currentObjectsInAggregate < maxObjectsInAggregate)) {
            currentAggregateSize += thisObject.size;
            currentObjectsInAggregate ++;
            currentAggregate.add(thisObject);
          } else {
            mustAddObject = true;
            break;
          }
        } else {
          if (logger.level <= Logger.WARNING) logger.log("Metadata in waiting object "+thisId.toStringFull()+" appears to be damaged. Scheduling for deletion...");
          deletionVector.add(thisId);
        }
      }
      
      int numObjectsInAggregate = currentAggregate.size();
      if (numObjectsInAggregate < 1) {
        if (logger.level <= Logger.WARNING) logger.log("Waiting list seems to consist entirely of damaged objects -- please remove!");
        flushComplete(new Boolean(true));
        return;
      }

      ObjectDescriptor[] desc = new ObjectDescriptor[numObjectsInAggregate];
      for (int i=0; i<numObjectsInAggregate; i++) {
        desc[i] = (ObjectDescriptor) currentAggregate.elementAt(i);
        if (logger.level <= Logger.FINE) logger.log( "#"+i+": "+desc[i].key+" "+desc[i].size+" bytes");
      }

      aggregates.add(desc);
      currentAggregate.clear();
      currentObjectsInAggregate = 0;
      currentAggregateSize = 0;
        
      if (mustAddObject) {
        currentAggregate.add(thisObject);
        currentAggregateSize += thisObject.size;
      } else {
        if (!iter.hasNext())
          break;
      }
    }

    Enumeration delenda = deletionVector.elements();
    while (delenda.hasMoreElements()) {
      final Id thisId = (Id) delenda.nextElement();
      if (logger.level <= Logger.INFO) logger.log( "Deleting object "+thisId.toStringFull()+" from waiting list (broken metadata)");
      waitingList.unstore(thisId, new Continuation() {
        public void receiveResult(Object o) {
          if (logger.level <= Logger.FINE) logger.log( "Successfully deleted: "+thisId);
        }
        public void receiveException(Exception e) {
          if (logger.level <= Logger.WARNING) logger.logException("Cannot delete: "+thisId+", e=",e);
        }
      });
    }
  
    Continuation.MultiContinuation c = new Continuation.MultiContinuation(new Continuation() {
      public void receiveResult(Object o) {
        flushComplete(new Boolean(true));
      }
      public void receiveException(Exception e) {
        flushComplete(e);
      }
    }, aggregates.size());
      
    for (int i=0; i<aggregates.size(); i++) {
      final ObjectDescriptor[] desc = (ObjectDescriptor[]) aggregates.elementAt(i);
      final GCPastContent[] obj = new GCPastContent[desc.length];
      final long aggrExpirationF = chooseAggregateLifetime(desc, environment.getTimeSource().currentTimeMillis(), 0);
      final Continuation thisContinuation = c.getSubContinuation(i);
      final int iF = i;
      
      if (logger.level <= Logger.FINE) logger.log( "Retrieving #"+i+".0: "+desc[0].key);
      waitingList.getObject(new VersionKey(desc[0].key, desc[0].version), new Continuation() {
        int currentQuery = 0;
        public void receiveResult(Object o) {
          if ((o!=null) && (o instanceof GCPastContent)) {
            obj[currentQuery++] = (GCPastContent) o;
            if (currentQuery < desc.length) {
              if (logger.level <= Logger.FINE) logger.log( "Retrieving #"+iF+"."+currentQuery+": "+desc[currentQuery].key);
              waitingList.getObject(new VersionKey(desc[currentQuery].key, desc[currentQuery].version), this);
            } else {
              Id[] pointers = aggregateList.getSomePointers(nominalReferenceCount, maxPointersPerAggregate, null);
              storeAggregate(aggregateFactory.buildAggregate(obj, pointers), aggrExpirationF, desc, pointers, new Continuation() {
                public void receiveResult(Object o) {
                  final Continuation.MultiContinuation c2 = new Continuation.MultiContinuation(thisContinuation, desc.length);
                  for (int i=0; i<desc.length; i++) {
                    final Continuation c2s = c2.getSubContinuation(i);
                    waitingList.unstore(new VersionKey(desc[i].key, desc[i].version), new Continuation() {
                      public void receiveResult(Object o) {
                        c2s.receiveResult(o);
                      }
                      public void receiveException(Exception e) {
                        if (logger.level <= Logger.WARNING) logger.logException("Exception while unstoring aggregate component: ",e);
                        c2s.receiveException(e);
                      }
                    });
                  }
                }
                public void receiveException(Exception e) {
                  if (logger.level <= Logger.WARNING) logger.logException("Exception while storing new aggregate: ",e);
                  thisContinuation.receiveException(e);
                }
              });
            }
          } else { 
            if (logger.level <= Logger.WARNING) logger.log("Aggregation cannot retrieve "+desc[currentQuery].key+" (found o="+o+")");
            thisContinuation.receiveException(new AggregationException("Cannot retrieve object from waiting list: "+desc[currentQuery].key));
          }
        }
        public void receiveException(Exception e) {
          if (logger.level <= Logger.WARNING) logger.log("Exception while building aggregate: "+e);
          thisContinuation.receiveException(e);
        }
      });
    }
  }

  private long chooseAggregateLifetime(ObjectDescriptor[] components, long now, long currentLifetime) {
    long maxLifetime = 0;

    for (int i=0; i<components.length; i++)
      if (components[i].refreshedLifetime > maxLifetime)
        maxLifetime = components[i].refreshedLifetime;

    return maxLifetime;
  }

  private void refreshAggregates() {
    Enumeration enumeration = aggregateList.elements();
    long now = environment.getTimeSource().currentTimeMillis();
    Vector removeList = new Vector();
    final Vector refreshAggregateList = new Vector();
    final Vector refreshLifetimeList = new Vector();

    if (logger.level <= Logger.INFO) logger.log( "Checking aggregate lifetimes");

    aggregateList.resetMarkers();
    while (enumeration.hasMoreElements()) {
      AggregateDescriptor aggr = (AggregateDescriptor) enumeration.nextElement();
      if (!aggr.marker) {
        aggr.marker = true;
        
        boolean isBeingRefreshed = false;
        if (aggr.currentLifetime < (now + expirationRenewThreshold)) {
          long newLifetime = chooseAggregateLifetime(aggr.objects, now, aggr.currentLifetime);
          if (newLifetime > aggr.currentLifetime) {
            if (logger.level <= Logger.INFO) logger.log( "Refreshing aggregate "+aggr.key.toStringFull()+", new expiration is "+newLifetime);
            isBeingRefreshed = true;
            
            refreshAggregateList.add(aggr);
            refreshLifetimeList.add(new Long(newLifetime));
          }
        }
            
        if ((aggr.currentLifetime < now) && !isBeingRefreshed) {
          if (logger.level <= Logger.FINE) logger.log( "Adding expired aggregate "+aggr.key+" to remove list");
          removeList.add(aggr);
        }
      }
    }

    boolean deletedOne = false;
    while (!removeList.isEmpty()) {
      AggregateDescriptor aggr = (AggregateDescriptor) removeList.elementAt(0);
      if (logger.level <= Logger.INFO) logger.log( "Removing expired aggregate "+aggr.key.toStringFull()+" from list");
      removeList.removeElementAt(0);
      deletedOne = true;
      aggregateList.removeAggregateDescriptor(aggr);
    }
    
    if (deletedOne)
      aggregateList.writeToDisk();

    if (!refreshAggregateList.isEmpty()) {
      if (logger.level <= Logger.INFO) logger.log( "Refreshing "+refreshAggregateList.size()+" aggregate(s)");
      if (aggregateStore instanceof GCPast) {
        Id[] ids = new Id[refreshAggregateList.size()];
        long[] lifetimes = new long[refreshAggregateList.size()];
        for (int i=0; i<refreshAggregateList.size(); i++) {
          ids[i] = ((AggregateDescriptor) refreshAggregateList.elementAt(i)).key;
          lifetimes[i] = ((Long) refreshLifetimeList.elementAt(i)).longValue();
        }
        
        ((GCPast)aggregateStore).refresh(ids, lifetimes, new Continuation() {
          public void receiveResult(Object o) {
            Object[] results = (Object[]) o;
            if (logger.level <= Logger.FINE) logger.log( "Received refresh results for "+results.length+" aggregates");
            int numOk = 0;
            
            for (int i=0; i<results.length; i++) {
              if (results[i] instanceof Boolean) {
                AggregateDescriptor aggr = (AggregateDescriptor) refreshAggregateList.elementAt(i);
                long newLifetime = ((Long) refreshLifetimeList.elementAt(i)).longValue();
                if (logger.level <= Logger.FINE) logger.log( "Aggregate #"+i+" ("+aggr.key.toStringFull()+"): OK, new lifetime is "+newLifetime);
                aggregateList.refreshAggregate(aggr, newLifetime);
                numOk ++;
              } else {
                AggregateDescriptor aggr = (AggregateDescriptor) refreshAggregateList.elementAt(i);
                Exception e = (Exception) results[i];
                if (logger.level <= Logger.WARNING) logger.logException("Aggregate #"+i+" ("+aggr.key.toStringFull()+"): Refresh failed, e=",e);
              }
            }
            
            aggregateList.writeToDisk();
            if (logger.level <= Logger.INFO) logger.log( "Refresh complete, "+numOk+"/"+results.length+" aggregates refreshed OK");
          }
          public void receiveException(Exception e) {
            if (logger.level <= Logger.WARNING) logger.logException("Interface contract broken; exception "+e+" returned directly",e);
          }
        });
      } else {
        if (logger.level <= Logger.FINE) logger.log( "Aggregate store does not support GC; refreshing directly");
        for (int i=0; i<refreshAggregateList.size(); i++) {
          AggregateDescriptor aggr = (AggregateDescriptor) refreshAggregateList.elementAt(i);
          long newLifetime = ((Long) refreshLifetimeList.elementAt(i)).longValue();
          aggregateList.refreshAggregate(aggr, newLifetime);
        }
      }
    }
  }

  private void consolidateAggregates() {
    final long now = environment.getTimeSource().currentTimeMillis();
    Enumeration enumeration = aggregateList.elements();
    Vector candidateList = new Vector();

    if (logger.level <= Logger.INFO) logger.log( "Looking for aggregates to consolidate");
    
    aggregateList.resetMarkers();
    while (enumeration.hasMoreElements()) {
      AggregateDescriptor aggr = (AggregateDescriptor) enumeration.nextElement();
      if (!aggr.marker) {
        aggr.marker = true;
        
        /* An aggregate is a candidate for consolidation iff
              - it does not need to be refreshed yet, but has reached a certain age
                (to prevent consolidation of 'garbage'), and
              - it does not contain enough objects, or too many of them are dead
           Pointer arrays are never consolidated; they expire after some time.
        */
        
        if ((aggr.currentLifetime > (now + expirationRenewThreshold)) &&
            (aggr.currentLifetime < (now + consolidationThreshold)) &&
            (aggr.objectsAliveAt(now) > 0)) {
          float fractionAlive = ((float)aggr.objectsAliveAt(now)) / aggr.objects.length;
          if ((aggr.objects.length < consolidationMinObjectsInAggregate) ||
              (fractionAlive < consolidationMinComponentsAlive)) {
            if (logger.level <= Logger.FINE) logger.log( "Can consolidate: "+aggr.key.toStringFull()+", "+aggr.objectsAliveAt(now)+"/"+aggr.objects.length+" alive");
            candidateList.add(aggr);
          }
        }
      }
    }

    if (candidateList.isEmpty()) {
      if (logger.level <= Logger.INFO) logger.log( "No candidates for consolidation");
      return;
    }
    
    if (logger.level <= Logger.FINE) logger.log( candidateList.size() + " candidate(s) for consolidation");
    
    final Vector componentList = new Vector();
    int objectsSoFar = 0;
    int bytesSoFar = 0;
    
    while (!candidateList.isEmpty()) {
      AggregateDescriptor adc = (AggregateDescriptor) candidateList.remove(environment.getRandomSource().nextInt(candidateList.size()));
      componentList.add(adc);
      if (logger.level <= Logger.FINE) logger.log( "Picked candidate "+adc.key.toStringFull()+" ("+adc.objectsAliveAt(now)+"/"+adc.objects.length+" objects, "+adc.bytesAliveAt(now)+" bytes alive)");
      objectsSoFar += adc.objectsAliveAt(now);
      bytesSoFar += adc.bytesAliveAt(now);
      
      int p = 0;
      while (p < candidateList.size()) {
        AggregateDescriptor adx = (AggregateDescriptor) candidateList.elementAt(p);
        if (((adx.objectsAliveAt(now) + objectsSoFar) > maxObjectsInAggregate) ||
            ((adx.bytesAliveAt(now) + bytesSoFar) > maxAggregateSize))
          candidateList.removeElementAt(p);
        else
          p++;
      }
    }
    
    if (componentList.isEmpty() || (objectsSoFar < consolidationMinObjectsInAggregate)) {
      if (logger.level <= Logger.INFO) logger.log( "Not enough objects ("+objectsSoFar+" found, "+consolidationMinObjectsInAggregate+" required), postponing...");
      return;
    }

    if (logger.level <= Logger.FINE) logger.log( "Consolidation: Decided to consolidate "+objectsSoFar+" objects from "+componentList.size()+" aggregates ("+bytesSoFar+" bytes)");
    
    final AggregateDescriptor[] adc = (AggregateDescriptor[]) componentList.toArray(new AggregateDescriptor[] {});
    final Aggregate[] aggr = new Aggregate[adc.length];
    final int objectsTotal = objectsSoFar;
    final Id firstKey = adc[0].key;

    if (logger.level <= Logger.FINE) logger.log( "Consolidation: Fetching aggregate #0: "+firstKey.toStringFull());
    aggregateStore.lookup(firstKey, new Continuation() {
      int currentLookup = 0;
      public void receiveResult(Object o) {
        if (o instanceof Aggregate) {
          aggr[currentLookup] = (Aggregate) o;
          currentLookup ++;
          if (currentLookup >= componentList.size()) {
            RawGCPastContent[] components = new RawGCPastContent[objectsTotal];
            ObjectDescriptor[] desc = new ObjectDescriptor[objectsTotal];
            int componentIndex = 0;

            if (logger.level <= Logger.INFO) logger.log( "Consolidation: All aggregates fetched OK, forming new aggregate...");

            for (int i=0; i<adc.length; i++) {
              for (int j=0; j<adc[i].objects.length; j++) {
                if (adc[i].objects[j].isAliveAt(now)) {
                  
                  // just for debugging...
                  GCPastContent temp = aggr[i].getComponent(j);
//                  if (!(temp instanceof RawGCPastContent)) {
//                    throw new RuntimeException(temp+" is not a RawGCPastContent"); 
//                  }
                  
                  components[componentIndex] = (RawGCPastContent)temp;
                  desc[componentIndex] = adc[i].objects[j];
                  if (logger.level <= Logger.FINE) logger.log( "  #"+componentIndex+": "+adc[i].objects[j].key.toStringFull());
                  componentIndex ++;
                } else {
                  if (logger.level <= Logger.FINE) logger.log( "Skipped (dead): "+adc[i].objects[j].key.toStringFull());
                }
              }
            }

            Id[] obsoleteAggregates = new Id[adc.length];
            for (int i=0; i<adc.length; i++)
              obsoleteAggregates[i] = adc[i].key;

            Id[] pointers = aggregateList.getSomePointers(nominalReferenceCount, maxPointersPerAggregate, obsoleteAggregates);
            final long aggrExpirationF = chooseAggregateLifetime(desc, environment.getTimeSource().currentTimeMillis(), 0);

            storeAggregate(aggregateFactory.buildAggregate(components, pointers), aggrExpirationF, desc, pointers, new Continuation() {
              public void receiveResult(Object o) {
                if (logger.level <= Logger.INFO) logger.log( "Consolidated Aggregate stored OK, removing old descriptors...");
                for (int i=0; i<adc.length; i++) {
                  if (logger.level <= Logger.FINE) logger.log( "Removing "+adc[i].key.toStringFull()+" ...");
                  aggregateList.removeAggregateDescriptor(adc[i]);
                }
                
                aggregateList.writeToDisk();
                if (logger.level <= Logger.INFO) logger.log( "Consolidation completed, "+objectsTotal+" objects from "+aggr.length+" aggregates consolidated");
              }
              public void receiveException(Exception e) {
                if (logger.level <= Logger.WARNING) logger.logException("Exception during consolidation store: e="+e+" -- aborting",e);
              }
            });
          } else {
            if (logger.level <= Logger.FINE) logger.log( "Consolidation: Fetching aggregate #"+currentLookup+": "+adc[currentLookup].key.toStringFull());
            aggregateStore.lookup(adc[currentLookup].key, this);
          }
        }
      }
      public void receiveException(Exception e) {
        if (logger.level <= Logger.WARNING) logger.logException("Exception during consolidation lookup "+adc[currentLookup].key.toStringFull()+": "+e+" -- aborting",e);
      }
    });
  }

  private void reconnectTree() {
  
    if (rebuildInProgress) {
      if (logger.level <= Logger.INFO) logger.log( "Skipping connectivity check (rebuild in progress)");
      return;
    }
  
    if (logger.level <= Logger.INFO) logger.log( "Checking for disconnections");
    
    Id[] disconnected = aggregateList.getSomePointers(1, maxPointersPerAggregate, null);
    if (disconnected.length < 2) {
      Id newRoot = (disconnected.length == 1) ? disconnected[0] : null;
      Id currentRoot = aggregateList.getRoot();
      if (((newRoot == null) && (currentRoot != null)) || ((newRoot != null) && (currentRoot == null)) || 
          ((newRoot != null) && (currentRoot != null) && !newRoot.equals(currentRoot)))
        aggregateList.setRoot(newRoot);
      if (logger.level <= Logger.INFO) logger.log( "No aggregates disconnected (n="+disconnected.length+")");
      if (logger.level <= Logger.FINE) logger.log( "root="+((aggregateList.getRoot() == null) ? "null" : aggregateList.getRoot().toStringFull()));
      return;
    }
    
    if (logger.level <= Logger.INFO) logger.log( "Found "+disconnected.length+" disconnected aggregates; inserting pointer array");
    storeAggregate(
      aggregateFactory.buildAggregate(new GCPastContent[] {}, disconnected), 
      environment.getTimeSource().currentTimeMillis() + pointerArrayLifetime,
      new ObjectDescriptor[] {},
      disconnected,
      new Continuation() {
        public void receiveResult(Object o) {
          if (logger.level <= Logger.FINE) logger.log( "Successfully inserted pointer array");
        }
        public void receiveException(Exception e) {
          if (logger.level <= Logger.WARNING) logger.logException("Error while inserting pointer array: ",e);
        }
      }
    );
  }

  private void timerExpired(char timerID) {
    if (logger.level <= Logger.FINE) logger.log( "TIMER EXPIRED: #" + (int) timerID);

    switch (timerID) {
      case tiFlush :
      {
        if (logger.level <= Logger.INFO) logger.log( "Scheduled flush, waiting list: "+waitingList.getSize());
        
        formAggregates(new Continuation() {
          public void receiveResult(Object o) { 
            if (logger.level <= Logger.FINE) logger.log( "Scheduled flush: Success (o="+o+")");
          }
          public void receiveException(Exception e) {
            if (logger.level <= Logger.WARNING) logger.logException("Scheduled flush: Failure (e="+e+")",e);
          }
        });
        
        if (logger.level <= Logger.INFO) logger.log( "Waiting list: "+waitingList.getSize()+" Scan: "+getNumObjectsWaiting()+" Max: "+(maxObjectsInAggregate * maxAggregatesPerRun));
        
        if (getNumObjectsWaiting() >= (maxObjectsInAggregate * maxAggregatesPerRun)) {
          if (logger.level <= Logger.INFO) logger.log( "Retrying later");
          addTimer(jitterTerm(flushStressInterval), tiFlush);
        } else {
          if (logger.level <= Logger.INFO) logger.log( "OK, waiting for next deadline");
          addTimer(jitterTerm(flushInterval), tiFlush);
        }
        
        break;
      }
      case tiExpire :
      {
        refreshAggregates();
        reconnectTree();
        addTimer(jitterTerm(aggrRefreshInterval), tiExpire);
        break;
      }
      case tiConsolidate :
      {
        consolidateAggregates();
        addTimer(jitterTerm(consolidationInterval), tiConsolidate);
        break;
      }
      case tiMonitor :
      {
        Id[] ids = (Id[]) monitorIDs.toArray(new Id[] {});
        if (logger.level <= Logger.INFO) logger.log( "Monitor: Refreshing "+ids.length+" objects");
        refresh(ids, environment.getTimeSource().currentTimeMillis() + 3 * monitorRefreshInterval, new Continuation() {
          public void receiveResult(Object o) {
            if (logger.level <= Logger.FINE) logger.log( "Monitor: Refresh completed, result="+o);
          }
          public void receiveException(Exception e) {
            if (logger.level <= Logger.FINE) logger.logException("Monitor: Refresh failed, exception=",e);
          }
        });
      
        addTimer(monitorRefreshInterval, tiMonitor);
        break;
      }
      case tiStatistics :
      {
        stats = aggregateList.getStatistics(statsGranularity, statsRange, nominalReferenceCount);
        stats.dump(environment.getLogManager().getLogger(AggregationStatistics.class, instance));
        addTimer(statsInterval, tiStatistics);
        break;
      }
      default:
      {
        panic("Unknown timer expired: " + (int) timerID);
      }
    }
  }

  private void refreshInObjectStore(Id[] ids, long[] expirations, Continuation command) {
    if (objectStore instanceof GCPast) {
      ((GCPast)objectStore).refresh(ids, expirations, command);
    } else {
      command.receiveResult(new Boolean(true));
    }
  }
  
  public void refresh(Id[] ids, long expiration, Continuation command) {
    long[] expirations = new long[ids.length];
    Arrays.fill(expirations, expiration);
    refresh(ids, expirations, command);
  }
  
  public void refresh(final Id[] ids, final long[] expirations, final Continuation command) {
    if (ids.length < 1) {
      command.receiveResult(new Boolean[] {});
      return;
    }

    if (logger.level <= Logger.INFO) logger.log( "Refreshing "+ids.length+" keys");

    refreshInObjectStore(ids, expirations, new Continuation() {
      Object[] result;
      
      public void receiveResult(Object o) {
        if (o instanceof Object[]) {
          result = (Object[]) o;
        } else {
          if (logger.level <= Logger.WARNING) logger.log("refresh: ObjectStore result is of incorrect type; expected Object[], got "+o);
          result = new Object[ids.length];
          for (int i=0; i<ids.length; i++)
            result[i] = o;
        }

        refreshInAggregates();
      }
      public void receiveException(Exception e) {
        result = new Object[ids.length];
        for (int i=0; i<ids.length; i++)
          result[i] = e;
        if (logger.level <= Logger.WARNING) logger.logException("", e);
        refreshInAggregates();
      }
      private void refreshInAggregates() {
        Continuation c = new Continuation() {
          public void receiveResult(Object o) {
            aggregateList.writeToDisk(); 
            command.receiveResult(o);
          }
          public void receiveException(Exception e) {
            if (logger.level <= Logger.WARNING) logger.logException("", e);
            command.receiveException(e);
          }
        };

        refreshInternal(ids, expirations, result, c);
      }
    });
  }
  
  public void refresh(final Id[] ids, final long[] versions, final long[] expirations, final Continuation command) {
    final Object result[] = new Object[ids.length];
    
    for (int i=0; i<ids.length; i++) {
      if (logger.level <= Logger.INFO) logger.log( "Refresh("+ids[i]+"v"+versions[i]+", expiration="+expirations[i]+")");

      AggregateDescriptor adc = (AggregateDescriptor) aggregateList.getADC(new VersionKey(ids[i], versions[i]));
      if (adc!=null) {
        int objDescIndex = adc.lookupSpecific(ids[i], versions[i]);
        if (objDescIndex < 0) {
          result[i] = new AggregationException("Inconsistency detected in aggregate list -- try restarting the application");
        } else {
          if (adc.objects[objDescIndex].refreshedLifetime < expirations[i])
            aggregateList.setObjectRefreshedLifetime(adc, objDescIndex, expirations[i]);

          result[i] = new Boolean(true);
        }
      } else result[i] = new AggregationException("Not found");
    }
      
    if (objectStore instanceof VersioningPast) {
      ((VersioningPast)objectStore).refresh(ids, versions, expirations, new Continuation() {
        public void receiveResult(Object o) {
          if (o instanceof Object[]) {
            Object[] subresult = (Object[]) o;
            for (int i=0; i<result.length; i++)
              if ((result[i] instanceof Boolean) && !(subresult[i] instanceof Boolean))
                result[i] = subresult[i];
          } else {
            Exception e = new AggregationException("Object store returns unexpected result: "+o);
            for (int i=0; i<result.length; i++)
              result[i] = e;
          }
          
          command.receiveResult(result);
        }
        public void receiveException(Exception e) {
          command.receiveException(e);
        }
      });
    } else {
      command.receiveResult(result);
    }
  }

  private void refreshInternal(final Id[] ids, final long[] expirations, final Object[] result, final Continuation command) {
    if (logger.level <= Logger.INFO) logger.log( "refreshInternal: Accepted "+ids.length+" keys, starting with first key...");
    
    final Continuation theContinuation = new Continuation() {
      int objectsMissing = 0;
      int objectsFetched = 0;
      int currentIndex = -1;

      public void receiveResult(Object o) {
        Object lastResult = o;
        while (true) {
          if (currentIndex >= 0) {
            if (logger.level <= Logger.FINE) logger.log( "receiveResult("+lastResult+") for index "+currentIndex+", length="+ids.length);
            if (logger.level <= Logger.FINE) logger.log( "Internal refresh of "+ids[currentIndex].toStringFull()+" returned "+lastResult);
            result[currentIndex] = lastResult;
          }

          /* If we completed the entire list, return */

          currentIndex ++;
          if (currentIndex >= ids.length) {
            if (objectsMissing > 0) 
              if (logger.level <= Logger.WARNING) logger.log("refresh: "+objectsMissing+"/"+ids.length+" objects not in aggregate list, fetched "+objectsFetched+" (max "+maxReaggregationPerRefresh+")");
            
            int nOK = 0;
            for (int i=0; i<ids.length; i++)
              if (result[i] instanceof Boolean)
                nOK ++;
              
            if (logger.level <= Logger.INFO) logger.log( "refreshInternal: Processed "+ids.length+" keys, completed "+nOK);
              for (int i=0; i<ids.length; i++)
                if (logger.level <= Logger.FINER) logger.log(" - "+ids[i].toStringFull()+": "+result[i]);
            
            command.receiveResult(result);
            return;
          }
        
          final Id id = ids[currentIndex];
          final long expiration = expirations[currentIndex];
          if (logger.level <= Logger.INFO) logger.log( "Refresh("+id.toStringFull()+", expiration="+expiration+") started");
        
          /* In the common case, the aggregate is in the list, and we can simply update its lifetime there */
          
          AggregateDescriptor adc = (AggregateDescriptor) aggregateList.getADC(id);
          if (adc!=null) {
            int objDescIndex = adc.lookupNewest(id);
            if (objDescIndex < 0) {
              if (logger.level <= Logger.WARNING) logger.log("NL: Aggregate found, but object not found in aggregate?!? -- aborted");
              command.receiveException(new AggregationException("Inconsistency detected in aggregate list -- try restarting the application"));
              return;
            }

            if (adc.objects[objDescIndex].refreshedLifetime < expiration) {
              if (logger.level <= Logger.FINE) logger.log( "Changing expiration date from "+adc.objects[objDescIndex].refreshedLifetime+" to "+expiration);
              aggregateList.setObjectRefreshedLifetime(adc, objDescIndex, expiration);
            } else {
              if (logger.level <= Logger.FINE) logger.log( "Expiration is "+adc.objects[objDescIndex].refreshedLifetime+" already, no update needed");
            }

            lastResult = new Boolean(true);
            continue;
          } else {
  
            /* If the aggregate is not in the list, check whether it is still in the waiting list */
          
            IdSet waitingIds = waitingList.scan();
            Iterator iter = waitingIds.getIterator();
      
            while (iter.hasNext()) {
              final VersionKey vkey = (VersionKey) iter.next();
              if (vkey.getId().equals(id)) {
                ObjectDescriptor thisObject = (ObjectDescriptor) waitingList.getMetadata(vkey);
                if (logger.level <= Logger.INFO) logger.log( "Refreshing in waiting list: " + vkey.toStringFull());
          
                if (thisObject == null) {
                  if (logger.level <= Logger.WARNING) logger.log("Broken object in waiting list: " + vkey.toStringFull() + ", removing...");
                  final Continuation myParent = this;
                  waitingList.unstore(vkey, new Continuation() {
                    public void receiveResult(Object o) {
                      if (logger.level <= Logger.INFO) logger.log( "Broken object "+vkey.toStringFull()+" removed successfully");
                      myParent.receiveResult(new AggregationException("Object in waiting list, but broken: "+vkey.toStringFull()));
                    }
                    public void receiveException(Exception e) {
                      if (logger.level <= Logger.WARNING) logger.logException("Cannot remove broken object "+vkey.toStringFull()+" from waiting list (exception: "+e+")",e);
                      myParent.receiveResult(new AggregationException("Object broken, in waiting list, and cannot remove: "+vkey.toStringFull()+" (e="+e+")"));
                    }
                  });
                  return;
                }
  
                if (thisObject.refreshedLifetime < expiration) {
                  ObjectDescriptor newDescriptor = new ObjectDescriptor(
                    thisObject.key, thisObject.version, thisObject.currentLifetime, 
                    expiration, thisObject.size
                  );

                  final Continuation myParent = this;
                  waitingList.setMetadata(vkey, newDescriptor, new Continuation() {
                    public void receiveResult(Object o) {
                      if (logger.level <= Logger.FINE) logger.log( "Refreshed metadata written ok for "+vkey.toStringFull());
                      myParent.receiveResult(new Boolean(true));
                    }
                    public void receiveException(Exception e) {
                      if (logger.level <= Logger.WARNING) logger.logException("Cannot refresh waiting object "+vkey.toStringFull()+", e=",e);
                      myParent.receiveResult(new AggregationException("Cannot refresh waiting object "+vkey.toStringFull()+", setMetadata() failed (e="+e+")"));
                    }
                  });
                  return;
                } else {
                  if (logger.level <= Logger.FINE) logger.log( "Object found in waiting list and no update needed: "+vkey.toStringFull());
                  receiveResult(new Boolean(true));
                  return;
                }
              }
            }
          
            /* If the object is neither in the aggregate list nor in the waiting list, there must have
               been a failure, and we need to reaggregate this object. */
    
            objectsMissing ++;
            if (addMissingAfterRefresh) {
              if (objectsFetched < maxReaggregationPerRefresh) {
                objectsFetched ++;
                final Continuation myParent = this;
                objectStore.lookup(id, false, new Continuation() {
                  public void receiveResult(Object o) {
                    if (o instanceof PastContent) {
                      final PastContent obj = (PastContent) o;
                      if (logger.level <= Logger.WARNING) logger.log("Refresh: Found in PAST, but not in aggregate list: "+id.toStringFull());
            
                      long theVersion;
                      if (o instanceof GCPastContent) {
                        theVersion = ((GCPastContent)obj).getVersion();
                      } else {
                        theVersion = 0;
                      } 

                      final VersionKey vkey = new VersionKey(obj.getId(), theVersion);
                      final long theVersionF = theVersion;
                      final int theSize = getSize(obj);

                      if (policy.shouldBeAggregated(obj, theSize)) {
                        if (!waitingList.exists(vkey)) {
                          if (logger.level <= Logger.FINE) logger.log( "ADDING MISSING OBJECT AFTER REFRESH: "+obj.getId());
  
                          waitingList.store(vkey, new ObjectDescriptor(obj.getId(), theVersionF, expiration, expiration, theSize), obj, new Continuation() {
                            public void receiveResult(Object o) {
                              ((PastImpl) objectStore).cache(obj, new Continuation() {
                                public void receiveResult(Object o) {
                                  if (logger.level <= Logger.FINE) logger.log( "Refresh: Missing object "+id.toStringFull()+" added ok");
                                  myParent.receiveResult(new Boolean(true));
                                }
                                public void receiveException(Exception e) {
                                  if (logger.level <= Logger.WARNING) logger.logException("Refresh: Exception while precaching object: "+id.toStringFull()+" (e="+e+")",e);
                                  myParent.receiveResult(new Boolean(true));
                                }
                              });
                            }
                            public void receiveException(Exception e) { 
                              if (logger.level <= Logger.WARNING) logger.logException("Refresh: Exception while refreshing aggregate: "+id.toStringFull()+" (e="+e+")",e);
                              myParent.receiveResult(new AggregationException("Cannot store reaggregated object in waiting list: "+id.toStringFull()));
                            }
                          });

                          return;
                        } else {
                          if (logger.level <= Logger.FINE) logger.log( "Refresh: Missing object already in waiting list: "+id.toStringFull());
                          myParent.receiveResult(new Boolean(true));
                          return;
                        }
                      }
                    
                      if (logger.level <= Logger.FINE) logger.log( "Refresh: Missing object should not be aggregated: "+id.toStringFull());
                      myParent.receiveResult(new Boolean(true));
                      return;
                    } else {
                      if (logger.level <= Logger.WARNING) logger.log("Refresh: Cannot find refreshed object "+id.toStringFull()+", lookup returns "+o);
                      myParent.receiveException(new AggregationException("Object not found during reaggregation: "+id.toStringFull()));
                    }
                  }
                  public void receiveException(Exception e) {
                    if (logger.level <= Logger.WARNING) logger.log("Refresh: Exception received while reaggregating "+id.toStringFull()+", e="+e);
                    myParent.receiveException(e);
                  }
                });
                return;
              } else {
                if (logger.level <= Logger.FINE) logger.log( "Refresh: Limit of "+maxReaggregationPerRefresh+" reaggregations exceeded; postponing id="+id.toStringFull());
                lastResult = new Boolean(true);
                continue;
              }
            } else {
              if (logger.level <= Logger.WARNING) logger.log("Refresh: Refreshed object not found in any aggregate: "+id.toStringFull());
              lastResult = new Boolean(true);
              continue;
            }
          }

          /* This should NEVER be reached */
        }
      }
      public void receiveException(Exception e) {
        if (logger.level <= Logger.WARNING) logger.logException("Exception while refreshing "+ids[currentIndex].toStringFull()+", e=",e);
        receiveResult(e);
      }
    };
    
    theContinuation.receiveResult(null);
  }

  private int getSize(PastContent obj) {
    try {
//      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
//      ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
//
//      objectStream.writeObject(obj);
//      objectStream.flush();
      
//      return byteStream.toByteArray().length;
      RawPastContent rpc;
      if (obj instanceof RawPastContent) {
        rpc = (RawPastContent)obj;
      } else {
        rpc = new JavaSerializedPastContent(obj);
      }
      SimpleOutputBuffer buf = new SimpleOutputBuffer();
      buf.writeShort(rpc.getType());
      rpc.serialize(buf);
      return buf.getWritten();
    } catch (IOException ioe) {
      if (logger.level <= Logger.WARNING) logger.log("Cannot serialize object, size unknown: "+ioe);
    }
    
    return 0;
  }

  public Serializable getHandle() {
    return aggregateList.getRoot();
  }
  
  public void setHandle(Serializable handle, Continuation command) {
    if (logger.level <= Logger.INFO) logger.log( "setHandle("+handle+")");
  
    if (!(handle instanceof Id)) {
      command.receiveException(new AggregationException("Illegal handle"));
      return;
    }
    
    if (aggregateList.getADC((Id) handle) != null) {
      if (logger.level <= Logger.INFO) logger.log( "Rebuild: Handle "+handle+" is already covered by current root");
      command.receiveResult(new Boolean(true));
    }
      
    aggregateList.setRoot((Id) handle);
    rebuildAggregateList(command);
  }

  private void rebuildRecursive(final Id fromKey, final Vector keysInProgress, final Vector keysPostponed, final Vector keysDone, final Continuation command) {
    keysInProgress.add(fromKey);
    
    if (logger.level <= Logger.INFO) logger.log( "Rebuild: Fetching handles for aggregate " + fromKey.toStringFull());
    aggregateStore.lookupHandles(fromKey, 999, new Continuation() {
      public void receiveResult(Object o) {
        if (logger.level <= Logger.FINE) logger.log( "Got handles for "+fromKey);

        if (o instanceof PastContentHandle[]) {
          PastContentHandle[] pch = (PastContentHandle[]) o;
          PastContentHandle bestHandle = null;

          for (int i=0; i<pch.length; i++) {
            if ((pch[i] == null) || ((pch[i] instanceof GCPastContentHandle) && (((GCPastContentHandle)pch[i]).getVersion() != 0)))
              continue;
              
            if (bestHandle == null)
              bestHandle = pch[i];
          }
          
          if (bestHandle != null) {
            final PastContentHandle thisHandle = bestHandle;
            final Continuation outerContinuation = this;

            if (logger.level <= Logger.FINE) logger.log( "Fetching "+thisHandle);
            aggregateStore.fetch(thisHandle, new Continuation() {
              public void receiveResult(Object o) {
                if (o instanceof Aggregate) {
                  keysInProgress.remove(fromKey);
                  keysDone.add(fromKey);

                  if (logger.level <= Logger.INFO) logger.log( "Rebuild: Got aggregate " + fromKey.toStringFull());

                  Aggregate aggr = (Aggregate) o;
                  ObjectDescriptor[] objects = new ObjectDescriptor[aggr.numComponents()];
                  long aggregateExpiration = (thisHandle instanceof GCPastContentHandle) ? ((GCPastContentHandle)thisHandle).getExpiration() : GCPast.INFINITY_EXPIRATION;
          
                  for (int i=0; i<aggr.numComponents(); i++) {
                    objects[i] = new ObjectDescriptor(
                      aggr.getComponent(i).getId(), 
                      aggr.getComponent(i).getVersion(),
                      aggregateExpiration,
                      aggregateExpiration,
                      getSize(aggr.getComponent(i))
                    );
                    
                    final GCPastContent objData = aggr.getComponent(i);
                    if (logger.level <= Logger.FINE) logger.log( "Checking whether "+objData.getId()+"v"+objData.getVersion()+" is in object store...");
                    objectStore.lookupHandles(objData.getId(), 1, new Continuation() {
                      public void receiveResult(Object o) {
/* evil... needs pastcontenthandle */                      
                        PastContentHandle[] result = (o instanceof PastContentHandle[]) ? ((PastContentHandle[])o) : new PastContentHandle[] {};
                        if (logger.level <= Logger.FINE) logger.log( "Handles for "+objData.getId()+"v"+objData.getVersion()+": "+result+" ("+result.length+", PCH="+(o instanceof PastContentHandle[])+")");
                        boolean gotOne = false;
                        for (int i=0; i<result.length; i++) {
                          if (result[i] != null) {
                            if (logger.level <= Logger.FINE) logger.log( "Have v"+((GCPastContentHandle)result[i]).getVersion());
                            if (((GCPastContentHandle)result[i]).getVersion() >= objData.getVersion())
                              gotOne = true;
                          }
                        }
                        
                        if (gotOne) {
                          if (logger.level <= Logger.FINE) logger.log( "Got it");
                        } else {
                          if (logger.level <= Logger.FINE) logger.log( "Ain't got it... reinserting");
                          objectStore.insert(objData, new Continuation() {
                            public void receiveResult(Object o) {
                              if (logger.level <= Logger.FINE) logger.log( "Reinsert "+objData.getId()+"v"+objData.getVersion()+" ok, result="+o);
                            }
                            public void receiveException(Exception e) {
                              if (logger.level <= Logger.FINE) logger.logException("Reinsert "+objData.getId()+"v"+objData.getVersion()+" failed, exception=",e);
                            }
                          });
                        }
                      }
                      public void receiveException(Exception e) {
                        if (logger.level <= Logger.FINE) logger.logException("Cannot retrieve handles for object "+objData.getId()+"v"+objData.getVersion()+" to be restored; e=",e);
                      }
                    });
                  }
            
                  aggregateList.addAggregateDescriptor(new AggregateDescriptor(fromKey, aggregateExpiration, objects, aggr.getPointers()));
          
                  Id[] pointers = aggr.getPointers();
                  int numAdded = 0;
                  if (pointers != null) {
                    for (int i=0; i<pointers.length; i++) {
                      if (pointers[i] instanceof Id) {
                        Id thisPointer = pointers[i];
                        if (!keysDone.contains(thisPointer) && !keysPostponed.contains(thisPointer) && !keysInProgress.contains(thisPointer)) {
                          if (keysInProgress.size() >= reconstructionMaxConcurrentLookups)  
                            keysPostponed.add(thisPointer);
                          else
                            rebuildRecursive(thisPointer, keysInProgress, keysPostponed, keysDone, command);
                            
                          numAdded ++;
                        }
                      }
                    }
                  }
                  
                  if (logger.level <= Logger.FINE) logger.log( "Rebuild: Added "+numAdded+" keys, now "+keysInProgress.size()+" in progress, "+keysPostponed.size()+" postponed and "+keysDone.size()+" done");
          
                  if (!keysInProgress.isEmpty() || !keysPostponed.isEmpty()) {
                    if (logger.level <= Logger.INFO) logger.log( "Rebuild: "+keysInProgress.size()+" keys in progress, "+keysPostponed.size()+" postponed, "+keysDone.size()+" done");
                    
                    while ((keysInProgress.size() < reconstructionMaxConcurrentLookups) && (keysPostponed.size() > 0)) {
                      Id nextKey = (Id) keysPostponed.firstElement();
                      if (logger.level <= Logger.FINE) logger.log( "Rebuild: Resuming lookup for postponed key "+nextKey.toStringFull());
                      keysPostponed.remove(nextKey);
                      rebuildRecursive(nextKey, keysInProgress, keysPostponed, keysDone, command);
                    }
                  } else {
                    aggregateList.writeToDisk();
                    rebuildInProgress = false;
                    if (logger.level <= Logger.INFO) logger.log( "Rebuild: Completed; "+keysDone.size()+" aggregates checked");
                    command.receiveResult(new Boolean(true));
                  }
                } else {
                  receiveException(new AggregationException("Fetch failed: "+fromKey+", returned "+o));
                }
              }
              public void receiveException(Exception e) {
                outerContinuation.receiveException(e);
              }
            });
          } else {
            receiveException(new AggregationException("LookupHandles did not return any valid handles for "+fromKey));
          }
        } else {
          receiveException(new AggregationException("LookupHandles for "+fromKey+" failed, returned o="+o));
        }
      }
      public void receiveException(Exception e) {
        if (logger.level <= Logger.WARNING) logger.logException("Rebuild: Exception ",e);
        keysInProgress.remove(fromKey);
        keysDone.add(fromKey);
        
        if (keysInProgress.isEmpty() && keysPostponed.isEmpty()) {
          rebuildInProgress = false;
          if (aggregateList.isEmpty()) {
            command.receiveException(new AggregationException("Cannot read root aggregate! -- retry later"));
          } else {
            aggregateList.writeToDisk();
            command.receiveResult(new Boolean(true));
          }
        }
        
        while ((keysInProgress.size() < reconstructionMaxConcurrentLookups) && (keysPostponed.size() > 0)) {
          Id nextKey = (Id) keysPostponed.firstElement();
          if (logger.level <= Logger.FINE) logger.log( "Rebuild: Resuming lookup for postponed key "+nextKey.toStringFull());
          keysPostponed.remove(nextKey);
          rebuildRecursive(nextKey, keysInProgress, keysPostponed, keysDone, command);
        }
      }
    });
  }

  private void rebuildAggregateList(final Continuation command) {
    Vector keysInProgress = new Vector();
    Vector keysPostponed = new Vector();
    Vector keysDone = new Vector();

    if (logger.level <= Logger.INFO) logger.log( "rebuildAggregateList("+aggregateList.getRoot()+")");
    if (aggregateList.getRoot() == null) {
      if (logger.level <= Logger.WARNING) logger.log("rebuildAggregateList invoked while rootKey is null");
      command.receiveException(new AggregationException("Set handle first!"));
      return;
    }
    
    rebuildInProgress = true;
    rebuildRecursive(aggregateList.getRoot(), keysInProgress, keysPostponed, keysDone, command);
  }
  
  public void insert(final PastContent obj, final Continuation command) {
    insert(obj, INFINITY_EXPIRATION, command);
  }

  public void insert(final PastContent obj, final long lifetime, final Continuation command) {

    long theVersion;
    if (obj instanceof GCPastContent) {
      theVersion = ((GCPastContent)obj).getVersion();
    } else {
      theVersion = 0;
    }

    final VersionKey vkey = new VersionKey(obj.getId(), theVersion);
    final long theVersionF = theVersion;
    final int theSize = getSize(obj);

    if (policy.shouldBeAggregated(obj, theSize)) {
      if (logger.level <= Logger.INFO) logger.log( "AGGREGATE INSERT: "+obj.getId()+" version="+theVersion+" size="+theSize+" class="+obj.getClass().getName());

      if (objectStore instanceof GCPast)
        ((GCPast)objectStore).insert(obj, lifetime, command);
      else 
        objectStore.insert(obj, command);
        
      waitingList.store(vkey, new ObjectDescriptor(obj.getId(), theVersionF, lifetime, lifetime, theSize), obj, new Continuation() {
        public void receiveResult(Object o) {
        }
        public void receiveException(Exception e) { 
          if (logger.level <= Logger.WARNING) logger.logException("Exception while storing aggregate: "+obj.getId()+" (e="+e+")",e);
        }
      });
    } else {
      if (logger.level <= Logger.INFO) logger.log( "INSERT WITHOUT AGGREGATION: "+obj.getId()+" version="+theVersionF+" size="+theSize+" class="+obj.getClass().getName());
      
      Continuation c = new Continuation() {
        boolean otherSucceeded = false;
        boolean otherFailed = false;

        public void receiveResult(Object o) {
          if (logger.level <= Logger.FINE) logger.log( "INSERT "+obj.getId()+" receiveResult("+o+"), otherSucc="+otherSucceeded+" otherFail="+otherFailed);
          if (otherSucceeded) {
            if (!otherFailed) {
              if (logger.level <= Logger.FINE) logger.log( "--reporting Success");
              command.receiveResult(new Boolean[] { new Boolean(true) });
            }
          } else {
            otherSucceeded = true;
          }
        }
        public void receiveException(Exception e) {
          if (logger.level <= Logger.FINE) logger.log( "INSERT "+obj.getId()+" receiveException("+e+"), otherSucc="+otherSucceeded+" otherFail="+otherFailed);
          if (logger.level <= Logger.FINE) logger.log( "--reporting Failure");
          command.receiveException(e);
          otherFailed = true;
        }
      };
      
      if (objectStore instanceof GCPast) 
        ((GCPast)objectStore).insert(obj, lifetime, c);
      else 
        objectStore.insert(obj, c);
        
      if (aggregateStore instanceof GCPast)
        ((GCPast)aggregateStore).insert(new NonAggregate(obj), lifetime, c);
      else
        aggregateStore.insert(new NonAggregate(obj), c);
    }
  }
  
  private void retrieveObjectFromAggregate(final AggregateDescriptor adc, final int objDescIndex, final Continuation command) {
    aggregateStore.lookup(adc.key, new Continuation() {
      public void receiveResult(Object o) {
        if (o instanceof Aggregate) {
          final Aggregate aggr = (Aggregate) o;
          endpoint.process(new Executable() {
            public Object execute() {
              return factory.buildId(aggr.getContentHash());
            }
          }, new Continuation() {
            public void receiveResult(Object o) {
              if (o instanceof Id) {
                Id aggrNominalKey = (Id) o;

                if (!aggrNominalKey.equals(adc.key)) {
                  if (logger.level <= Logger.WARNING) logger.log("Cannot validate aggregate "+adc.key+", hash="+aggrNominalKey);
                  command.receiveException(new AggregationException("Cannot validate aggregate -- retry?"));
                  return;
                }
            
                if (logger.level <= Logger.FINE) logger.log( "Object "+adc.objects[objDescIndex].key+" (#"+objDescIndex+") successfully retrieved from "+adc.key);

                objectStore.insert(aggr.getComponent(objDescIndex), new Continuation() {
                  public void receiveResult(Object o) {}
                  public void receiveException(Exception e) {}
                });

                command.receiveResult(aggr.getComponent(objDescIndex));
              } else {
                if (logger.level <= Logger.WARNING) logger.log("retrieveObjectFromAggregate cannot determine content hash, received "+o);
                command.receiveException(new AggregationException("retrieveObjectFromAggregate cannot determine content hash"));
              }
            }
            public void receiveException(Exception e) {
              if (logger.level <= Logger.WARNING) logger.logException("retrieveObjectFromAggregate cannot determine content hash, exception ",e);
              command.receiveException(e);
            }
          });
        } else {
          if (logger.level <= Logger.WARNING) logger.log("retrieveObjectFromAggregate failed; receiveResult("+o+")");
          command.receiveResult(null);
        }
      }
      public void receiveException(Exception e) {
        if (logger.level <= Logger.WARNING) logger.logException("retrieveObjectFromAggregate failed; receiveException("+e+")",e);
        command.receiveException(e);
      }
    });
  }
  
  public void lookup(final Id id, boolean cache, final Continuation command) {
    if (logger.level <= Logger.INFO) logger.log( "lookup("+id+", cache="+cache+")");
    
    objectStore.lookup(id, cache, new Continuation() {
      public void receiveResult(Object o) {
        if (o != null) {
          if (logger.level <= Logger.FINE) logger.log( "NL: Found in PAST: "+id);
          command.receiveResult(o);
        } else {
          AggregateDescriptor adc = (AggregateDescriptor) aggregateList.getADC(id);
          if (adc!=null) {
            if (logger.level <= Logger.FINE) logger.log( "NL: Must retrieve from aggregate");

            int objDescIndex = adc.lookupNewest(id);
            if (objDescIndex < 0) {
              if (logger.level <= Logger.WARNING) logger.log("NL: Aggregate found, but object not found in aggregate?!? -- aborted");
              command.receiveException(new AggregationException("Inconsistency detected in aggregate list -- try restarting the application"));
              return;
            }
        
            retrieveObjectFromAggregate(adc, objDescIndex, command);

          } else {
            if (logger.level <= Logger.WARNING) logger.log("NL: LOOKUP FAILED, OBJECT NOT FOUND: "+id);
            command.receiveResult(null);
          }
        }
      }
      public void receiveException(Exception e) {
        command.receiveException(e);
      }
    });
  }

  public void lookup(final Id id, final long version, final Continuation command) {
    if (logger.level <= Logger.INFO) logger.log( "lookup("+id+", version="+version+")");

    AggregateDescriptor adc = (AggregateDescriptor) aggregateList.getADC(new VersionKey(id, version));
    if (adc!=null) {
      if (logger.level <= Logger.FINE) logger.log( "VL: Retrieving from aggregate");

      int objDescIndex = adc.lookupSpecific(id, version);
      if (objDescIndex < 0) {
        if (logger.level <= Logger.WARNING) logger.log("VL: Aggregate found, but object not found in aggregate?!? -- aborted");
        command.receiveException(new AggregationException("Inconsistency detected in aggregate list -- try restarting the application"));
        return;
      }
        
      retrieveObjectFromAggregate(adc, objDescIndex, command);
    } else {
      if (logger.level <= Logger.FINE) logger.log( "VL: Not found in aggregate list: "+id+"v"+version);
      if (aggregateStore instanceof VersioningPast) {
        VersioningPast vaggr = (VersioningPast) aggregateStore;
        vaggr.lookup(id, version, new Continuation() {
          public void receiveResult(Object o) {
            if (o != null) {
              if (logger.level <= Logger.FINE) logger.log( "VL: Found in Aggregate.VersioningPAST: "+id+"v"+version);
              command.receiveResult(o);
            } else {
              if (logger.level <= Logger.FINE) logger.log( "VL: Not found in Aggregate.VersioningPAST: "+id+"v"+version);
              if (objectStore instanceof VersioningPast) {
                VersioningPast vpast = (VersioningPast) objectStore;
                vpast.lookup(id, version, new Continuation() {
                  public void receiveResult(Object o) {
                    if (o != null) {
                      if (logger.level <= Logger.FINE) logger.log( "VL: Found in Object.VersioningPAST: "+id+"v"+version);
                      command.receiveResult(o);
                    } else {
                      if (logger.level <= Logger.WARNING) logger.log("VL: LOOKUP FAILED, OBJECT NOT FOUND: "+id+"v"+version);
                      command.receiveResult(null);
                    }
                  }
                  public void receiveException(Exception e) {
                    command.receiveException(e);
                  }
                });
              } else {
                if (logger.level <= Logger.FINE) logger.log( "VL: Object store does not support versioning");
                command.receiveException(new AggregationException("Cannot find "+id+"v"+version+" -- try rebuilding aggregate list?"));
              }   
            }
          }
          public void receiveException(Exception e) {
            if (logger.level <= Logger.WARNING) logger.logException("Aggregate.VersioningPAST returned exception for "+id+"v"+version+": ",e);
            command.receiveException(new AggregationException("Aggregate.VersioningPAST returned exception for "+id+"v"+version+": "+e));
          }
        });
      } else {
        if (logger.level <= Logger.FINE) logger.log( "VL: Aggregate store does not support versioning");
        if (objectStore instanceof VersioningPast) {
          VersioningPast vpast = (VersioningPast) objectStore;
          vpast.lookup(id, version, new Continuation() {
            public void receiveResult(Object o) {
              if (o != null) {
                if (logger.level <= Logger.FINE) logger.log( "VL: Found in Object.VersioningPAST: "+id+"v"+version);
                command.receiveResult(o);
              } else {
                if (logger.level <= Logger.WARNING) logger.log("VL: LOOKUP FAILED, OBJECT NOT FOUND: "+id+"v"+version);
                command.receiveResult(null);
              }
            }
            public void receiveException(Exception e) {
              command.receiveException(e);
            }
          });
        }

        if (logger.level <= Logger.FINE) logger.log( "VL: Object store does not support versioning");
        command.receiveResult(null);
      }
    }
  }
  
  public void lookup(Id id, Continuation command) {
    lookup(id, true, command);
  }
  
  public void lookupHandles(final Id id, final long version, final int max, final Continuation command) {
    ((VersioningPast)aggregateStore).lookupHandles(id, version, max, command);
  }
  
  public void lookupHandle(final Id id, final NodeHandle handle, final Continuation command) {
    command.receiveException(new UnsupportedOperationException("LookupHandle() is not supported on Aggregation"));
  }
  
  public void lookupHandles(final Id id, final int max, final Continuation command) {
    if (logger.level <= Logger.INFO) logger.log( "lookupHandles("+id+","+max+")");
    objectStore.lookupHandles(id, max, new Continuation() {
      public void receiveResult(Object o) {
        PastContentHandle[] result = (o instanceof PastContentHandle[]) ? ((PastContentHandle[])o) : new PastContentHandle[] {};
        boolean foundHandle = false;
        
        for (int i=0; i<result.length; i++)
          if (result[i] != null)
            foundHandle = true;

        if (foundHandle) {
          if (logger.level <= Logger.FINE) logger.log( "lookupHandles("+id+","+max+") handled by PAST; ret="+o);
          command.receiveResult(o);
        } else {
          if (logger.level <= Logger.INFO) logger.log( "lookupHandles("+id+","+max+") failed, ret="+o);

          AggregateDescriptor adc = (AggregateDescriptor) aggregateList.getADC(id);
          if (adc!=null) {
            if (logger.level <= Logger.FINE) logger.log( "lookupHandles: Retrieving from aggregate");

            int objDescIndex = adc.lookupNewest(id);
            if (objDescIndex < 0) {
              if (logger.level <= Logger.WARNING) logger.log("lookupHandles: Aggregate found, but object not found in aggregate?!? -- aborted");
              command.receiveException(new AggregationException("Inconsistency detected in aggregate list -- try restarting the application"));
              return;
            }
          
            if (adc.objects[objDescIndex].refreshedLifetime < environment.getTimeSource().currentTimeMillis()) {
              if (logger.level <= Logger.FINE) logger.log( "Object "+id+" exists, but has expired -- ignoring");
              command.receiveResult(new PastContentHandle[] { null });
              return;
            }
        
            retrieveObjectFromAggregate(adc, objDescIndex, new Continuation() {
              public void receiveResult(Object o) {
                if (logger.level <= Logger.FINE) logger.log( "lookupHandles: Retrieved from aggregate: "+id+", result="+o);
                /* re-inserted implicitly by retrieveObjectFromAggregate */
                objectStore.lookupHandles(id, max, command);
              }
              public void receiveException(Exception e) {
                if (logger.level <= Logger.WARNING) logger.log("lookupHandles: Cannot retrieve from aggregate, exception "+e);
                command.receiveException(e);
              }
            });
          } else {
            if (logger.level <= Logger.INFO) logger.log( "lookupHandles: "+id+" is neither in object store nor in aggregate list");
            /* Note that we have to give up here... even if the object has not been 
               aggregated, there is no efficient way to find out its version number.
               The user must call lookupVersion in this case. */
            command.receiveResult(new PastContentHandle[] { null });
          }
        }
      }
      public void receiveException(Exception e) {
        if (logger.level <= Logger.WARNING) logger.log("Exception in lookupHandles: "+e);
        command.receiveException(e);
      }
    });
  }
  
  public void fetch(PastContentHandle handle, Continuation command) {

    /* Note that we never give out any handles from the aggregate store, so this must
       be an object store handle */
    if (handle instanceof GlacierContentHandle)
      aggregateStore.fetch(handle, command);
    else
      objectStore.fetch(handle, command);
  }

  public void flush(Id id, Continuation command) {
    Iterator iter = waitingList.scan().getIterator();
    boolean objectIsWaiting = false;

    if (logger.level <= Logger.INFO) logger.log( "flush("+id+") invoked");
    
    while (iter.hasNext()) {
      VersionKey thisKey = (VersionKey) iter.next();
      if (thisKey.getId().equals(id)) {
        objectIsWaiting = true;
        break;
      }
    }
    
    if (objectIsWaiting)
      formAggregates(command);
    else
      command.receiveResult(new Boolean(true));
  }
  
  public void flush(final Continuation command) {
    formAggregates(command);
  }
  
  public void rollback(Id id, Continuation command) {
    AggregateDescriptor adc = (AggregateDescriptor) aggregateList.getADC(id);

    if (adc!=null) {
      int objDescIndex = adc.lookupNewest(id);
      if (objDescIndex < 0) {
        if (logger.level <= Logger.WARNING) logger.log("Rollback: Aggregate found, but object not found in aggregate?!? -- aborted");
        command.receiveException(new AggregationException("Inconsistency detected in aggregate list -- try restarting the application"));
        return;
      }
      
      if (logger.level <= Logger.FINE) logger.log( "Rollback: Found "+adc.objects[objDescIndex].key+"v"+adc.objects[objDescIndex].version);
      retrieveObjectFromAggregate(adc, objDescIndex, command);
    }

    if (logger.level <= Logger.FINE) logger.log( "Rollback: No version of "+id+" found");    
    command.receiveResult(null);
  }
  
  public void reset(Continuation command) {
    aggregateList.clear();

    Iterator iter = waitingList.scan().getIterator();
    while (iter.hasNext()) {
      VersionKey thisKey = (VersionKey) iter.next();
      waitingList.unstore(thisKey, new Continuation() {
        public void receiveResult(Object o) {}
        public void receiveException(Exception e) {}
      });
    }

    command.receiveResult(new Boolean(true));
  }

  public NodeHandle getLocalNodeHandle() {
    return objectStore.getLocalNodeHandle();
  }
  
  public int getReplicationFactor() {
    return objectStore.getReplicationFactor();
  }

  public boolean forward(RouteMessage message) {
    return true;
  }
  
  public void update(NodeHandle handle, boolean joined) {
  }

  public void deliver(Id id, Message message) {

    final AggregationMessage msg = (AggregationMessage) message;
    if (logger.level <= Logger.FINE) logger.log( "Received message " + msg + " with destination " + id + " from " + msg.getSource().getId());

    if (msg instanceof AggregationTimeoutMessage) {
    
      /* TimeoutMessages are generated by the local node when a 
         timeout expires. */
    
      AggregationTimeoutMessage gtm = (AggregationTimeoutMessage) msg;
      timerExpired((char) gtm.getUID());
      return;
    } else {
      panic("AGGREGATION ERROR - Received message " + msg + " of unknown type.");
    }
  }

  public void setFlushInterval(int flushIntervalSec) {
    flushInterval = flushIntervalSec * SECONDS;
  }
  
  public void setMaxAggregateSize(int maxAggregateSize) {
    this.maxAggregateSize = maxAggregateSize;
  }

  public void setMaxObjectsInAggregate(int maxObjectsInAggregate) {
    this.maxObjectsInAggregate = maxObjectsInAggregate;
  }

  public void setRenewThreshold(int expirationRenewThresholdHrs) {
    this.expirationRenewThreshold = expirationRenewThresholdHrs * HOURS;
  }
  
  public void setConsolidationInterval(long consolidationIntervalSec) {
    this.consolidationInterval = consolidationIntervalSec * SECONDS;
  }
  
  public void setConsolidationThreshold(long consolidationThresholdSec) {
    this.consolidationThreshold = consolidationThresholdSec * SECONDS;
  }
  
  public void setConsolidationMinObjectsPerAggregate(int minObjectsInAggregateArg) {
    this.consolidationMinObjectsInAggregate = minObjectsInAggregateArg;
  }
  
  public void setConsolidationMinUtilization(double minUtilization) {
    this.consolidationMinComponentsAlive = minUtilization;
  }

  public Past getAggregateStore() {
    return aggregateStore;
  }
  
  public Past getObjectStore() {
    return objectStore;
  }
  
  public int getNumObjectsWaiting() {
    return waitingList.scan().numElements();
  }
  
  public AggregationStatistics getStatistics() {
    return stats;
  }

  public String getInstance() {
    return instance;
  }
  
  public Environment getEnvironment() {
    return environment;
  }

  public void setContentDeserializer(PastContentDeserializer deserializer) {    
    contentDeserializer = deserializer;
    objectStore.setContentDeserializer(contentDeserializer);
  }

  public void setContentHandleDeserializer(PastContentHandleDeserializer deserializer) {
    contentHandleDeserializer = deserializer;
    objectStore.setContentHandleDeserializer(deserializer);
  }
}
