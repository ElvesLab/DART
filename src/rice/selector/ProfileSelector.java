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
 * Created on Jul 27, 2004
 *
 * To change the template for this generated file go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package rice.selector;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;

import rice.environment.logging.*;
import rice.environment.random.RandomSource;
import rice.environment.random.simple.SimpleRandomSource;
import rice.environment.time.TimeSource;

/**
 * @author jeffh
 *
 * To change the template for this generated type comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
public class ProfileSelector extends SelectorManager {
  public static boolean useHeartbeat = true;
  int HEART_BEAT_INTERVAL = 60000;
  long lastHeartBeat = 0;

  public static boolean recordStats = true;

  public String lastTaskType = null;
  public String lastTaskClass = null;
  public String lastTaskToString = null;
  public long lastTaskHash = 0;

  int numInvocationsScheduled = 0;
  int numInvocationsExecuted = 0;
  
  public ProfileSelector(String instance,
      TimeSource timeSource, LogManager log) {    
    this(instance,timeSource,log,null);
  }

  public ProfileSelector(String instance, TimeSource timeSource, LogManager log, RandomSource rand) {
    this(instance, timeSource,log, null, 60000);
  }
  
  /**
   * 
   */
  public ProfileSelector(String instance, TimeSource timeSource, LogManager log, RandomSource rand, final int lastTaskTime) {
    super(instance, timeSource, log, (rand == null)?new SimpleRandomSource(log):rand);
    new Thread(new Runnable() {
      public void run() {
        while(true) {
          System.out.println("LastTask: type:"+lastTaskType+" class:"+lastTaskClass+" toString():"+lastTaskToString+" hash:"+lastTaskHash);
          try {
            Thread.sleep(lastTaskTime);
          } catch (InterruptedException ie) {
          }
        }
      }
    }, "ProfileSelectorWatchdog").start();

  }

//  int numLoops = 0;
  protected void onLoop() {
//    numLoops++;
//    if (numLoops % 100 == 0) System.out.println("Selector loops:"+numLoops);
    if (!useHeartbeat) return;  
    long curTime = timeSource.currentTimeMillis();
    if ((curTime - lastHeartBeat) > HEART_BEAT_INTERVAL) {
      System.out.println("selector heartbeat "+new Date()+" maxInvokes:"+maxInvokes+" invokesSched:"+numInvocationsScheduled+" invokesExe:"+numInvocationsExecuted+" CurrentThread:"+Thread.currentThread()+"@"+System.identityHashCode(Thread.currentThread()));
      printStats();
      lastHeartBeat = curTime;          
    }
  }

  int maxInvokes = 0;
  public void invoke(Runnable d) {
    synchronized(this) {
      numInvocationsScheduled++;
      super.invoke(d);
    }
    //System.out.println("ProfileSelector.invoke("+d.getClass().getName()+"@"+System.identityHashCode(d)+")");
//    if (!(d instanceof ConnectionManager.SenderInvokee)) {
//      //Thread.dumpStack();
//    }
    int numInvokes = invocations.size();
    if (numInvokes > maxInvokes) {
      maxInvokes = numInvokes;
    }
  }

  // *********************** debugging statistics ****************
  /**
   * Records how long it takes to receive each type of message.
   */
  private Hashtable<String, Stat> stats = new Hashtable<String, Stat>();
  
  public void addStat(String s, long time) {
    if (!recordStats) return;
    Stat st = (Stat)stats.get(s);
    if (st == null) { 
      st = new Stat(s);
      stats.put(s,st);
    }
    st.addTime(time);
  }

  public void printStats() {
    if (!recordStats) return;

    ArrayList<Stat> list = new ArrayList<Stat>(stats.size());
    if (stats != null) {
      synchronized(stats) {
        Iterator<Stat> e = stats.values().iterator();
        while(e.hasNext()) {
          Stat s = (Stat)e.next(); 
          list.add(s);
//          System.out.println("  "+s);
        }
      }
    }
    
    Collections.sort(list,new Comparator<Stat>() {
      public boolean equals(Object arg0) {
        return false;
      }

      public int compare(Stat arg0, Stat arg1) {
        Stat stat1 = (Stat)arg0;
        Stat stat2 = (Stat)arg1;
        
        return (int)(stat2.totalTime-stat1.totalTime);
      }
    });
    Iterator<Stat> i = list.iterator();
    while(i.hasNext()) {
      System.out.println("  "+i.next()); 
    }
  }

  protected boolean executeTask(TimerTask next) {
    lastTaskType = "Executing "+next;
    long startTime = timeSource.currentTimeMillis();
    boolean ret = super.executeTask(next);
    int time = (int)(timeSource.currentTimeMillis() - startTime);
    lastTaskType = "Executing "+next+" Complete";
    addStat("executing "+next.getClass(),time);   
    return ret;
  }

  protected void doSelections() throws IOException {
    SelectionKey[] keys = selectedKeys();

    for (int i = 0; i < keys.length; i++) {
      selector.selectedKeys().remove(keys[i]);

      SelectionKeyHandler skh = (SelectionKeyHandler) keys[i].attachment();

      if (skh != null) {
        // accept
        if (keys[i].isValid() && keys[i].isAcceptable()) {
          lastTaskType = "Accept";
          lastTaskClass = skh.getClass().getName();
          lastTaskToString = skh.toString();
          lastTaskHash = System.identityHashCode(skh);
          long startTime = timeSource.currentTimeMillis();
          skh.accept(keys[i]);
          int time = (int)(timeSource.currentTimeMillis() - startTime);
          lastTaskType = "Accept Complete";
          addStat("accepting",time);   
        }

        // connect
        if (keys[i].isValid() && keys[i].isConnectable()) {
          lastTaskType = "Connect";
          lastTaskClass = skh.getClass().getName();
          lastTaskToString = skh.toString();
          lastTaskHash = System.identityHashCode(skh);
          long startTime = timeSource.currentTimeMillis();
          skh.connect(keys[i]);
          int time = (int)(timeSource.currentTimeMillis() - startTime);
          lastTaskType = "Connect Complete";
          addStat("connecting",time);   
        }

        // read
        if (keys[i].isValid() && keys[i].isReadable()) {
          lastTaskType = "Read";
          lastTaskClass = skh.getClass().getName();
          lastTaskToString = skh.toString();
          lastTaskHash = System.identityHashCode(skh);
          long startTime = timeSource.currentTimeMillis();
          skh.read(keys[i]);
          int time = (int)(timeSource.currentTimeMillis() - startTime);
          lastTaskType = "Read Complete";
//          if (skh instanceof PingManager) {
//            addStat("readingUDP",time);   
//          } else {
//            addStat("readingTCP",time);               
//          }
          //addStat("reading",time);               
        }

        // write
        if (keys[i].isValid() && keys[i].isWritable()) {
          lastTaskType = "Write";
          lastTaskClass = skh.getClass().getName();
          lastTaskToString = skh.toString();
          lastTaskHash = System.identityHashCode(skh);
          long startTime = timeSource.currentTimeMillis();
          skh.write(keys[i]);
          int time = (int)(timeSource.currentTimeMillis() - startTime);
          lastTaskType = "Write Complete";
//          if (skh instanceof PingManager) {
//            addStat("writingUDP",time);   
//          } else {
//            addStat("writingTCP",time);               
//          }
//          addStat("writing",time);   
        }
      } else {
        keys[i].channel().close();
        keys[i].cancel();
      }
    }
  }


  /**
   * Method which invokes all pending invocations. This method should *only* be
   * called by the selector thread.
   */
  protected void doInvocations() {    
    Iterator<Runnable> i;
    synchronized(this) {
      i = new ArrayList<Runnable>(invocations).iterator();
      invocations.clear();
    }
    Runnable run;
    while (i.hasNext()) {
      numInvocationsExecuted++;
      run = (Runnable)i.next();
      //System.out.println("ProfileSelector.doInvocations()"+run.getClass().getName()+"@"+System.identityHashCode(run));
      try {
        lastTaskType = "Invocation";
        lastTaskClass = run.getClass().getName();
        lastTaskToString = run.toString();
        lastTaskHash = System.identityHashCode(run);
        long startTime = timeSource.currentTimeMillis();
        run.run();
        int time = (int)(timeSource.currentTimeMillis() - startTime);
//        if (run instanceof ConnectionManager.SenderInvokee) {
//          ConnectionManager.SenderInvokee si = (ConnectionManager.SenderInvokee)run;
//          addStat("sending "+si.message.getClass().getName(), time);  
//        } else {
//          addStat(run.getClass().getName(),time);        
//        }
        lastTaskType = "Invocation Complete";
      } catch (Exception e) {
        if (logger.level <= Logger.SEVERE) logger.logException(
            "Invoking runnable caused exception " + e + " - continuing",e);
      }
    }

    Iterator<SelectionKey> i2;
    synchronized(this) {
      i2 = new ArrayList<SelectionKey>(modifyKeys).iterator();
    }
    SelectionKey key;
    while (i2.hasNext()) {
      key = (SelectionKey)i2.next();
      if (key.isValid() && (key.attachment() != null)) {
        SelectionKeyHandler skh = (SelectionKeyHandler) key.attachment();
        lastTaskType = "ModifyKey";
        lastTaskClass = skh.getClass().getName();
        lastTaskHash = System.identityHashCode(skh);
        lastTaskToString = skh.toString();        
        skh.modifyKey(key);
        lastTaskType = "ModifyKey Complete";
      }
    }
  }

  protected void doInvocations2() {
    Runnable run = getInvocation();

    while (run != null) {
      try {
        lastTaskType = "Invocation";
        lastTaskClass = run.getClass().getName();
        lastTaskToString = run.toString();
        lastTaskHash = System.identityHashCode(run);
        long startTime = timeSource.currentTimeMillis();
        run.run();
        int time = (int)(timeSource.currentTimeMillis() - startTime);
        addStat(run.getClass().getName(),time);        
        lastTaskType = "Invocation Complete";
      } catch (Exception e) {
        if (logger.level <= Logger.SEVERE) logger.logException(
            "Invoking runnable caused exception " + e + " - continuing",e);
      }
      
      run = getInvocation();
    }

    SelectionKey key = getModifyKey();
    while (key != null) {
      if (key.isValid() && (key.attachment() != null)) {
        SelectionKeyHandler skh = (SelectionKeyHandler) key.attachment();
        lastTaskType = "ModifyKey";
        lastTaskClass = skh.getClass().getName();
        lastTaskHash = System.identityHashCode(skh);
        lastTaskToString = skh.toString();        
        skh.modifyKey(key);
        lastTaskType = "ModifyKey Complete";
      }

      key = getModifyKey();
    }
  }

  
  
  /**
   * A statistic as to how long user code is taking to process a paritcular message.
   * 
   * @author Jeff Hoye
   */
  class Stat {
    int num = 0;
    String name = null;
    long totalTime = 0;
    long maxTime = 0;
    
    public Stat(String name) {
      this.name = name;
    }
    
    public void addTime(long t) {
      num++;
      totalTime+=t;
      if (t > maxTime) {
        maxTime = t;  
      }
    }
    
    public String toString() {
      long avgTime = totalTime/num;
      return name+"\t maxTime:"+maxTime+"\t avgTime:"+avgTime+"\t numInstances:"+num+"\t totalTime:"+totalTime;
    }
  }




}
