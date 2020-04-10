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
 * Created on Aug 9, 2005
 */
package rice.environment.processing.simple;

import rice.*;
import rice.environment.Environment;
import rice.environment.logging.LogManager;
import rice.environment.processing.*;
import rice.environment.time.TimeSource;
import rice.p2p.commonapi.Cancellable;
import rice.selector.SelectorManager;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.Queue;

/**
 * @author Jeff Hoye
 */
public class SimpleProcessor implements Processor {
  // the queue used for processing requests
  protected PriorityBlockingQueue<ProcessingRequest> QUEUE;
  protected ProcessingThread THREAD;

  // for blocking IO WorkRequests
  protected WorkQueue workQueue;
  protected BlockingIOThread bioThread;
  
  long seq = Long.MIN_VALUE;

  public SimpleProcessor(String name) {
    QUEUE = new PriorityBlockingQueue<ProcessingRequest>();
    THREAD = new ProcessingThread(name + ".ProcessingThread", QUEUE);
    THREAD.start();
    THREAD.setPriority(Thread.MIN_PRIORITY);
    workQueue = new WorkQueue();
    bioThread = new BlockingIOThread(workQueue);
    bioThread.start();
  }

  /**
   * Schedules a job for processing on the dedicated processing thread. CPU
   * intensive jobs, such as encryption, erasure encoding, or bloom filter
   * creation should never be done in the context of the underlying node's
   * thread, and should only be done via this method.
   * 
   * @param task
   *          The task to run on the processing thread
   * @param command
   *          The command to return the result to once it's done
   */
  public <R, E extends Exception> Cancellable process(Executable<R,E> task, Continuation<R, E> command,
      SelectorManager selector, TimeSource ts, LogManager log) {
    return process(task, command, 0, selector, ts, log);
  }

  public <R, E extends Exception> Cancellable process(Executable<R,E> task, Continuation<R, E> command, int priority,
      SelectorManager selector, TimeSource ts, LogManager log) {
    long nextSeq;
    synchronized(SimpleProcessor.this) {
      nextSeq = seq++;
    }
    ProcessingRequest ret = new ProcessingRequest(task, command, priority, nextSeq, log, ts, selector);
    QUEUE.offer(ret);
    return ret;
  }

  public Cancellable processBlockingIO(WorkRequest workRequest) {
    workQueue.enqueue(workRequest);
    return workRequest;
  }

  public Queue<ProcessingRequest> getQueue() {
    return QUEUE;
  }

  public void destroy() {
    THREAD.destroy();
    QUEUE.clear();
    bioThread.destroy();
    workQueue.destroy();
  }

  public WorkQueue getIOQueue() {
    return workQueue;
  }
  
  /**
   * This is a test to make sure the order is correct.
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    Environment env = new Environment();
    Processor p = env.getProcessor();
    // block the processor for 1 second while we schedule some more stuff
    p.process(new Executable() {    
      public Object execute() {
        try { Thread.sleep(1000); } catch (InterruptedException ie) {}
        return null;
      }
    
    }, new Continuation() {
    
      public void receiveResult(Object result) {
        System.out.println("Done blocking.");
      }
    
      public void receiveException(Exception exception) {
        exception.printStackTrace();
      }
    
    }, env.getSelectorManager(), env.getTimeSource(), env.getLogManager());
    
    for (int seq = 0; seq < 10; seq++) {
      final int mySeq = seq;
      p.process(new Executable() {    
        public Object execute() {
          System.out.println("Executed Seq: "+mySeq);
          return null;
        }
      
      }, new Continuation() {      
        public void receiveResult(Object result) {
          System.out.println("Received Seq: "+mySeq);
        }
      
        public void receiveException(Exception exception) {
          exception.printStackTrace();
        }
      
      }, env.getSelectorManager(), env.getTimeSource(), env.getLogManager());      
      System.out.println("Done scheduling "+mySeq);    
    }
  }
}
