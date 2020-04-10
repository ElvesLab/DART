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
 * Created on Apr 6, 2005
 */
package rice.environment.logging;

/**
 * The Logger is a simplified interface of the java.util.logging.Logger.  It is 
 * envisioned that one could implement this interface using java.util.logging, but
 * that many times this interface is overkill. 
 * 
 * @author Jeff Hoye
 */
public abstract class Logger {

  // These are suggested base level priorities.
  
  /**
   * SEVERE is a message level indicating a serious failure.
   */
  public static final int SEVERE = 1000; 
  
  /**
   * WARNING is a message level indicating a potential problem.
   */
  public static final int WARNING = 900;
  
  /**
   * INFO is a message level for informational messages.
   * Things that only happen once per node creation.
   */
  public static final int INFO = 800;
  
  /**
   * CONFIG is a message level for static configuration messages.
   */
  public static final int CONFIG = 700;
  
  /**
   * FINE is a message level providing tracing information.
   * Things that get logged once per specific message.
   */
  public static final int FINE = 500;
  
  /**
   * FINER indicates a fairly detailed tracing message. 
   * Things that get logged once per general message.
   */
  public static final int FINER = 400;
  
  /**
   * FINEST indicates a highly detailed tracing message. 
   * Things that happen more than once per general message.
   */
  public static final int FINEST = 300;

  /**
   * ALL indicates that all messages should be logged.
   */
  public static final int ALL = Integer.MIN_VALUE;
  
  /**
   * OFF is a special level that can be used to turn off logging. 
   */
  public static final int OFF = Integer.MAX_VALUE;

  /**
   * This is public for performance reasons.
   */
  public int level = 0;
    
  /**
   * Prints the message if the priority is equal to or higher than the minimum priority.
   * @param priority the priority of this log message
   * @param message the message to print
   */
  public abstract void log(String message);

  /**
   * Prints the stack trace of the exception.  If you only want to print the 
   * exception's string, use the log() method.
   * 
   * This is necessary because Exception doesn't have a convienient way of printing the stack trace as a string.
   * 
   * @param priority the priority of this log message
   * @param exception the exception to print
   */
  public abstract void logException(String message, Throwable exception);
  
  /**
   * Prints the message if the priority is equal to or higher than the minimum priority.
   * 
   * Deprecated because string construction performance was the bottleneck
   * 
   * @param priority the priority of this log message
   * @param message the message to print
   * @deprecated use if (logger.level <= priority) log(String);
   */
  public void log(int priority, String message) {
    if (level <= priority) log(message); 
  }

  /**
   * Prints the stack trace of the exception.  If you only want to print the 
   * exception's string, use the log() method.
   * 
   * This is necessary because Exception doesn't have a convienient way of printing the stack trace as a string.
   * 
   * Deprecated because string construction performance was the bottleneck
   * 
   * @param priority the priority of this log message
   * @param exception the exception to print
   * @deprecated use if (logger.level <= priority) logException(String, Throwable);
   */
  public void logException(int priority, String message, Throwable exception) {
    if (level <= priority) logException(message, exception);     
  }

}
