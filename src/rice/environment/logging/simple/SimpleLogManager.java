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
package rice.environment.logging.simple;

import java.io.PrintStream;
import java.util.Hashtable;

import rice.environment.logging.*;
import rice.environment.logging.LogManager;
import rice.environment.params.ParameterChangeListener;
import rice.environment.params.Parameters;
import rice.environment.time.TimeSource;
import rice.environment.time.simple.SimpleTimeSource;

/**
 * This class creates loggers that log to a specified PrintStream System.out by default.
 * 
 * @author Jeff Hoye
 */
public class SimpleLogManager extends AbstractLogManager implements CloneableLogManager {

  /**
   * Constructor.
   * 
   * @param stream the stream to write to
   * @param timeSource the timesource to get times from
   * @param minPriority the minimum priority to print
   */
  public SimpleLogManager(PrintStream stream, TimeSource timeSource, Parameters params) {
    this(stream, timeSource, params, "", null);  
  }
  
  public SimpleLogManager(PrintStream stream, TimeSource timeSource, Parameters params, String prefix, String dateFormat) {
    super(stream, timeSource, params, prefix, dateFormat);
  }
  
  public PrintStream getPrintStream() {
    return ps;
  }
  
  public Parameters getParameters() {
    return params;
  }
  
  public TimeSource getTimeSource() {
    return time;
  }
  
  
  /**
   * Convienience constructor.
   * 
   * Defauts to System.out as the stream, and SimpleTimeSource as the timesource.
   * 
   * @param minPriority the minimum priority to print.
   */  
  public SimpleLogManager(Parameters params) {
    this(null, new SimpleTimeSource(), params);
  }
  
  /**
   * Convienience constructor.
   * 
   * Defauts to SimpleTimeSource as the timesource.
   * 
   * @param stream the stream to write to
   * @param minPriority the minimum priority to print
   */
  public SimpleLogManager(PrintStream stream, Parameters params) {
    this(stream, new SimpleTimeSource(), params);
  }
  
  /**
   * Convienience constructor.
   * 
   * Defauts to System.out as the stream.
   * 
   * @param timeSource the timesource to get times from
   * @param minPriority the minimum priority to print
   */
  public SimpleLogManager(TimeSource timeSource, Parameters params) {
    this(null, timeSource, params);
  }
  
  protected Logger constructLogger(String clazz, int level, boolean useDefault) {
    return new SimpleLogger(clazz,this,level, useDefault);
  }

  /* (non-Javadoc)
   * @see rice.environment.logging.CloneableLogManager#clone(java.lang.String)
   */
  public LogManager clone(String detail) {
    return clone(detail,time);
  }
  public LogManager clone(String detail, TimeSource ts) {
    return new SimpleLogManager(ps, ts, params, detail, dateFormat);
  }
  
  @Override
  public String toString() {
    return "SimpleLogManager("+prefix+")";
  }
}
