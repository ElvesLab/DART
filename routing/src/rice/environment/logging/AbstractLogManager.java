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
 * Created on Jun 28, 2005
 *
 */
package rice.environment.logging;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

import javax.swing.text.DateFormatter;

import rice.environment.logging.simple.SimpleLogger;
import rice.environment.params.ParameterChangeListener;
import rice.environment.params.Parameters;
import rice.environment.time.TimeSource;

/**
 * @author jstewart
 *
 */
public abstract class AbstractLogManager implements LogManager {
  /**
   * Hashtable of loggers stored by full.class.name[instance]
   */
  protected Hashtable<String, Logger> loggers;
  protected Parameters params;
  
  protected TimeSource time;
  protected PrintStream ps;
  protected String prefix;
  protected String dateFormat;
  
  /**
   * the "default" log level
   */
  int globalLogLevel;

  /**
   * If we only want package level granularity.
   */
  protected boolean packageOnly = true;
  
  protected boolean enabled;
  protected static final PrintStream nullPrintStream = new PrintStream(new NullOutputStream());

  public DateFormatter dateFormatter;
  
  public static final String SYSTEM_OUT = "System.out";
  public static final String SYSTEM_ERR = "System.err";
  
  protected AbstractLogManager(PrintStream stream, TimeSource timeSource, Parameters params, String prefix, String df) {
    this.ps = stream;
    if (ps == null) {
      ps = System.out;
      if (params.contains("logging_output_stream")) {
        String loggingType = params.getString("logging_output_stream");
        if (loggingType.equals(SYSTEM_OUT)) {
          ps = System.out; 
        } else if (loggingType.equals(SYSTEM_ERR)) {
          ps = System.err; 
        } else {
          try {
            ps = new PrintStream(new FileOutputStream(loggingType, true)); 
          } catch (FileNotFoundException fnfe) {
            throw new RuntimeException(fnfe); 
          }
        }
      }        
    }
    this.time = timeSource;
    this.params = params;
    this.prefix = prefix;
    this.dateFormat = df;
    if (this.dateFormat == null) {
      this.dateFormat = params.getString("logging_date_format");
    }
    if (this.dateFormat != null && !this.dateFormat.equals("")) {      
      dateFormatter = new DateFormatter(new SimpleDateFormat(this.dateFormat));
//      System.out.println("DateFormat "+this.dateFormat);
    }

    this.enabled = params.getBoolean("logging_enable");
    if (params.contains("logging_packageOnly")) {
      this.packageOnly = params.getBoolean("logging_packageOnly");
    }

    this.loggers = new Hashtable<String, Logger>();
    this.globalLogLevel = parseVal("loglevel");

    params.addChangeListener(new ParameterChangeListener() {
      public void parameterChange(String paramName, String newVal) {
        if (paramName.equals("logging_enable")) {
            enabled = Boolean.valueOf(newVal).booleanValue();
        } else if (paramName.equals("loglevel")) {            
            synchronized(this) {
              // iterate over all loggers, if they are default loggers,
              // set the level
              globalLogLevel = parseVal(paramName);
              Iterator<Logger> i = loggers.values().iterator();
              while(i.hasNext()) {
                HeirarchyLogger hl = (HeirarchyLogger)i.next();
                if (hl.useDefault) {
                  hl.level = globalLogLevel; 
                }
              }
            } // synchronized
        } else if (paramName.endsWith("_loglevel")) {
            String loggerName = paramName.substring(0,paramName.length()-"_loglevel".length());
            if ((newVal == null) || (newVal.equals(""))) {
              // parameter "removed" 
              // a) set the logger to use defaultlevel, 
              // b) set the level 
              Iterator<String> i = loggers.keySet().iterator();
              while(i.hasNext()) {
                String name = (String)i.next();
                if (name.startsWith(loggerName)) {
//              if (loggers.containsKey(loggerName)) { // perhaps we haven't even created such a logger yet
                  HeirarchyLogger hl = (HeirarchyLogger)loggers.get(name);
                  hl.useDefault = true;
                  hl.level = globalLogLevel;
                }
              }
            } else {
              // a) set the logger to not use the defaultlevel, 
              // b) set the level 
              Iterator<String> i = loggers.keySet().iterator();
              while(i.hasNext()) {
                String name = (String)i.next();
                if (name.startsWith(loggerName)) {
//              if (loggers.containsKey(loggerName)) { // perhaps we haven't even created such a logger yet
                  HeirarchyLogger hl = (HeirarchyLogger)loggers.get(name);
                  hl.useDefault = false;
                  hl.level = parseVal(paramName);
                } 
              }
          }
        }
      }
    });
  }

  protected int parseVal(String key) {
    try {
      return params.getInt(key);
    } catch (NumberFormatException nfe) {
      String val = params.getString(key);

      if (val.equalsIgnoreCase("ALL")) return Logger.ALL;
      if (val.equalsIgnoreCase("OFF")) return Logger.OFF;
      if (val.equalsIgnoreCase("SEVERE")) return Logger.SEVERE; 
      if (val.equalsIgnoreCase("WARNING")) return Logger.WARNING; 
      if (val.equalsIgnoreCase("INFO")) return Logger.INFO; 
      if (val.equalsIgnoreCase("CONFIG")) return Logger.CONFIG; 
      if (val.equalsIgnoreCase("FINE")) return Logger.FINE; 
      if (val.equalsIgnoreCase("FINER")) return Logger.FINER; 
      if (val.equalsIgnoreCase("FINEST")) return Logger.FINEST; 
      throw new InvalidLogLevelException(key,val);
    }
  }
  
  /**
   * 
   */
  @SuppressWarnings("unchecked")
  public Logger getLogger(Class clazz, String instance) {
    // first we want to get the logger name
    String loggerName;
    String className = clazz.getName();
    String[] parts = null;
    if (packageOnly) {
      // builds loggername = just the package
      parts = className.split("\\.");
      loggerName = parts[0];
      // the "-1" cuts off the class part of the full package name
      for (int curPart = 1; curPart < parts.length-1; curPart++) {
        loggerName+="."+parts[curPart];   
      }            
    } else {
      // loggerName is the className
      loggerName = className;
    }
    
    if (instance != null) {
      loggerName = loggerName+"@"+instance;
    }
    
    // see if this logger exists
    if (loggers.containsKey(loggerName)) {
      return (Logger)loggers.get(loggerName);
    }
    
    // OPTIMIZATION: parts is only built if needed, and it may have been needed earlier, or it may not have been
    if (parts == null) parts = className.split("\\.");
    
    // at this point we know we are going to have to build a logger.  We need to
    // figure out what level to initiate it with.
    
    
    
    int level = globalLogLevel;
    boolean useDefault = true;    
    
    String baseStr;
    
    // Ex: if clazz.getName() == rice.pastry.socket.PingManager, try:
    // 1) rice.pastry.socket.PingManager
    // 2) rice.pastry.socket
    // 3) rice.pastry
    // 4) rice
    
    int lastPart = parts.length;
    
    // this strips off the ClassName from the package
    if (packageOnly) lastPart--;
    
    // numParts is how much of the prefix we want to use, start with the full name    
    for (int numParts = lastPart; numParts >= 0; numParts--) {     
      
      // build baseStr which is the prefix of the clazz up to numParts
      baseStr = parts[0];
      for (int curPart = 1; curPart < numParts; curPart++) {
        baseStr+="."+parts[curPart];   
      }
      
      
      // try to find one matching a specific instance
      if (instance != null) {            
        String searchString = baseStr+"@"+instance+"_loglevel";
        // see if this logger should exist
        if (params.contains(searchString)) {
          level = parseVal(searchString);
          useDefault = false;
          break;
        }
      }
      
      String searchString = baseStr+"_loglevel";
      if (params.contains(searchString)) {
        level = parseVal(searchString);
        useDefault = false;
        break;
      }
    }
    
    
    // at this point, we didn't find anything that matched, so now return a logger
    // that has the established level
    Logger logger = constructLogger(loggerName, level, useDefault);     
    loggers.put(loggerName, logger);
    return logger;
  }

  protected abstract Logger constructLogger(String clazz, int level, boolean useDefault);

  public TimeSource getTimeSource() {
    return time;
  }

  public PrintStream getPrintStream() {
    if (enabled) {
      return ps;
    } else {
      return nullPrintStream;
    }
  }
  
  public String getPrefix() {
    return prefix;
  }
  
  private static class NullOutputStream extends OutputStream {
    public void write(int arg0) throws IOException {
      // do nothing
    }
    public void write(byte[] buf) throws IOException {
      // do nothing
    }
    public void write(byte[] buf, int a, int b) throws IOException {
      // do nothing
    }
    public void close() {
      // do nothing
    }
  }
}
