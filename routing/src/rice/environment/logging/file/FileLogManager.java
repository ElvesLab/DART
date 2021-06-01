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
 * Created on Jul 7, 2005
 */
package rice.environment.logging.file;

import java.io.*;
import java.io.PrintStream;

import rice.environment.logging.LogManager;
import rice.environment.logging.simple.SimpleLogManager;
import rice.environment.params.Parameters;
import rice.environment.time.TimeSource;
import rice.environment.time.simple.SimpleTimeSource;

/**
 * @author Jeff Hoye
 */
public class FileLogManager extends SimpleLogManager {
  String filePrefix;
  String fileSuffix;
  
  public FileLogManager(PrintStream stream, TimeSource timeSource, Parameters params) {
    this(stream, timeSource, params, "");  
  }
  
  public FileLogManager(PrintStream stream, TimeSource timeSource, Parameters params, String prefix) {
    this(stream, timeSource, params, prefix,
        params.getString("fileLogManager_filePrefix"),
        params.getString("fileLogManager_fileSuffix"),
        null);
  }
  
  public FileLogManager(PrintStream stream, TimeSource timeSource, Parameters params, String prefix, String filePrefix, String fileSuffix, String dateFormat) {
    super(stream, timeSource, params, prefix, dateFormat);
    this.filePrefix = filePrefix;
    this.fileSuffix = fileSuffix;
  }
  
  /**
   * Convienience constructor.
   * 
   * Defauts to System.out as the stream, and SimpleTimeSource as the timesource.
   * 
   * @param minPriority the minimum priority to print.
   */  
  public FileLogManager(Parameters params) {
    this(getDefaultPrintStream(params), new SimpleTimeSource(), params);
  }
  
  /**
   * Convienience constructor.
   * 
   * Defauts to SimpleTimeSource as the timesource.
   * 
   * @param stream the stream to write to
   * @param minPriority the minimum priority to print
   */
  public FileLogManager(PrintStream stream, Parameters params) {
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
  public FileLogManager(TimeSource timeSource, Parameters params) {
    this(getDefaultPrintStream(params), timeSource, params);
  }
  
  private static PrintStream getDefaultPrintStream(Parameters params) {
    // TODO Auto-generated method stub
    return getPrintStream(
        params.getString("fileLogManager_filePrefix"),
        params.getString("fileLogManager_defaultFileName"),
        params.getString("fileLogManager_fileSuffix"), 
        !params.getBoolean("fileLogManager_overwrite_existing_log_file"));
  }
  
  private static PrintStream getPrintStream(String filePrefix, String detail, String fileSuffix, boolean append) {
    PrintStream newPS = System.out;
    try {
      String fname = filePrefix+detail+fileSuffix;
      newPS = new PrintStream(new FileOutputStream(fname,append));
    } catch (IOException ioe) {
      throw new RuntimeException(ioe); 
//      ioe.printStackTrace(newPS);
    }
    return newPS;
  }
  
  public LogManager clone(String detail) {
    PrintStream newPS = this.ps;
    boolean append = !params.getBoolean("fileLogManager_overwrite_existing_log_file");
    if (params.getBoolean("fileLogManager_multipleFiles")) {
      newPS = getPrintStream(filePrefix, detail, fileSuffix, append);
    }
    String linePrefix = "";
    if (params.getBoolean("fileLogManager_keepLinePrefix")) {
      linePrefix = detail; 
    }
    return new FileLogManager(newPS, time, params, linePrefix, filePrefix, fileSuffix, dateFormat);
  }  
}
