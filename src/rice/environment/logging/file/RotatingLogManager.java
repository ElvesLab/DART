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
package rice.environment.logging.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.text.*;
import java.util.Date;

import javax.swing.text.DateFormatter;

import rice.environment.Environment;
import rice.environment.logging.AbstractLogManager;
import rice.environment.logging.LogManager;
import rice.environment.logging.Logger;
import rice.environment.logging.simple.SimpleLogManager;
import rice.environment.logging.simple.SimpleLogger;
import rice.environment.params.Parameters;
import rice.environment.time.TimeSource;
import rice.p2p.commonapi.CancellableTask;
import rice.selector.SelectorManager;
import rice.selector.TimerTask;

/**
 * The file name is 
 * 
 * @author jstewart
 * 
 */
public class RotatingLogManager extends AbstractLogManager {

  protected TimerTask rotateTask;
  protected TimerTask sizeRotateTask;

  public RotatingLogManager(TimeSource timeSource, Parameters params) {
    this(timeSource, params, "", null);
  }

  /**
   * @param timeSource
   * @param params
   * @param prefix appears in the file, not the file-name
   */
  public RotatingLogManager(TimeSource timeSource, Parameters params,
      String prefix, String dateFormat) {
    super(AbstractLogManager.nullPrintStream, timeSource, params, prefix, dateFormat);
    rotate();
  }

  public void startRotateTask(SelectorManager sm) {
    if (rotateTask == null) {
      rotateTask = new LogRotationTask();
      sm.getTimer().schedule(rotateTask, params.getInt("log_rotate_interval"),
          params.getInt("log_rotate_interval"));
      if (params.contains("log_rotate_size_check_interval") && sizeRotateTask == null) {
        sizeRotateTask = new LogSizeRotationTask();
        sm.getTimer().schedule(sizeRotateTask, 
            params.getInt("log_rotate_size_check_interval"), 
            params.getInt("log_rotate_size_check_interval"));
      }
    } else {
      throw new RuntimeException("Task already started");
    }
  }

  public void cancelRotateTask() {
    rotateTask.cancel();
    rotateTask = null;
  }

  void rotate() {
    synchronized (this) {
      PrintStream oldps = ps;
      String dateFormat = params.getString("log_rotating_date_format");
      DateFormatter dateFormatter = null;
      if (dateFormat != null && !dateFormat.equals("")) {
        dateFormatter = new DateFormatter(new SimpleDateFormat(
            dateFormat));
      }

      System.out.println("rotate: about to rotate log");
      
      String filename = params.getString("log_rotate_filename");
      File oldfile = new File(filename);
      if (oldfile.exists()) {
        long filedate = oldfile.lastModified();
        String rot_filename = filename + "." + filedate;
        if (dateFormatter != null) {
          try {
            rot_filename = filename + "." + dateFormatter.valueToString(new Date(filedate));         
          } catch (ParseException pe) {
            pe.printStackTrace();
          }
        }
        System.out.println("rotate: renaming "+filename+" to "+rot_filename);
        // have to close before we rename for Windows 
        if (oldps != null)
          oldps.close();
        boolean result = oldfile.renameTo(new File(rot_filename));
      }
      try {
        ps = new PrintStream(new FileOutputStream(oldfile, true), true);
      } catch (FileNotFoundException e) {
        // this won't happen
        System.err.println("could not rotate log " + filename + " because of "
            + e);
        // XXX should also log it
      }
      System.out.println("rotate: starting new log");
    }
  }

  private class LogRotationTask extends TimerTask {
    public void run() {
      rotate();
    }
  }

  private class LogSizeRotationTask extends TimerTask {
    public void run() {
      synchronized (RotatingLogManager.this) {
        if (new File(params.getString("log_rotate_filename")).length() >= params.getLong("log_rotate_max_size"))
          rotate();
      }
    }
  }
  
  public PrintStream getPrintStream() {
    synchronized (this) {
      if (enabled) {
          return ps;
      } else {
        return nullPrintStream;
      }
    }
  }

  protected Logger constructLogger(String clazz, int level, boolean useDefault) {
    return new SimpleLogger(clazz, this, level, useDefault);
  }
}
