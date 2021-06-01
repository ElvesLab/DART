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
package rice.environment.logging;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

import rice.environment.Environment;

/**
 * This class constructs an output stream that will send its output to a logger, line
 * by line.  This could for example be wrapped in a PrintStream to capture stdout
 * or stderr to the log.  As so:
 * System.setOut(new PrintStream(new LogOutputStream(environment, Logger.INFO, "out"), true));
 * System.setErr(new PrintStream(new LogOutputStream(environment, Logger.INFO, "err"), true));
 * 
 * @author jstewart
 *
 */
public class LogOutputStream extends OutputStream {

  // the underlying logger to output to
  protected Logger logger;
  
  // buffer where we build up output
  protected byte[] buffer;
  
  // position where the next write into the buffer would go.
  protected int offset;
  
  // log level to output as
  protected int level;
  
  // size of the output buffer, in bytes
  public static final int BUFFER_SIZE = 1024;
  
  /**
   * Constructs a LogOutputStream
   * 
   * @param env - the environment to log to
   * @param level - the log level of this OutputStream's messages
   */
  public LogOutputStream(Environment env, int level) {
    this(env,level,"");
  }

  /**
   * Constructs a LogOutputStream
   * 
   * @param env - the environment to log to
   * @param level - the log level of this OutputStream's messages
   * @param instance - an instance name string for disambiguation
   */
  public LogOutputStream(Environment env, int level, String instance) {
    logger = env.getLogManager().getLogger(LogOutputStream.class, instance);
    buffer = new byte[BUFFER_SIZE];
    offset = 0;
    this.level = level;
  }

  public void write(int b) throws IOException {
    if (b == '\n') {
      if ((offset > 0) && (buffer[offset-1] == '\r'))
        offset--;
      flush();
      return;
    }
    if ((offset > 0) && buffer[offset-1]=='\r') {
      // treat bare \r as a newline
      offset--;
      flush();
    }
    // I don't actually know why it needs to be - 1 but it works
    if (offset == buffer.length-1)
      flush();
    // okay, so we're not unicode friendly. Cry me a river.
    buffer[offset++] = (byte)(b & 0xff);
  }
  
  public void flush() {
    if (offset == 0)
      return;
    if (logger.level <= level) logger.log(new String(buffer, 0, offset));
    offset = 0;
  }
  
  public void close() {
    flush();
  }

}
