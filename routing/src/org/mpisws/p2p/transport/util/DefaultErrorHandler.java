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
package org.mpisws.p2p.transport.util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

import org.mpisws.p2p.transport.ErrorHandler;

import rice.environment.logging.Logger;

/**
 * Just logs the problems.
 * 
 * @author Jeff Hoye
 *
 * @param <Identifier>
 * @param <E>
 */
public class DefaultErrorHandler<Identifier> implements
    ErrorHandler<Identifier> {
  public int NUM_BYTES_TO_PRINT = 8;
  protected int printlevel;
  private Logger logger;
  
  public DefaultErrorHandler(Logger logger) {
    this(logger, Logger.INFO);
  }
  
  public DefaultErrorHandler(Logger logger, int printlevel) {
    if (logger == null) throw new IllegalArgumentException("logger is null");
    this.logger = logger;
    this.printlevel = printlevel;
  }
  
  public void receivedUnexpectedData(Identifier id, byte[] bytes, int pos, Map<String, Object> options) {
    if (logger.level <= Logger.INFO) {
      // make this pretty
      String s = "";
      int numBytes = NUM_BYTES_TO_PRINT;
      if (bytes.length < numBytes) numBytes = bytes.length;
      for (int i = 0; i < numBytes; i++) {
        s+=bytes[i]+","; 
      }
      logger.log("Unexpected data from "+id+" "+s);
    }
  }

  public void receivedException(Identifier i, Throwable error) {
    if (logger.level <= printlevel) {      
      logger.logException(i == null ? null : i.toString(), error);
//      logger.logException("here I am", new Exception("ErrorHandlerCall"));
    }
  }

}
