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
package org.mpisws.p2p.transport.sourceroute;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;

import rice.environment.logging.Logger;

public class Forwarder<Identifier> {
  SourceRoute sr;
  P2PSocket<Identifier> socka;
  P2PSocket<Identifier> sockb;
  Logger logger;
  
  public Forwarder(SourceRoute<Identifier> sr, P2PSocket<Identifier> socka, P2PSocket<Identifier> sockb, Logger logger) {
    this.sr = sr;
    this.socka = socka;
    this.sockb = sockb;
    this.logger = logger;
    
    new HalfPipe(socka,sockb);
    new HalfPipe(sockb,socka);
  }
    
  private class HalfPipe implements P2PSocketReceiver {
    P2PSocket from;
    P2PSocket to;
    ByteBuffer buf;
    boolean shutdownTo = false;
    
    public HalfPipe(P2PSocket from, P2PSocket to) {
      this.from = from;
      this.to = to;    
      buf = ByteBuffer.allocate(1024);
      from.register(true, false, this);
    }
    
    public String toString() {
      return "HalfPipe "+from+"=>"+to;    
    }

    public void receiveException(P2PSocket socket, Exception e) {
      if (logger.level <= Logger.FINE) logger.logException(this+" "+socket, e);
      from.close();
      to.close();
    }

    public void receiveSelectResult(P2PSocket socket, boolean canRead, boolean canWrite) throws IOException {
      if (canRead) {
        if (socket != from) throw new IOException("Expected to read from "+from+" got "+socket);      
        long result = from.read(buf);
        if (result == -1) {
          if (logger.level <= Logger.FINE) logger.log(from+" has shut down input, shutting down output on "+to);
          shutdownTo = true;
          return;
        }
        if (logger.level <= Logger.FINER) logger.log("Read "+result+" bytes from "+from);
        buf.flip(); 
        to.register(false, true, this);        
      } else {
        if (canWrite) {
          if (socket != to) throw new IOException("Expected to write to "+to+" got "+socket);      
          
          long result = to.write(buf);         
          if (result == -1) {
            if (logger.level <= Logger.FINE) logger.log(to+" has closed, closing "+from);
            from.close();            
          }
          if (logger.level <= Logger.FINER) logger.log("Wrote "+result+" bytes to "+to);
          
          
          if (buf.hasRemaining()) {
            // keep writing
            to.register(false, true, this);
          } else {
            if (shutdownTo) {
              to.shutdownOutput();
              return;
            }
            // read again
            buf.clear();
            from.register(true, false, this);
          }
        } else {
          throw new IOException("Didn't select for either "+socket+","+canRead+","+canWrite); 
        }
      }
    }
  }
}
