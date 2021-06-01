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

import java.util.Map;

import org.mpisws.p2p.transport.SocketRequestHandle;

import rice.environment.logging.Logger;
import rice.p2p.commonapi.Cancellable;

public class SocketRequestHandleImpl<Identifier> implements SocketRequestHandle<Identifier> {
  Identifier identifier;
  Map<String, Object> options;
  Cancellable subCancellable;
  Logger logger;
//  protected boolean cancelled = false;
  
  public SocketRequestHandleImpl(Identifier i, Map<String, Object> options, Logger logger) {
    this.identifier = i;
    this.options = options;
    if (logger == null) throw new IllegalArgumentException("logger is null");
    this.logger = logger;
  }

  public Identifier getIdentifier() {
    return identifier;
  }

  public Map<String, Object> getOptions() {
    return options;
  }

  public boolean cancel() {
//    if (cancelled) return true;
//    cancelled = true;
    if (subCancellable != null) {
//      try {
        return subCancellable.cancel();
//      } catch (IllegalStateException ise) {
//        if (logger.level <= Logger.WARNING) logger.log(ise.toString());
//        throw ise;
//      }
    }
    return false;
  }

  public void setSubCancellable(Cancellable sub) {
//    if (sub == null) throw new IllegalArgumentException("sub must be non-null");
//    if (cancelled) sub.cancel();
    this.subCancellable = sub;
  }

  public Cancellable getSubCancellable() {
    return subCancellable;
  }

  @Override
  public String toString() {
    return "SRHi{"+identifier+","+options+"}";
  }
}
