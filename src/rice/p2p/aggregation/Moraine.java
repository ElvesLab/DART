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
package rice.p2p.aggregation;

import java.util.Arrays;

import rice.Continuation;
import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.glacier.VersioningPast;
import rice.p2p.past.Past;
import rice.p2p.past.PastContent;
import rice.p2p.past.PastContentHandle;
import rice.p2p.past.gc.GCPast;
import rice.p2p.past.rawserialization.PastContentDeserializer;
import rice.p2p.past.rawserialization.PastContentHandleDeserializer;

/**
 * This Past takes 2 pasts, an old Past and a new
 * Past.  It treats the old Past as a backing store for the new Past.
 *
 * Pretty much it's only going to work with glacier, and maybe aggregation
 * 
 * A Moraine is the hill of rubble (aggregate as it were) left behind at
 * the edges of a glacier or at the end of a retreating glacier
 * 
 * @author jstewart
 *
 */
@SuppressWarnings("unchecked")
public class Moraine implements GCPast, VersioningPast {
  protected GCPast newPast;
  protected GCPast oldPast;
  // convenience variables to prevent casting everywhere
  // identical to the above vars, just different types
  protected VersioningPast vNewPast;
  protected VersioningPast vOldPast;
  
  protected Logger logger;

  public Moraine(GCPast newPast, GCPast oldPast) {
    this.newPast = newPast;
    this.oldPast = oldPast;
    this.vNewPast = (VersioningPast)newPast;
    this.vOldPast = (VersioningPast)oldPast;
    this.logger = newPast.getEnvironment().getLogManager().getLogger(Moraine.class, newPast.getInstance());
  }
  
  // --------------------------------------------------------------------------------
  // Past methods

  public void insert(PastContent obj, Continuation command) {
    newPast.insert(obj,command);
  }

  public void lookup(Id id, Continuation command) {
    // assume caching
    lookup(id, true, command);
  }

  public void lookup(final Id id, final boolean cache, final Continuation command) {
    newPast.lookup(id, cache, new Continuation() {
      public void receiveResult(Object result) {
        if (result == null) {
          oldPast.lookup(id, cache, new Continuation() {
            public void receiveResult(Object result) {
              // XXX store the result in newPast
              command.receiveResult(result);
            }

            public void receiveException(Exception result) {
              command.receiveException(result);
            }
          });
        } else {
          command.receiveResult(result);
        }
      }

      public void receiveException(Exception result) {
        // XXX do we try the other Past?
        command.receiveException(result);
      }
    });
  }

  public void lookupHandles(final Id id, final int max, final Continuation command) {
    newPast.lookupHandles(id, max, new Continuation() {
      public void receiveResult(Object result) {
        Object[] results = (Object[])result;
        if (results.length == 1 && results[0] == null) {
          oldPast.lookupHandles(id, max, command);
        } else {
          command.receiveResult(result);
        }
      }

      public void receiveException(Exception result) {
        if (logger.level <= Logger.WARNING) {
          logger.logException("in Moraine.lookupHandles, newPast threw up: ",result);
        }
        oldPast.lookupHandles(id, max, command);
      }
    });
  }

  // this is unsupported by Glacier and Aggregation anyway
  public void lookupHandle(Id id, NodeHandle handle, Continuation command) {
    command.receiveException(new UnsupportedOperationException("LookupHandle() is not supported on Moraine"));
  }

  public void fetch(final PastContentHandle handle, final Continuation command) {
    newPast.fetch(handle, new Continuation() {
      public void receiveResult(Object result) {
        if (result == null) {
          // XXX store the result of the fetch in the newPast
          oldPast.fetch(handle, command);
        } else {
          command.receiveResult(result);
        }
      }

      public void receiveException(Exception result) {
        // XXX do we try the other Past?
        command.receiveException(result);
      }
    });
  }

  public NodeHandle getLocalNodeHandle() {
    return newPast.getLocalNodeHandle();
  }

  public int getReplicationFactor() {
    return newPast.getReplicationFactor();
  }

  public Environment getEnvironment() {
    return newPast.getEnvironment();
  }

  public String getInstance() {
    return newPast.getInstance();
  }

  public void setContentDeserializer(PastContentDeserializer deserializer) {
    newPast.setContentDeserializer(deserializer);
    oldPast.setContentDeserializer(deserializer);
    // XXX maybe force this on the members and just throw an UnsupportedOperationException
  }

  public void setContentHandleDeserializer(
      PastContentHandleDeserializer deserializer) {
    newPast.setContentHandleDeserializer(deserializer);
    oldPast.setContentHandleDeserializer(deserializer);
    // XXX maybe force this on the members and just throw an UnsupportedOperationException
  }

  // --------------------------------------------------------------------------------
  // GCPast methods
  
  public void insert(PastContent obj, long expiration, Continuation command) {
    newPast.insert(obj, expiration, command);
  }

  public void refresh(final Id[] ids, final long[] expirations, final Continuation command) {
    oldPast.refresh(ids, expirations, new Continuation() {
      public void receiveResult(Object result) {
        newPast.refresh(ids, expirations, command);
      }

      public void receiveException(Exception result) {
        if (logger.level <= Logger.WARNING) {
          logger.logException("in Moraine.refresh, oldPast threw up: ",result);
        }
        receiveResult(null);
      }
    }); 
  }

  public void refresh(Id[] ids, long expiration, Continuation command) {
    long[] expirations = new long[ids.length];
    Arrays.fill(expirations, expiration);
    refresh(ids, expirations, command);
  }

  // --------------------------------------------------------------------------------
  // VersioningPast methods
  
  public void lookup(final Id id, final long version, final Continuation command) {
    vNewPast.lookup(id, version, new Continuation() {
      public void receiveResult(Object result) {
        if (result == null) {
          vOldPast.lookup(id, version, new Continuation() {
            public void receiveResult(Object result) {
              // XXX store the result in newPast
              command.receiveResult(result);
            }

            public void receiveException(Exception result) {
              command.receiveException(result);
            }
          });
        } else {
          command.receiveResult(result);
        }
      }

      public void receiveException(Exception result) {
        // XXX do we try the other Past?
        command.receiveException(result);
      }
    });
  }

  public void lookupHandles(final Id id, final long version, final int num, final Continuation command) {
    vNewPast.lookupHandles(id, version, num, new Continuation() {
      public void receiveResult(Object result) {
        Object[] results = (Object[])result;
        if (results.length == 1 && results[0] == null) {
          vOldPast.lookupHandles(id, version, num, command);
        } else {
          command.receiveResult(result);
        }
      }

      public void receiveException(Exception result) {
        if (logger.level <= Logger.WARNING) {
          logger.logException("in Moraine.lookupHandles, newPast threw up: ",result);
        }
        vOldPast.lookupHandles(id, version, num, command);
      }
    });
  }

  public void refresh(final Id[] ids, final long[] versions, final long[] expirations, final Continuation command) {
    vOldPast.refresh(ids, versions, expirations, new Continuation() {
      public void receiveResult(Object result) {
        vNewPast.refresh(ids, versions, expirations, command);
      }

      public void receiveException(Exception result) {
        if (logger.level <= Logger.WARNING) {
          logger.logException("in Moraine.refresh, oldPast threw up: ",result);
        }
        receiveResult(null);
      }
    }); 
  }

}
