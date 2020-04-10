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

package rice.p2p.scribe.maintenance;

import java.util.Collection;
import java.util.List;

import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.scribe.BaseScribe;
import rice.p2p.scribe.Scribe;
import rice.p2p.scribe.ScribeClient;
import rice.p2p.scribe.ScribeContent;
import rice.p2p.scribe.ScribeMultiClient;
import rice.p2p.scribe.Topic;
import rice.p2p.scribe.rawserialization.RawScribeContent;

/**
 * This is an interface to scribe so that the MaintenacePolicy 
 * can have additional access to Scribe, that most users will not need.
 * 
 * @author Jeff Hoye
 *
 */
public interface MaintainableScribe extends BaseScribe {
  public static final int MAINTENANCE_ID = Integer.MAX_VALUE;
  
  public Collection<Topic> getTopics();
  public Endpoint getEndpoint();
  
  /**
   * This returns the topics for which the parameter 'parent' is a Scribe tree
   * parent of the local node
   * 
   * @param parent null/localHandle for topics rooted by us
   */
  public Collection<Topic> getTopicsByParent(NodeHandle parent);

  /**
   * This returns the topics for which the parameter 'child' is a Scribe tree
   * child of the local node
   */
  public Collection<Topic> getTopicsByChild(NodeHandle child);
  
  public void subscribe(Collection<Topic> nodeWasParent, ScribeMultiClient client, RawScribeContent content, NodeHandle hint);

  public void setParent(Topic topic, NodeHandle parent, List<Id> pathToRoot);
  
  public List<Id> getPathToRoot(Topic topic);

}
