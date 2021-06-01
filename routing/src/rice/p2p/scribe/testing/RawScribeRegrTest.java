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

package rice.p2p.scribe.testing;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.environment.params.simple.SimpleParameters;
import rice.environment.time.simulated.DirectTimeSource;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.commonapi.testing.CommonAPITest;
import rice.p2p.scribe.*;
import rice.p2p.scribe.messaging.SubscribeMessage;
import rice.p2p.scribe.rawserialization.*;

/**
 * @(#) DistScribeRegrTest.java Provides regression testing for the Scribe service using distributed
 * nodes.
 *
 * @version $Id: ScribeRegrTest.java 3157 2006-03-19 12:16:58Z jeffh $
 * @author Alan Mislove
 */

public class RawScribeRegrTest extends ScribeRegrTest {

  public RawScribeRegrTest(Environment env) throws IOException {
    super(env);
  }
  
  public TestScribeContent buildTestScribeContent(Topic topic, int num) {
    return new RawTestScribeContent(topic, num);
  }

  protected void processNode(int num, Node node) {
    super.processNode(num, node);    
    scribes[num].setContentDeserializer(new ScribeContentDeserializer() {
      
      public ScribeContent deserializeScribeContent(InputBuffer buf, Endpoint endpoint,
          short contentType) throws IOException {
        switch(contentType) {
          case RawTestScribeContent.TYPE:
            return new RawTestScribeContent(buf,endpoint);
        }
        throw new IllegalArgumentException("Unknown type: "+contentType);
      }
    
    });
  }

  /**
   * Usage: DistScribeRegrTest [-port p] [-bootstrap host[:port]] [-nodes n] [-protocol (rmi|wire)]
   * [-help]
   *
   * @param args DESCRIBE THE PARAMETER
   */
  public static void main(String args[]) throws IOException {
    Environment env = parseArgs(args);
    
    ScribeRegrTest scribeTest = new RawScribeRegrTest(env);
    
    
    scribeTest.start();
    env.destroy();
  }

  /**
   * Utility class for past content objects
   *
   * @version $Id: ScribeRegrTest.java 3157 2006-03-19 12:16:58Z jeffh $
   * @author amislove
   */
  protected static class RawTestScribeContent extends TestScribeContent implements RawScribeContent {

    public static final short TYPE = 1;
    /**
     * Constructor for TestScribeContent.
     *
     * @param topic DESCRIBE THE PARAMETER
     * @param num DESCRIBE THE PARAMETER
     */
    public RawTestScribeContent(Topic topic, int num) {
      super(topic, num);
    }
    
    public short getType() {
      return TYPE;
    }
    
    public RawTestScribeContent(InputBuffer buf, Endpoint endpoint) throws IOException {
      super(new Topic(buf, endpoint), buf.readInt()); 
    }

    public void serialize(OutputBuffer buf) throws IOException {
      topic.serialize(buf);
      buf.writeInt(num);
    }
  }
}














