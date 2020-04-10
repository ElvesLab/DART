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
package rice.p2p.splitstream;

import java.io.IOException;
import java.util.StringTokenizer;

import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.scribe.ScribeContent;
import rice.p2p.scribe.rawserialization.RawScribeContent;

/**
 * This represents data sent through scribe for splitstream
 *
 * @version $Id: SplitStreamContent.java 3810 2007-07-30 12:59:55Z jeffh $
 * @author Alan Mislove
 */
public class SplitStreamContent implements RawScribeContent {
  public static final short TYPE = 1;

  /**
   * The internal data - just the bytes
   */
  protected byte[] data;

  /**
   * Constructor taking in a byte[]
   *
   * @param data The data for this content
   */
  public SplitStreamContent(byte[] data) {
    this.data = data;
  }

  /**
   * Returns the data for this content
   *
   * @return The data for this content
   */
  public byte[] getData() {
    return data;
  }
/*
  public String toString() {


      //Byte bt = new Byte(data[0]);
      String ds = new String(data);
      StringTokenizer tk = new StringTokenizer(ds);
      String seqNumber = tk.nextToken();
      String sentTime = tk.nextToken();
//      Id stripeId = (rice.pastry.Id)(s.getStripeId().getId());
//      String str = stripeId.toString().substring(3, 4);
//      int recv_time = (int)Systemm.currentTimeMillis();
//      int diff;
////      char [] c = str.toString().toCharArray();
//      int stripe_int = c[0] - '0';
//      if(stripe_int > 9)
//    stripe_int = 10 + c[0] - 'A';
//      else
//    stripe_int = c[0] - '0';
      
      return seqNumber+"\t"+"\t"+sentTime;
  }
  
  */
  
//  public String toString() {
//    return "SSContent{"+data.length+"}";
//  }
  
  /***************** Raw Serialization ***************************************/
  public short getType() {
    return TYPE;
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeByte((byte)0);
    buf.writeInt(data.length);
    buf.write(data,0,data.length);
  }
  
  public SplitStreamContent(InputBuffer buf) throws IOException {
    byte version = buf.readByte();
    switch(version) {
      case 0:
        data = new byte[buf.readInt()];
        buf.read(data);
        break;
      default:
        throw new IOException("Unknown Version: "+version);
    }
  }
}

