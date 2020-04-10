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

package rice.pastry.standard;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.pastry.*;
import rice.pastry.messaging.*;

import java.security.*;

/**
 * Constructs an address for a specific class and instance name.
 *
 * @version $Id: StandardAddress.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Alan Mislove
 */
public class StandardAddress {

  //serial ver for backward compatibility
  private static final long serialVersionUID = 1564239935633411277L;

  @SuppressWarnings("unchecked")
  public static int getAddress(Class c, String instance, Environment env) {
    MessageDigest md = null;
    
    try {
      md = MessageDigest.getInstance("SHA");
    } catch ( NoSuchAlgorithmException e ) {
      Logger logger = env.getLogManager().getLogger(StandardAddress.class, null);
      if (logger.level <= Logger.SEVERE) logger.log(
        "No SHA support!" );
    }
    
    byte[] digest = new byte[4];
    md.update(c.toString().getBytes());
    
    digest[0] = md.digest()[0];
    digest[1] = md.digest()[1];

    if ((instance == null) || (instance.equals(""))) {
      // digest 2,3 == 0
    } else {
      md.reset();
      md.update(instance.getBytes());
      
      digest[2] = md.digest()[0];
      digest[3] = md.digest()[1];
    }    
    
    int myCode = shift(digest[0],24) | shift(digest[1],16) |
             shift(digest[2],8) | shift(digest[3],0);

    return myCode;
  }
  
  /**
   * Returns the short prefix to look for.
   * 
   * @param c
   * @param env
   * @return
   */
  @SuppressWarnings("unchecked")
  public static short getAddress(Class c, Environment env) {
    int myCode = getAddress(c, null, env);
    
    short code = (short)unshift(myCode, 16);
    return code;
  }
  
  private static int shift(int n, int s) {
    return (n & 0xff) << s;
  }

  private static int unshift(int n, int s) {
    return n >> s;
  }

  
  @SuppressWarnings("unchecked")
  public static void main(String[] args) {
    Class c = StandardAddress.class;
//    Class c = SocketPastryNode.class;
    int a = getAddress(c, null, null);
    int b = getAddress(c, "b", null);
    short a1 = getAddress(c, null);
    
    System.out.println(a+" "+Integer.toBinaryString(a)+" "+a1+" "+Integer.toBinaryString(a1));
    System.out.println(b+" "+Integer.toBinaryString(b));
  }
  
//  public static int getAddress(Class c, String instance, Environment env) {
//    MessageDigest md = null;
//    
//    try {
//      md = MessageDigest.getInstance("SHA");
//    } catch ( NoSuchAlgorithmException e ) {
//      Logger logger = env.getLogManager().getLogger(StandardAddress.class, null);
//      if (logger.level <= Logger.SEVERE) logger.log(
//        "No SHA support!" );
//    }
//    
//    String name = c.toString() + "-" + instance;
//
//    md.update(name.getBytes());
//    byte[] digest = md.digest();
//
//    int myCode = (digest[0] << 24) + (digest[1] << 16) +
//             (digest[2] << 8) + digest[3];
//
//    return myCode;
//  }
}

