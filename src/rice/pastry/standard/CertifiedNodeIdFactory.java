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

import java.io.*;
import java.net.InetAddress;
import java.security.*;
import java.security.cert.*;
import java.security.spec.*;
import java.util.*;
import java.util.zip.*;
import javax.crypto.*;
import javax.crypto.spec.*;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.environment.random.simple.SimpleRandomSource;
import rice.pastry.*;
import rice.p2p.util.*;

/**
 * Builds nodeIds in a certified manner, guaranteeing that a given node will always
 * have the same nodeId.  NOTE:  Actual certification is not yet implemented, rather, 
 * using this factory simply guarantees that the node's nodeId will never change.
 *
 * @version $Id: CertifiedNodeIdFactory.java 3613 2007-02-15 14:45:14Z jstewart $
 *
 * @author Alan Mislove
 */
public class CertifiedNodeIdFactory implements NodeIdFactory {
  
  public static String NODE_ID_FILENAME = "nodeId";

  protected int port;
  protected IPNodeIdFactory realFactory;

  protected Environment environment;
  
  protected Logger logger;
  
  /**
   * Constructor.
   */
  public CertifiedNodeIdFactory(InetAddress localIP, int port, Environment env) {
    this.environment = env;
    this.port = port;
    this.logger = environment.getLogManager().getLogger(CertifiedNodeIdFactory.class,null);
    this.realFactory = new IPNodeIdFactory(localIP, port, env);
  }
  
  /**
   * generate a nodeId
   *
   * @return the new nodeId
   */
  public Id generateNodeId() {
    XMLObjectInputStream xois = null;
    try {
      File f = new File(NODE_ID_FILENAME);
      
      if (! f.exists()) {
        File g = new File("." + NODE_ID_FILENAME + "-" + port);
        
        if (g.exists())
          g.renameTo(f);
      }
      
      if (f.exists()) {
        xois = new XMLObjectInputStream(new FileInputStream(f));
        return (Id) xois.readObject();
      } else {
        if (logger.level <= Logger.WARNING) logger.log(
          "Unable to find NodeID certificate - exiting.");
        throw new RuntimeException("Unable to find NodeID certificate - make sure that the NodeID certificate file '" + NODE_ID_FILENAME + "' exists in your ePOST directory.");
      }
    } catch (IOException e) {
      if (logger.level <= Logger.WARNING) logger.logException("",e);
      throw new RuntimeException(e);
    } catch (ClassNotFoundException e) {
      if (logger.level <= Logger.WARNING) logger.logException("",e);
      throw new RuntimeException(e);
    } finally {
      try {
        if (xois != null)
          xois.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Method which generates a certificate given the nodeid, location, and private key
   *
   * @param id The id of the certificate to generate
   * @param file The location to write the certificate to
   * @param key The private key to use to sign the result
   */
  public static void generateCertificate(Id id, OutputStream os, PrivateKey key) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      XMLObjectOutputStream xoos = new XMLObjectOutputStream(baos);
      xoos.writeObject(id);
      xoos.close();
      
      XMLObjectOutputStream xoos2 = new XMLObjectOutputStream(os);
      xoos2.writeObject(id);
      xoos2.write(SecurityUtils.sign(baos.toByteArray(), key));
      xoos2.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } 
  }
  
  /**
   * Main method which, for convenience, allows certificate creation.  The parameters allowed are
   * -ca [file] -out [dir]
   */
  public static void main(String[] args) throws Exception {
    String pw = getArg(args, "-pw");
    String caDirectory = getArg(args, "-ca");
    String out = getArg(args, "-out");
    
    File f = new File(caDirectory,"ca.keypair.enc");      
    FileInputStream fis = new FileInputStream(f);
    ObjectInputStream ois = new XMLObjectInputStream(new BufferedInputStream(new GZIPInputStream(fis)));
    
    byte[] cipher = (byte[]) ois.readObject();
      
    KeyPair caPair = (KeyPair) SecurityUtils.deserialize(SecurityUtils.decryptSymmetric(cipher, SecurityUtils.hash(pw.getBytes())));
          
    Environment env = new Environment();
    generateCertificate(new RandomNodeIdFactory(env).generateNodeId(), new FileOutputStream(new File(NODE_ID_FILENAME)), caPair.getPrivate());
    env.destroy();
  }
  
  public static String getArg(String[] args, String argType) {
    for (int i = 0; i < args.length; i++) {
      if (args[i].startsWith(argType)) {
        if (args.length > i+1) {
          String ret = args[i+1];
          if (!ret.startsWith("-"))
            return ret;
        } 
      } 
    } 
    return null;
  }  
}

