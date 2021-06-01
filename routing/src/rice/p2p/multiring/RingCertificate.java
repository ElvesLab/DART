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

package rice.p2p.multiring;

import java.io.*;
import java.net.*;
import java.security.*;
import java.util.*;
import java.util.zip.*;

import rice.environment.Environment;
import rice.p2p.commonapi.*;
import rice.p2p.multiring.*;
import rice.p2p.util.*;
import rice.pastry.commonapi.*;
import rice.pastry.dist.*;

/**
 * @(#) RingCertificate.java
 *
 * This class represents a certificate for a specific ring attached, which
 * contains informtation about the ring, such as bootstraps, etc...
 *
 * @version $Id: RingCertificate.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Alan Mislove
 */
public class RingCertificate implements Serializable {
  
  // serialver 
  private static final long serialVersionUID = 5915358246762577456L;
  
  // the static map of all RingCertificates available (id -> cert)
  protected static HashMap<Id, RingCertificate> CERTIFICATES = new HashMap<Id, RingCertificate>();
  
  // load all certificates that con be found
  static {
    try {
      // first load the CA public key
      URL a = ClassLoader.getSystemResource("ca.publickey");
//      System.out.println(a.getPath());
      InputStream b = a.openStream();
      GZIPInputStream c = new GZIPInputStream(b);
      BufferedInputStream d = new BufferedInputStream(c);
      ObjectInputStream ois = new XMLObjectInputStream(d);
//      ObjectInputStream ois = new XMLObjectInputStream(new BufferedInputStream(new GZIPInputStream(ClassLoader.getSystemResource("ca.publickey").openStream())));
      PublicKey caPublic = (PublicKey) ois.readObject();
      ois.close();
      
      // and finally load and verify the certs
      URL a1 = ClassLoader.getSystemResource("ringcert.list");
      InputStream b1 = a1.openStream();
      InputStreamReader c1 = new InputStreamReader(b1);
      BufferedReader r = new BufferedReader(c1);
      String filename = null;
      
      while ((filename = r.readLine()) != null) {
//        System.out.println(filename);
        URL a2 = ClassLoader.getSystemResource(filename);
        InputStream b2 = a2.openStream();
        RingCertificate cert = readFromStream(b2);
        
        if (cert.verify(caPublic)) {
          if ((getCertificate(cert.getId()) == null) || 
              (getCertificate(cert.getId()).getVersion() < cert.getVersion())) {
            cert.refresh();
            CERTIFICATES.put(cert.getId(), cert);
          } 
        } else {
          System.err.println("RINGCERT: Could not verify ring certificate " + cert + " ignoring.");
        }
      }
    } catch (Exception e) {
      System.err.println("RINGCERT: ERROR: Found exception " + e + " while reading in ring certificates!");
      e.printStackTrace();
    }
  }
  
  /**
   * Method which returns the certificates found for the given ringId
   *
   * @param ringId The id
   * @return All certificate
   */
  public static RingCertificate getCertificate(Id ringId) {
    return (RingCertificate) CERTIFICATES.get(ringId);
  }
  
  /**
   * Refreshes all of the InetAddresses based on their name, not IP address.
   * 
   * In other words, it fixes InetAddresses that are stale from storage.  It reads
   * the "name" of the InetAddress, and then builds a new InetAddress with this name
   * which should do a proper dns lookup.
   *
   */
  private void refresh() {
    logServer = refreshAddress(logServer);
  }
  
  /**
   * Helper function for refresh().  Returns an identical InetSocketAddress unless
   * the ip address in address is stale.  Then it returns the current one based on a
   * dns lookup.
   * 
   * @param address
   * @return
   */
  private InetSocketAddress refreshAddress(InetSocketAddress address) {
    return new InetSocketAddress(address.getAddress().getHostName(), address.getPort());
  }

  // the name of this ring
  protected String name;
  
  // the Id of this ring
  protected Id id;
  
  // the protocol of this ring
  protected Integer protocol;
  
  // the list of bootstrap nodes for this ring
  protected InetSocketAddress[] bootstraps;
  
  // the preferred port for this ring
  protected Integer port;
  
  // the logserver for this ring
  protected InetSocketAddress logServer;
  
  // the key for the ring, used in visualization and log uploading
  protected PublicKey key;
  
  // the version number of this certificate
  protected Long version;
  
  // the signature of the above info
  protected byte[] signature;
  
  /**
   * Builds a new RingCertificate given the appropriate info
   *
   */
  public RingCertificate(String name, Id id, int protocol, InetSocketAddress[] bootstraps, int port, PublicKey key, InetSocketAddress logServer) {
    this.name = name;
    this.id = id;
    this.bootstraps = bootstraps;
    this.port = new Integer(port);
    this.key = key;
    this.logServer = logServer;
    this.version = new Long(System.currentTimeMillis());
    this.protocol = new Integer(protocol);
  }
  
  /**
   * Returns the name of this ring
   *
   * @return The name
   */
  public String getName() {
    return name;
  }
  
  /**
   * Returns the id of this ring
   *
   * @return The id
   */
  public Id getId() {
    return id;
  }
  
  /**
    * Returns the protcol of this ring
   *
   * @return The protocol
   */
  public int getProtocol() {
    return protocol.intValue();
  }
  
  /**
   * Returns the version of this ring cert
   *
   * @return The version
   */
  public long getVersion() {
    return version.longValue();
  }
  
  /**
   * Returns the bootstraps of this ring
   *
   * @return The bootstraps
   */
  public InetSocketAddress[] getBootstraps() {
    return bootstraps;
  }
  
  /**
   * Returns the preferred port of this ring
   *
   * @return The preferred port
   */
  public int getPort() {
    return port.intValue();
  }
  
  /**
   * Returns the public key which is used to authenticate
   *
   * @return The public key
   */
  public PublicKey getKey() {
    return key;
  }
  
  /**
   * Returns the log server of this ring
   *
   * @return The log server
   */
  public InetSocketAddress getLogServer() {
    return logServer;
  }
  
  /**
   * Signs this RingCertificate, given the private key to sign 
   * with
   *
   * @priv The private key to sign with
   */
  private void sign(PrivateKey priv) {
    if (signature != null)
      throw new IllegalArgumentException("Attempt to sign an already-signed RingCertificate!");
    
    try {
      signature = SecurityUtils.sign(SecurityUtils.serialize(getIdentifier()), priv);
    } catch (IOException e) {
      throw new RuntimeException(e); 
    }
  }
  
  /**
   * Verifies this RingCertificate, given the public key to verify 
   * with
   *
   * @pub The public key to verify with
   */
  private boolean verify(PublicKey pub) {
    if (signature == null)
      throw new IllegalArgumentException("Attempt to verify an unsigned RingCertificate!");
    
    try {
      return SecurityUtils.verify(SecurityUtils.serialize(getIdentifier()), signature, pub);
    } catch (SecurityException e) {
      return false;
    } catch (IOException e) {
      throw new RuntimeException(e); 
    }
  }
  
  /**
   * Returns a string of this object
   *
   * @return a string
   */
  public String toString() {
    return "[Ring Certificate for ring '" + name + "' (" + id + ")]";
  }
  
  /**
   * Writes this certificate to the given file
   *
   * @param file The file to write to
   */
  private void writeToFile(File file) throws IOException {
    ObjectOutputStream oos = null;
    
    try {
      oos = new XMLObjectOutputStream(new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(file))));
      oos.writeObject(this);
    } finally {
      if (oos != null)
        oos.close();
    }
  }
    
  /**
   * Reads a certificate from the given stream
   *
   * @param stream The file to write to
   */
  private static RingCertificate readFromStream(InputStream stream) throws IOException {
    ObjectInputStream ois = null;
    
    try {
      ois = new XMLObjectInputStream(new BufferedInputStream(new GZIPInputStream(stream)));
      return (RingCertificate) ois.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e.getMessage());
    } finally {
      if (ois != null)
        ois.close();
    }
  }
  
  /**
   * Internal method which writes out the keypair to a file, encrypted
   *
   * @param pair The keypair
   * @param pass THe password
   * @param ring The ring name
   */
  private static void writeKeyPair(KeyPair pair, String pass, String ring) throws IOException {
    byte[] cipher = SecurityUtils.encryptSymmetric(SecurityUtils.serialize(pair), SecurityUtils.hash(pass.getBytes()));
    ObjectOutputStream oos = new XMLObjectOutputStream(new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(ring.toLowerCase() + ".ringkeypair.enc"))));
    oos.writeObject(cipher);
    oos.close();
  }  
  
  /**
   * Internal method which writes out the keypair to a file, encrypted
   *
   * @param pass THe password
   * @param ring The ring name
   */
  public static KeyPair readKeyPair(String ring, String pass) throws IOException, ClassNotFoundException {
    ObjectInputStream ois = new XMLObjectInputStream(new BufferedInputStream(new GZIPInputStream(new FileInputStream(ring.toLowerCase() + ".ringkeypair.enc"))));
    byte[] cipher = (byte[]) ois.readObject();
    ois.close();

    return (KeyPair) SecurityUtils.deserialize(SecurityUtils.decryptSymmetric(cipher, SecurityUtils.hash(pass.getBytes())));
  }    
  
  /**
   * Internal method which returns the to-be signed data
   *
   * @return The data
   */
  private Object getIdentifier() {
    return new Object[] {name, id, bootstraps, port, key, logServer, version, protocol};
  }
  
  /**
   * Main method which, as a utility, generates a RingCertificate by asking the user
   * for prompts
   *
   */
  public static void main(String[] args) throws Exception {
    Environment env = new Environment();
    BufferedReader r = new BufferedReader(new InputStreamReader(System.in));
    BufferedWriter w = new BufferedWriter(new OutputStreamWriter(System.out));
    
    // first, load the CA keypair
    File f = new File("ca.keypair.enc");      
    ObjectInputStream ois = new XMLObjectInputStream(new BufferedInputStream(new GZIPInputStream(new FileInputStream(f))));
    KeyPair caPair = (KeyPair) SecurityUtils.deserialize(SecurityUtils.decryptSymmetric((byte[]) ois.readObject(), SecurityUtils.hash(prompt(r, w, "Please enter the CA password: ").trim().getBytes())));
    ois.close();
    
    // get the ring info
    String ring = prompt(r, w, "Please enter the name of the ring (rice, berkeley): ");
//    String protocol = prompt(r, w, "Please enter the protocol of the ring (socket, wire, rmi): ");
    String[] bootstrap = prompt(r, w, "Please enter the bootstraps (host1:port1,host2:port2...): ").trim().split(",");
    int port = Integer.parseInt(prompt(r, w, "Please enter the default port for nodes: "));
    String logServer = prompt(r, w, "Please enter the log upload server (host:port): ");
    String pass = prompt(r, w, "Please enter a password for the ring keypair: ");
        
    // translate the protocol
    int protocolId = 0;
//    if (protocol.equalsIgnoreCase("wire")) {
//      protocolId = DistPastryNodeFactory.PROTOCOL_WIRE;
//    } else if (protocol.equalsIgnoreCase("rmi")) {
//      protocolId = DistPastryNodeFactory.PROTOCOL_RMI;
//    } else 
//      if (protocol.equalsIgnoreCase("socket")) {
      protocolId = DistPastryNodeFactory.PROTOCOL_SOCKET;
//    } 
    
    // build the id
    Id id = generateId(ring, env);
    
    // translate the InetSocketAddresses
    InetSocketAddress log = toInetSocketAddress(logServer);
    InetSocketAddress[] bootstraps = new InetSocketAddress[bootstrap.length];
    for (int i=0; i<bootstraps.length; i++)
      bootstraps[i] = toInetSocketAddress(bootstrap[i]);
    
    // generate a keypair
    KeyPair pair = SecurityUtils.generateKeyAsymmetric();

    // now create the Ring Certificate
    RingCertificate cert = new RingCertificate(ring, id, protocolId, bootstraps, port, pair.getPublic(), log);
    cert.sign(caPair.getPrivate());
    if (! cert.verify(caPair.getPublic())) 
      throw new RuntimeException("Could not verify generated certificate!");
    
    cert.writeToFile(new File(ring.toLowerCase() + ".ringcert"));
    
    // and finally write out the KeyPair
    writeKeyPair(pair, pass, ring.toLowerCase());
    
    // Environment's Daemon thread.
    System.exit(0);
  }
  
  /**
   * Internal method for prompting the user
   *
   * @param reader The reader
   * @param writer The writer
   * @param prompt The prompt
   * @return The result
   */
  private static String prompt(BufferedReader r, BufferedWriter w, String prompt) throws IOException {
    w.write(prompt);
    w.flush();
    return r.readLine();
  }
  
  /**
   * Intenrla method for String -> InetSocketAddress
   *
   * @param string The string
   * @return The address
   */
  private static InetSocketAddress toInetSocketAddress(String s) throws IOException {
    String host = s.substring(0, s.indexOf(":"));
    int port = Integer.parseInt(s.substring(s.indexOf(":") + 1));
    
    return new InetSocketAddress(host, port);
  }
  
  /**
   * Internal method for convering to canocial form
   *
   * @param string THe string
   */
  private static Id generateId(String s, Environment env) {
    String ring = s.substring(0, 1).toUpperCase() + s.substring(1).toLowerCase();
    
    PastryIdFactory pif = new PastryIdFactory(env);
    
    Id ringId = pif.buildId(ring);
    byte[] ringData = ringId.toByteArray();
    
    for (int i=0; i<ringData.length - env.getParameters().getInt("p2p_multiring_base"); i++) 
      ringData[i] = 0;
    
    
    if (s.toLowerCase().equals("global"))
      for (int i=0; i<ringData.length; i++) 
        ringData[i] = 0;
    
    return pif.buildId(ringData);
  }
}




