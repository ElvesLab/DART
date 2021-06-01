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
package rice.p2p.glacier.v2;

import rice.environment.Environment;
import rice.environment.logging.*;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.glacier.*;
import rice.p2p.past.PastContent;
import rice.p2p.past.rawserialization.*;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.Continuation;
import java.io.*;
import java.util.Arrays;
import java.security.*;

public class GlacierDefaultPolicy implements GlacierPolicy {

  protected ErasureCodec codec;
  protected String instance;
  protected Environment environment;
  protected Logger logger;
  
  public GlacierDefaultPolicy(ErasureCodec codec, String instance, Environment env) {
    this.codec = codec;
    this.instance = instance;
    this.environment = env;
    logger = environment.getLogManager().getLogger(GlacierDefaultPolicy.class, instance);
  }

  public boolean checkSignature(Manifest manifest, VersionKey key) {
    if (manifest.getSignature() == null)
      return false;
      
    return Arrays.equals(manifest.getSignature(), key.toByteArray());
  }

  protected void signManifest(Manifest manifest, VersionKey key) {
    manifest.setSignature(key.toByteArray());
  }

  @SuppressWarnings("unchecked")
  public void prefetchLocalObject(VersionKey key, Continuation command) {
    command.receiveResult(null);
  }

  public PastContent decodeObject(Fragment[] fragments, Endpoint endpoint, PastContentDeserializer pcd) {
    return codec.decode(fragments, endpoint, pcd);
  }
  
  public Manifest[] createManifests(VersionKey key, RawPastContent obj, Fragment[] fragments, long expiration) {
    try {    
      SimpleOutputBuffer sob = new SimpleOutputBuffer();
      sob.writeShort(obj.getType());
      obj.serialize(sob);
      return createManifestsHelper(key, sob.getBytes(), sob.getWritten(), fragments, expiration);
    } catch (IOException ioe) {
      if (logger.level <= Logger.WARNING) logger.log( 
          "Cannot serialize object: "+ioe);
      return null;
    }
  }
  
  public Manifest[] createManifests(VersionKey key, PastContent obj, Fragment[] fragments, long expiration) {
    return createManifests(key, obj instanceof RawPastContent ? (RawPastContent)obj : new JavaSerializedPastContent(obj), fragments, expiration);
  }
  
  private Manifest[] createManifestsHelper(VersionKey key, byte[] bytes, int length, Fragment[] fragments, long expiration) {

    /* Get the SHA-1 hash object. */

    MessageDigest md = null;
    try {
      md = MessageDigest.getInstance("SHA");
    } catch (NoSuchAlgorithmException e) {
      if (logger.level <= Logger.WARNING) logger.log( 
          "No SHA support!");
      return null;
    }

    /* Compute the hash values. */

    byte[][] fragmentHash = new byte[fragments.length][];
    for (int i = 0; i < fragments.length; i++) {
      md.reset();
      md.update(fragments[i].getPayload());
      fragmentHash[i] = md.digest();
    }

    byte[] objectHash = null;
    md.reset();
    md.update(bytes,0,length);
    objectHash = md.digest();
    
    /* Create the manifest */
    
    Manifest[] manifests = new Manifest[fragments.length];
    for (int i=0; i<fragments.length; i++) {
      manifests[i] = new Manifest(objectHash, fragmentHash, expiration);
      signManifest(manifests[i], key);
    }
    
    return manifests;
  }
  
  public Fragment[] encodeObject(RawPastContent obj, boolean[] generateFragment) {
    if (logger.level <= Logger.FINER) logger.log( 
        "Serialize object: " + obj);

    try {
      SimpleOutputBuffer sob = new SimpleOutputBuffer();
      sob.writeShort(obj.getType());
      obj.serialize(sob);
      return encodeObjectHelper(obj, sob.getBytes(), sob.getWritten(), generateFragment); 
    } catch (IOException ioe) {
      if (logger.level <= Logger.WARNING) logger.log( 
          "Cannot serialize object: "+ioe);
      return null;
    }
  }
  
  public Fragment[] encodeObject(PastContent obj, boolean[] generateFragment) {
    return encodeObject(obj instanceof RawPastContent ? (RawPastContent)obj : new JavaSerializedPastContent(obj), generateFragment);
  }

    
  private Fragment[] encodeObjectHelper(PastContent obj, byte[] bytes, int length, boolean[] generateFragment) {  
    if (logger.level <= Logger.FINER) logger.log( 
        "Create fragments: " + obj);
    Fragment[] fragments = codec.encode(bytes, length, generateFragment);
    if (logger.level <= Logger.FINER) logger.log( 
        "Completed: " + obj);
    
    return fragments;
  }

  public Manifest updateManifest(VersionKey key, Manifest manifest, long newExpiration) {
    if (!checkSignature(manifest, key))
      return null;

    Manifest newManifest = new Manifest(manifest.getObjectHash(), manifest.getFragmentHashes(), newExpiration);
    signManifest(newManifest, key);
    
    return newManifest;
  }
}
