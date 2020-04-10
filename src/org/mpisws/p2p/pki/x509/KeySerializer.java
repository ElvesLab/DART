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
package org.mpisws.p2p.pki.x509;

import java.io.IOException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.spec.InvalidKeySpecException;

import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;

public interface KeySerializer {
  public static final byte KEY_PRIVATE = 0;
  public static final byte KEY_PUBLIC = 1;
  public static final byte KEY_SECRET = 2;
  
  public void serialize(Key k, OutputBuffer buf) throws IOException;
  public Key deserialize(InputBuffer buf) throws IOException, InvalidKeySpecException, NoSuchAlgorithmException, NoSuchProviderException;
  
//private void encodeKey(Key key, DataOutputStream dataoutputstream)
//throws IOException
//{
//byte abyte0[] = key.getEncoded();
//if(key instanceof PrivateKey)
//    dataoutputstream.write(0);
//else
//if(key instanceof PublicKey)
//    dataoutputstream.write(1);
//else
//    dataoutputstream.write(2);
//dataoutputstream.writeUTF(key.getFormat());
//dataoutputstream.writeUTF(key.getAlgorithm());
//dataoutputstream.writeInt(abyte0.length);
//dataoutputstream.write(abyte0);
//}


//private Key decodeKey(DataInputStream datainputstream)
//throws IOException
//{
//int i;
//String s1;
//Object obj;
//i = datainputstream.read();
//String s = datainputstream.readUTF();
//s1 = datainputstream.readUTF();
//byte abyte0[] = new byte[datainputstream.readInt()];
//datainputstream.readFully(abyte0);
//if(s.equals("PKCS#8") || s.equals("PKCS8"))
//    obj = new PKCS8EncodedKeySpec(abyte0);
//else
//if(s.equals("X.509") || s.equals("X509"))
//    obj = new X509EncodedKeySpec(abyte0);
//else
//if(s.equals("RAW"))
//    return new SecretKeySpec(abyte0, s1);
//else
//    throw new IOException((new StringBuilder()).append("Key format ").append(s).append(" not recognised!").toString());
//i;
//JVM INSTR tableswitch 0 2: default 215
//   goto _L1 _L2 _L3 _L4
//_L2:
//return KeyFactory.getInstance(s1, "BC").generatePrivate(((java.security.spec.KeySpec) (obj)));
//_L3:
//try
//{
//    return KeyFactory.getInstance(s1, "BC").generatePublic(((java.security.spec.KeySpec) (obj)));
//}
//catch(Exception exception)
//{
//    throw new IOException((new StringBuilder()).append("Exception creating key: ").append(exception.toString()).toString());
//}
//_L4:
//return SecretKeyFactory.getInstance(s1, "BC").generateSecret(((java.security.spec.KeySpec) (obj)));
//_L1:
//throw new IOException((new StringBuilder()).append("Key type ").append(i).append(" not recognised!").toString());
//}

}
