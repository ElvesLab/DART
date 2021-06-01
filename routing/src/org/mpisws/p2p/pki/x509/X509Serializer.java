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
import java.security.NoSuchProviderException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import org.mpisws.p2p.transport.util.Serializer;

import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;

public interface X509Serializer extends Serializer<X509Certificate> {
//  X509Certificate deserialize(InputBuffer buf) throws IOException, CertificateException, NoSuchProviderException;
//  void serialize(OutputBuffer buf, X509Certificate cert) throws IOException, CertificateEncodingException;  
  
  


//  private void encodeCertificate(Certificate certificate, DataOutputStream dataoutputstream)
//  throws IOException
//{
//  try
//  {
//      byte abyte0[] = certificate.getEncoded();
//      dataoutputstream.writeUTF(certificate.getType());
//      dataoutputstream.writeInt(abyte0.length);
//      dataoutputstream.write(abyte0);
//  }
//  catch(CertificateEncodingException certificateencodingexception)
//  {
//      throw new IOException(certificateencodingexception.toString());
//  }
//}
//private Certificate decodeCertificate(DataInputStream datainputstream)
//  throws IOException
//{
//  String s = datainputstream.readUTF();
//  byte abyte0[] = new byte[datainputstream.readInt()];
//  datainputstream.readFully(abyte0);
//  try
//  {
//      CertificateFactory certificatefactory = CertificateFactory.getInstance(s, "BC");
//      ByteArrayInputStream bytearrayinputstream = new ByteArrayInputStream(abyte0);
//      return certificatefactory.generateCertificate(bytearrayinputstream);
//  }
//  catch(NoSuchProviderException nosuchproviderexception)
//  {
//      throw new IOException(nosuchproviderexception.toString());
//  }
//  catch(CertificateException certificateexception)
//  {
//      throw new IOException(certificateexception.toString());
//  }
//}

}
