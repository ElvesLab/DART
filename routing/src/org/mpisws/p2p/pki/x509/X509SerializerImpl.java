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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;

public class X509SerializerImpl implements X509Serializer {

  CertificateFactory certificatefactory;

  public X509SerializerImpl() throws CertificateException, NoSuchProviderException {
    certificatefactory = CertificateFactory.getInstance("X.509", "BC");    
  }
  
  public void serialize(X509Certificate cert, OutputBuffer buf) throws IOException {
    try {
      byte[] encoded = cert.getEncoded();
      buf.writeInt(encoded.length);
      buf.write(encoded, 0, encoded.length);
    } catch (CertificateEncodingException cee) {
      IOException ioe = new IOException(cee.getLocalizedMessage());
      ioe.initCause(cee);
      throw ioe;
    }
  }
  
  public X509Certificate deserialize(InputBuffer buf) throws IOException {
    try {
      byte[] encoded = new byte[buf.readInt()]; 
      buf.read(encoded);
      ByteArrayInputStream bytearrayinputstream = new ByteArrayInputStream(encoded);
      return (X509Certificate)certificatefactory.generateCertificate(bytearrayinputstream);    
    } catch (CertificateException ce) {
      IOException ioe = new IOException(ce.getLocalizedMessage());
      ioe.initCause(ce);
      throw ioe;
    }
  }

  
}
