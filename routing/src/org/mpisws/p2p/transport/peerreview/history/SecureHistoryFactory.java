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
package org.mpisws.p2p.transport.peerreview.history;

import java.io.IOException;

public interface SecureHistoryFactory {

  /**
   * Creates a new history (aka log). Histories are stored as two files: The 'index' file has a 
   * fixed-size record for each entry, which contains the sequence number, content and node
   * hashes, as well as an index into the data file. The 'data' file just contains the raw
   * bytes for each entry. Note that the caller must specify the node hash and the sequence
   * number of the first log entry, which forms the base of the hash chain.
   */
  SecureHistory create(String name, long baseSeq, byte[] baseHash) throws IOException;
  SecureHistory createTemp(long baseSeq, byte[] baseHash) throws IOException;
  SecureHistory open(String name, String mode) throws IOException;
}
