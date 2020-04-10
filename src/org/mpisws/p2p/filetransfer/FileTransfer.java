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
package org.mpisws.p2p.filetransfer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import rice.Continuation;

public interface FileTransfer {
  /**
   * 
   * @param f the file to send
   * @param metadata this data will be delivered to the FileAllocationStrategy and the FileTransferCallback, it can
   * contain whatever the application needs to name the file, often a filename is sufficient.
   * @param priority the priority of sending
   * @param offset where to start in the file
   * @param length how many bytes to send (must be <= fileLength-offset)
   * @param c who to notify when it is done
   * @return
   */
  public FileReceipt sendFile(File f, ByteBuffer metadata, byte priority, long offset, long length, Continuation<FileReceipt, Exception> c) throws IOException;
  public FileReceipt sendFile(File f, ByteBuffer metadata, byte priority, Continuation<FileReceipt, Exception> c) throws IOException;
  
  public BBReceipt sendMsg(ByteBuffer bb, byte priority, Continuation<BBReceipt, Exception> c);
  public void addListener(FileTransferListener listener);
  public void removeListener(FileTransferListener listener);
}
