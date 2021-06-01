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

/**
 * Just creates a temp file, ignoring the filename/size.  Does not delete on exit, as this is not memory safe.
 * 
 * @author Jeff Hoye
 *
 */
public class TempFileAllocationStrategy implements FileAllocationStrategy {
  protected String prefix;
  protected String suffix;
  protected File dir;
  
  public TempFileAllocationStrategy() {
    this("FreePastry",null);
  }
  
  public TempFileAllocationStrategy(String prefix, String suffix) {
    this(prefix, suffix, null);
  }
  
  public TempFileAllocationStrategy(String prefix, String suffix, File directory) {
    this.prefix = prefix;
    this.suffix = suffix;
    this.dir = directory;
  }
  
  public synchronized File getFile(ByteBuffer metadata, long offset, long length) throws IOException {
    File temp = File.createTempFile(prefix, suffix, dir);
    //temp.deleteOnExit();
    return temp;
  }

  public void fileCancelled(ByteBuffer metadata, File f, long offset,
      long downloadedLength, long requestedLength, Exception reason) {
    f.delete();
  }
  
  

}
