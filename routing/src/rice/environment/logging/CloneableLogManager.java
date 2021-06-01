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
/*
 * Created on Jun 15, 2005
 */
package rice.environment.logging;

import rice.environment.time.TimeSource;

/**
 * If you implement this interface, then your log manager can be cloned.
 * This is usually used so different nodes can have different log managers.
 * 
 * The simple log manager uses the @param detail arg as a prefix to each
 * line corresponding to that logger.
 * 
 * Another example may be logging to multiple files, but using the "detail"
 * in the filename to indicate which node the log corresponds to.
 * 
 * A PastryNodeFactory should clone the LogManager for each node if the 
 * pastry_factory_multipleNodes parameter is set to true.
 * 
 * 
 * @author Jeff Hoye
 */
public interface CloneableLogManager extends LogManager {
  /**
   * Return a new LogManager with identical parameters except that 
   * there is an indication of detail in each line, or filename if 
   * seperated by files.
   * 
   * @param detail usually will be a nodeid
   * @return a new LogManager 
   */
  LogManager clone(String detail);
  LogManager clone(String detail, TimeSource ts);
  String getPrefix();
}
