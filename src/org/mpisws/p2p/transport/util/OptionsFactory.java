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
package org.mpisws.p2p.transport.util;

import java.util.HashMap;
import java.util.Map;

public class OptionsFactory {
  public static Map<String, Object> addOption(Map<String, Object> existing, String s, Object i) {
    Map<String, Object> ret = copyOptions(existing);
    ret.put(s,i);
    return ret;
  }
  public static Map<String, Object> addOption(Map<String, Object> existing, String s1, Object i1, String s2, Object i2) {
    Map<String, Object> ret = copyOptions(existing);
    ret.put(s1,i1);
    ret.put(s2,i2);
    return ret;
  }
  
  public static Map<String, Object> addOption(Map<String, Object> existing, String s1, Object i1, String s2, Object i2, String s3, Object i3) {
    Map<String, Object> ret = copyOptions(existing);
    ret.put(s1,i1);
    ret.put(s2,i2);
    ret.put(s3,i3);
    return ret;
  }

  public static Map<String, Object> copyOptions(Map<String, Object> existing) {
    if (existing == null) return new HashMap<String, Object>();
    return new HashMap<String, Object>(existing);
  }
  
  /**
   * Merge 2 options, keeping the first if there is a conflict
   * 
   * @param options
   * @param options2
   * @return
   */
  public static Map<String, Object> merge(Map<String, Object> options,
      Map<String, Object> options2) {
    Map<String, Object> ret = copyOptions(options2);
    if (options == null) return ret;
    for (String k : options.keySet()) {
      ret.put(k, options.get(k));
    }
    return ret;
  }
  
  public static Map<String, Object> removeOption(Map<String, Object> options,
      String option) {
    Map<String, Object> ret = copyOptions(options);
    ret.remove(option);
    return ret;
  }
}
