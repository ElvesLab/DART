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
package rice.p2p.util.tuples;

public class Tuple3<A,B,C> {
  protected A a;
  protected B b;
  protected C c;
  
  public Tuple3(A a, B b, C c) {
    this.a = a;
    this.b = b;
    this.c = c;
  }
  
  public A a() {
    return a;
  }
  
  public B b() {
    return b;
  }
  
  public C c() {
    return c;
  }
  
  /**
   * Gotta handle null values
   */
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Tuple3) {  
      Tuple3<A,B,C> that = (Tuple3<A,B,C>)obj;
      if (this.a == null) { 
        if (that.a != null) return false;        
      } else {
        if (that.a == null) return false;
        // we know this.a && that.a != null
        if (!this.a.equals(that.a)) return false;
      }
      
      if (this.b == null) { 
        if (that.b != null) return false;        
      } else {
        if (that.b == null) return false;
        // we know this.a && that.a != null
        if (!this.b.equals(that.b)) return false;
      }
      
      if (this.c == null) { 
        if (that.c != null) return false;        
      } else {
        if (that.c == null) return false;
        // we know this.a && that.a != null
        if (!this.c.equals(that.c)) return false;
      }
      
      return true;
    }
    return false;
  }

  /**
   * Gotta handle null values.
   */
  @Override
  public int hashCode() {
    int hashA = 0;
    if (a != null) hashA = a.hashCode();
    int hashB = 0;
    if (b != null) hashB = b.hashCode();
    int hashC = 0;
    if (b != null) hashC = c.hashCode();
    return hashA^hashB^hashC;
  }  

}
