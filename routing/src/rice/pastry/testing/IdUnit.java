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
package rice.pastry.testing;

import rice.pastry.Id;

public class IdUnit {

  /**
   * @param args
   */
  public static void main(String[] args) {
    Id id0 = Id.build("0");
    Id id1 = Id.build("1");
    Id id2 = Id.build("2");
    Id id3 = Id.build("3");
    Id id4 = Id.build("4");
    Id id5 = Id.build("5");
    Id id6 = Id.build("6");
    Id id7 = Id.build("7");
    Id id8 = Id.build("8");
    Id id9 = Id.build("9");
    Id ida = Id.build("A");
    Id idb = Id.build("B");
    Id idc = Id.build("C");
    Id idd = Id.build("D");
    Id ide = Id.build("E");
    Id idf = Id.build("F");

    test("clockwise 0-1",id0.clockwise(id1));
    test("clockwise 1-0",!id1.clockwise(id0));
    test("clockwise 0-8",id0.clockwise(id8));
    test("clockwise 8-0",id8.clockwise(id0));
    test("clockwise 0-9",!id0.clockwise(id9));
    test("clockwise 9-0",id9.clockwise(id0));

    test("between 0-1-2",id1.isBetween(id0,id2));
    test("between 2-1-0",!id1.isBetween(id2,id0));
    test("between 0-4-2",!id4.isBetween(id0,id2));
    test("between 2-4-0",id4.isBetween(id2,id0));
    test("between F-0-1",id0.isBetween(idf,id1));
    test("between 1-0-F",!id0.isBetween(id1,idf));
  }
  
  public static void test(String msg, boolean result) {
    System.out.println(msg + ": " + (result ? "PASS" : "FAIL"));
  }

}
