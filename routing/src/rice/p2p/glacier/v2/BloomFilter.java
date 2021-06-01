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
package rice.p2p.glacier.v2;

import java.io.*;
import java.util.Arrays;

import rice.environment.logging.Logger;
import rice.environment.random.RandomSource;
import rice.environment.random.simple.SimpleRandomSource;
import rice.p2p.commonapi.rawserialization.*;

public class BloomFilter implements Serializable {
  
  private static final long serialVersionUID = -3938913031743354080L;
  
  private byte bitfield[];
  private int hashParams[];
  
  public BloomFilter(int length, int[] hashParams) {
    bitfield = new byte[(length+7)/8];
    Arrays.fill(bitfield, (byte)0);
    this.hashParams = hashParams;
  }  
  
  public BloomFilter(int length, int numHashes, RandomSource rand) {
    bitfield = new byte[(length+7)/8];
    Arrays.fill(bitfield, (byte)0);

    int numPrimeCandidates = numHashes*100;
    if (numPrimeCandidates >= (length - 5)) 
      numPrimeCandidates = length - 5;
    int maxFactor = (int)(Math.sqrt(length));
    int offset = length - numPrimeCandidates + 1;
    boolean[] isPrimeH = new boolean[numPrimeCandidates];
    boolean[] isPrimeL = new boolean[maxFactor + 1];
    Arrays.fill(isPrimeH, true);
    Arrays.fill(isPrimeL, true);
    
    for (int i=2; i<=maxFactor; i++) {
      if (isPrimeL[i]) {
        for (int j=0; j<=(int)(maxFactor/i); j++)
          isPrimeL[j*i] = false;
        for (int j=(int)((offset+i-1)/i); j<=(int)(length/i); j++)
          isPrimeH[j*i - offset] = false;
      }
    }
    
    hashParams = new int[numHashes];
    for (int i=0; i<numHashes; i++) {
      int index = rand.nextInt(numPrimeCandidates);
      while (!isPrimeH[index])
        index = (index+1) % numPrimeCandidates;
      isPrimeH[index] = false;
      hashParams[i] = offset + index;
    }
  }

  private int[] getHashes(byte[] data) {
    long cache = 0;
    int ctr = 0;
    int[] hash = new int[hashParams.length];
    Arrays.fill(hash, 0);
    
    for (int i=0; i<data.length; i++) {
      cache = (cache<<8) + data[i] + ((data[i]<0) ? 256 : 0);
      if (((++ctr) == 7) || (i==(data.length-1))) {
        for (int j=0; j<hashParams.length; j++)
          hash[j] += cache % hashParams[j];
        ctr = 0;
        cache = 0;
      }
    }
    
    for (int j=0; j<hashParams.length; j++)
      hash[j] = hash[j] % hashParams[j];
      
    return hash;
  }
  
  private void dump(Logger logger) {
    String s = "";
    for (int i=0; i<bitfield.length*8; i++)
      if ((bitfield[i/8] & (1<<(i&7))) == 0)
        s+="0";
      else
        s+="1";
    s+="\n";
    if (logger.level <= Logger.INFO) logger.log(s);
  }
  
  public void add(byte[] data) {
    int[] hash = getHashes(data);

/*System.outt.print("Adding ");
for (int i=0; i<data.length; i++)
  System.outt.print(data[i]+" ");
System.outt.println();
System.outt.print("  => ");
for (int i=0; i<hash.length; i++)
  System.outt.print(hash[i]+" ");
System.outt.println(); */
  
    for (int i=0; i<hashParams.length; i++)
      bitfield[hash[i]/8] |= (1<<(hash[i]&7));
  }
  
  public boolean contains(byte[] data) {
    int[] hash = getHashes(data);

/* System.outt.print("Checking ");
for (int i=0; i<data.length; i++)
  System.outt.print(data[i]+" ");
System.outt.println();
System.outt.print("  => ");
for (int i=0; i<hash.length; i++)
  System.outt.print(hash[i]+" ");
System.outt.println(); */
    
    for (int i=0; i<hashParams.length; i++)
      if ((bitfield[hash[i]/8] & (1<<(hash[i]&7))) == 0)
        return false;
        
    return true;
  }
  
  public String toString() {
    String result = "[BV "+(bitfield.length*8)+"bit = { ";
    for (int i=0; i<hashParams.length; i++)
      result = result + ((i==0) ? "" : ", ") + hashParams[i];
    result = result + " }]";
    return result;
  }
  
  public BloomFilter(InputBuffer buf) throws IOException {
    hashParams = new int[buf.readInt()];
    for (int i = 0; i < hashParams.length; i++) {
      hashParams[i] = buf.readInt();
    }
    bitfield = new byte[buf.readInt()];
    buf.read(bitfield);
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeInt(hashParams.length); 
    for (int i = 0; i < hashParams.length; i++) {
      buf.writeInt(hashParams[i]);
    }    
    buf.writeInt(bitfield.length); 
    buf.write(bitfield, 0, bitfield.length);
  }
}    
    
