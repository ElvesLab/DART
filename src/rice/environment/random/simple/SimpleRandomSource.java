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
 * Created on May 26, 2005
 */
package rice.environment.random.simple;

import java.net.InetAddress;
import java.util.Random;

import rice.environment.logging.*;
import rice.environment.random.RandomSource;

/**
 * @author Jeff Hoye
 */
public class SimpleRandomSource implements RandomSource {
  Random rnd;
  
  Logger logger;

  String instance;
  
  public SimpleRandomSource(long seed, LogManager manager, String instance) {
    init(seed, manager, instance); 
  }
    
  public SimpleRandomSource(long seed, LogManager manager) {
    this(seed, manager, null); 
  }
    
  public SimpleRandomSource(LogManager manager) {
    this(manager, null);
  }
  
  public SimpleRandomSource(LogManager manager, String instance) {
      // NOTE: Since we are often starting up a bunch of nodes on planetlab
      // at the same time, we need this randomsource to be seeded by more
      // than just the clock, we will include the IP address
      // as amazing as this sounds, it happened in a network of 20 on 7/19/2005
      // also, if you think about it, I was starting all of the nodes at the same 
      // instant, and they had synchronized clocks, if they all started within 1/10th of
      // a second, then there is only 100 different numbers to seed the generator with
      // -Jeff
      long time = System.currentTimeMillis();
      try {
        byte[] foo = InetAddress.getLocalHost().getAddress();
        for (int ctr = 0; ctr < foo.length; ctr++) {
          int i = (int)foo[ctr];
          i <<= (ctr*8);
          time ^= i; 
        }
      } catch (Exception e) {
        // if there is no NIC, screw it, this is really unlikely anyway  
      }
      init(time, manager, instance);
  }
  
  public void setLogManager(LogManager manager) {
    logger = manager.getLogger(SimpleRandomSource.class, instance);     
  }
  
  private void init(long seed, LogManager manager, String instance) {
    if (manager != null)
      logger = manager.getLogger(SimpleRandomSource.class, instance);
    if (logger != null) 
      if (logger.level <= Logger.INFO) logger.log("RNG seed = "+seed);
    rnd = new Random(seed);    
  }
  
  public boolean nextBoolean() {
    boolean ret = rnd.nextBoolean();
    if (logger != null) 
      if (logger.level <= Logger.FINER) logger.log("nextBoolean = "+ret);
    return ret;
  }
  
  public void nextBytes(byte[] bytes) {
    rnd.nextBytes(bytes);
    if (logger != null) 
      if (logger.level <= Logger.FINER) logger.log("nextBytes["+bytes.length+"] = "+bytes);
  }
  
  public double nextDouble() {
    double ret = rnd.nextDouble();
    if (logger != null) 
      if (logger.level <= Logger.FINER) logger.log("nextDouble = "+ret);
    return ret;
  }
  
  public float nextFloat() {
    float ret = rnd.nextFloat();
    if (logger != null) 
      if (logger.level <= Logger.FINER) logger.log("nextFloat = "+ret);
    return ret;
  }
  
  public double nextGaussian() {
    double ret = rnd.nextGaussian();
    if (logger != null) 
      if (logger.level <= Logger.FINER) logger.log("nextGaussian = "+ret);
    return ret;
  }
  
  public int nextInt() {
    int ret = rnd.nextInt();
    if (logger != null) 
      if (logger.level <= Logger.FINER) logger.log("nextInt = "+ret);
    return ret;
  }
  
  public int nextInt(int max) {
    int ret = rnd.nextInt(max);
    if (logger != null) 
      if (logger.level <= Logger.FINER) logger.log("nextInt2 = "+ret);
    return ret;
  }
  
  public long nextLong() {
    long ret = rnd.nextLong();
    if (logger != null) 
      if (logger.level <= Logger.FINER) logger.log("nextLong = "+ret);
    return ret;
  }
}
