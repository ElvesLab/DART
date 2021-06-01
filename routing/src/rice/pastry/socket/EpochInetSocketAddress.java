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

package rice.pastry.socket;

import java.io.*;
import java.net.*;

import rice.p2p.commonapi.rawserialization.*;

/**
 * Class which represets a source route to a remote IP address.
 *
 * @version $Id: EpochInetSocketAddress.java 3613 2007-02-15 14:45:14Z jstewart $
 * @author Alan Mislove
 */
public class EpochInetSocketAddress implements Serializable {
  
  /**
   * 
   */
  private static final long serialVersionUID = 2081191512212313338L;

  // a static epoch which indicates an unknown (and unmattering) epoch number
  public static final long EPOCH_UNKNOWN = -1;
    
  // the address list, most external first
  protected InetSocketAddress address[];
  
  // the epoch number of the remote node
  protected long epoch;
  
  /**
   * Constructor - don't use this unless you know what you are doing
   *
   * @param address The remote address
   */
  public EpochInetSocketAddress(InetSocketAddress address) {
    this(address, EPOCH_UNKNOWN);
  }  
  
  /**
   * Constructor
   *
   * @param address The remote address
   * @param epoch The remote epoch
   */
  public EpochInetSocketAddress(InetSocketAddress address, long epoch) {
    this(new InetSocketAddress[]{address}, epoch);
  }  

  public EpochInetSocketAddress(InetSocketAddress[] addressList, long epoch) {
    this.address = addressList;
    this.epoch = epoch;    
  }
  
  /**
   * Returns the hashCode of this source route
   *
   * @return The hashCode
   */
  public int hashCode() {
    int result = (int)epoch;
    for (int i = 0; i < address.length; i++) {
      result ^=  address[i].hashCode();
    }
    return result;
  }
  
  /**
   * Checks equaltiy on source routes
   *
   * @param o The source route to compare to
   * @return The equality
   */
  public boolean equals(Object o) {
    if (o == null) return false;
    if (! (o instanceof EpochInetSocketAddress)) return false;
    EpochInetSocketAddress that = (EpochInetSocketAddress)o;
    if (this.epoch != that.epoch) return false;
    return addressEquals(that);
  }
  
  public boolean addressEquals(EpochInetSocketAddress that) {
    if (this.address.length != that.address.length) return false;
    for (int ctr = 0; ctr < this.address.length; ctr++) {
      if (!this.address[ctr].equals(that.address[ctr])) return false;
    }
    return true;
  }
  
  /**
    * Internal method for computing the toString of an array of InetSocketAddresses
   *
   * @param path The path
   * @return THe string
   */
  public String toString() {
    String s = "";
    for (int ctr = 0; ctr < address.length; ctr++) {
      s+=address[ctr];
      if (ctr < address.length-1) s+=":";  
    }
    return s + " [" + epoch + "]";
  }
  
  /**
   * Method which returns the address of this address
   *
   * @return The address
   */
  public InetSocketAddress getAddress(EpochInetSocketAddress local) {   
    // start from the outside address, and return the first one not equal to the local address (sans port)
    
    try {
      for (int ctr = 0; ctr < address.length; ctr++) {
        if (!address[ctr].getAddress().equals(local.address[ctr].getAddress())) {
          return address[ctr];
        }
      }
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      throw new RuntimeException("ArrayIndexOutOfBoundsException in "+this+".getAddress("+local+")",aioobe);
    }
    return address[address.length-1]; // the last address if we are on the same computer
  }
  
  /**
   * This is for hairpinning support.  The Node is advertising many different 
   * InetSocketAddresses that it could be contacted on.  In a typical NAT situation 
   * this will be 2: the NAT's external address, and the Node's non-routable 
   * address on the Lan.  
   * 
   * The algorithm sees if the external address matches its own external 
   * address.  If it doesn't then the node is on a different lan, use the external.
   * If the external address matches then both nodes are on the same Lan, and 
   * it uses the internal address because the NAT may not support hairpinning.  
   * 
   * @param local my sorted list of InetAddress
   * @return the address I should use to contact the node
   */
  public InetSocketAddress getAddress(InetAddress[] local) {   
    // start from the outside address, and return the first one not equal to the local address (sans port)
    try {
      for (int ctr = 0; ctr < address.length; ctr++) {
        if (!address[ctr].getAddress().equals(local[ctr])) {
          return address[ctr];
        }
      }
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      String s = "";
      for (int ctr = 0; ctr < local.length; ctr++) {
        s+=local[ctr];
        if (ctr < local.length-1) s+=":";  
      }
      throw new RuntimeException("ArrayIndexOutOfBoundsException in "+this+".getAddress("+local.length+")",aioobe);
    }
    return address[address.length-1]; // the last address if we are on the same computer
  }
  
  
  
  public InetSocketAddress getInnermostAddress() {
    return address[address.length-1];     
  }
  
  /**
   * Method which returns the epoch of this address
   *
   * @return The epoch
   */
  public long getEpoch() {
    return epoch;
  }

  /**
   *   EpochInetSocketAddress: (IPV4):
   *   +-+-+-+-+-+-+-+-+
   *   +   numAddrs    +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +   internet address 0                                          +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +   port 0                      +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +   internet address 1                                          +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +   port 1                      +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +   internet address k                                          +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +   port k                      +       ...
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   + epoch (long)                                                  +
   *   +                                                               +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *  
   * @param buf
   * @return
   * @throws IOException
   */
  public static EpochInetSocketAddress build(InputBuffer buf) throws IOException {
    byte numAddresses = buf.readByte();
    InetSocketAddress[] saddr = new InetSocketAddress[numAddresses];
    for (int ctr = 0; ctr < numAddresses; ctr++) {
      byte[] addrBytes = new byte[4];
      buf.read(addrBytes);
      InetAddress addr = InetAddress.getByAddress(addrBytes);
      short port = buf.readShort();
      saddr[ctr] = new InetSocketAddress(addr, 0xFFFF & port);
    }
//    System.out.println("EISA.build():numAddresses:"+numAddresses);
    long epoch = buf.readLong();
    return new EpochInetSocketAddress(saddr, epoch);
  }

  /**
   *   EpochInetSocketAddress: (IPV4):
   *   +-+-+-+-+-+-+-+-+
   *   +   numAddrs    +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +   internet address 0                                          +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +   port 0                      +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +   internet address 1                                          +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +   port 1                      +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +   internet address k                                          +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +   port k                      +       ...
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   + epoch (long)                                                  +
   *   +                                                               +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   * 
   * @param buf
   */
  public void serialize(OutputBuffer buf) throws IOException {
//    System.out.println("EISA.serialize():numAddresses:"+address.length);
    buf.writeByte((byte)address.length);
    for (int ctr = 0; ctr < address.length; ctr++) {
      buf.write(address[ctr].getAddress().getAddress(),0,4);
      buf.writeShort((short)address[ctr].getPort());
    }
    buf.writeLong(epoch);    
  }

  public short getSerializedLength() {
    return (short)(8+1+(address.length*6)); // epoch+numAddresses+(numAddresses*(address.length+port.length)
  }

}


