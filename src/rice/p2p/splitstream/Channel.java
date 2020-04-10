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

package rice.p2p.splitstream;

import java.io.*;
import java.math.*;
import java.util.*;

import rice.p2p.commonapi.*;
import rice.p2p.scribe.*;

/**
 * The channel controls all the meta data associated with a group of stripes. It contains the
 * stripes themselves, and one of the stripe is the primary stripe for this channel.
 * A stripe whose topicId matches some prefix with local node id, then it becomes its primary stripe.
 * A channelId uniquely identifies a channel. Number of stripes is obtained by pow(2, STRIPE_BASE), so for
 * STRIPE_BASE = 4, it is 16. Stripe identifiers are obtained by replacing first digit by every possible value of 
 * a digit and second digit by 8. So, if channelId is <0x1AB88..>, then stripe id's generated are <0x08B88..>,
 * <0x18B88..>, <0x28B88..> etc. 
 *
 * @version $Id: Channel.java 3613 2007-02-15 14:45:14Z jstewart $
 * @author Ansley Post
 * @author Alan Mislove
 * @author Atul Singh
 */
public class Channel {

  /**
   * The default number of stripes to create
   */
  private int stripeBase;// = 4;
  
  /**
   * ChannelId for this channel
   */
  protected ChannelId channelId;

  /**
   * The list of stripeIds for this channel
   */
  protected Stripe[] stripes;

  /**
   * The Id of the local node
   */
  protected Id localId;

  /**
   * Constructor to create a new channel from scratch
   *
   * @param channelId The Id of the channel
   * @param scribe The underlying stripe object
   * @param factory The Id factory
   * @param localId The local Id
   */
  public Channel(ChannelId channelId, Scribe scribe, String instance, IdFactory factory, Id localId, int stripeBase, int maxFailedSubscriptions) {

    /*
     *  Initialize Member variables
     */
    this.stripeBase = stripeBase;
    this.channelId = channelId;
    this.localId = localId;

    /*
     *  Create the stripe id and stripe arrays
     */
    StripeId[] stripeIds = generateStripeIds(channelId, factory);
    stripes = new Stripe[stripeIds.length];

    /*
     *  Create the stripes
     */
    for (int i = 0; i < stripeIds.length; i++) {
      stripes[i] = new Stripe(stripeIds[i], scribe, instance, this, maxFailedSubscriptions);
    }
  }

  /**
   * Gets the local node id.
   *
   * @return The local node id
   */
  public Id getLocalId(){
    return localId;
  }

  /**
   * Gets the channelId for this channel
   *
   * @return ChannelId the channelId for this channel
   */
  public ChannelId getChannelId() {
    return channelId;
  }

  /**
   * At any moment a node is subscribed to at least 1 but possibly more stripes. They will always be
   * subscribed to their primary Stripe.
   *
   * @return Vector the Stripes this node is subscribed to.
   */
  public Stripe[] getStripes() {
    return stripes;
  }

  /**
   * The primary stripe is the stripe that the user must have.
   *
   * @return Stripe The Stripe object that is the primary stripe.
   */
  protected Stripe getPrimaryStripe() {
    for(int i = 0; i < stripes.length; i++) {
      if (SplitStreamScribePolicy.getPrefixMatch(this.localId, stripes[i].getStripeId().getId(), stripeBase) > 0)
        return stripes[i];
    }
    
    return null;
  }

  public int getStripeBase() {
    return stripeBase;
  }
  
  /**
   * Creates and returns the Ids associated with the provided channelId
   *
   * @param channelId The id of the channel
   * @return The array of stripeIds based on this channelId
   */
  protected StripeId[] generateStripeIds(ChannelId id, IdFactory factory) {
    int num = (int) Math.pow(2, stripeBase);
    StripeId[] stripeIds = new StripeId[num];

    for (int i=0; i<num; i++) {
      byte[] array = id.getId().toByteArray();
      stripeIds[i] = new StripeId(factory.buildId(process(array, stripeBase, i)));
    }

    return stripeIds; 
  }

  /**
   * Returns a new array, of the same length as array, with the first
   * base bits set to represent num.
   *
   * @param array The input array
   * @param base The number of bits to replace
   * @param num The number to replace the first base bits with
   * @return A new array
   */
  private static byte[] process(byte[] array, int base, int num) {
    int length = array.length * 8;
    BigInteger bNum = new BigInteger(num + "");
    bNum = bNum.shiftLeft(length-base);
    BigInteger bArray = new BigInteger(1, switchEndian(array));
    
    for (int i=length-1; i>length-base-1; i--) {
      if (bNum.testBit(i)) {
        bArray = bArray.setBit(i);
      } else {
        bArray = bArray.clearBit(i);
      }
    }

    byte[] newArray = bArray.toByteArray();
    byte[] result = new byte[array.length];

    if (newArray.length <= array.length) {
      System.arraycopy(newArray, 0, result, result.length-newArray.length, newArray.length);
    } else {
      System.arraycopy(newArray, newArray.length-array.length, result, 0, array.length);
    }

    return switchEndian(result);
  }

  /**
   * Switches the endianness of the array
   *
   * @param array The array to switch
   * @return THe switched array
   */
  private static byte[] switchEndian(byte[] array) {
    byte[] result = new byte[array.length];

    for (int i=0; i<result.length; i++) {
      result[i] = array[result.length-1-i];
    }

    return result;
  }

}
