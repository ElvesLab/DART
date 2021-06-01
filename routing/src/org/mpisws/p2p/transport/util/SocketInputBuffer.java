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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.mpisws.p2p.transport.ClosedChannelException;
import org.mpisws.p2p.transport.P2PSocket;

import rice.p2p.commonapi.rawserialization.InputBuffer;

/**
 * An easy way to read a complete object in from a socket.  Wraps a Socket as an InputBuffer
 * Throws an InsufficientBytesExceptoin when there is not enough data available locally, but 
 * caches the data so you can attempt to deserialize the object from the beginning when there are
 * additional bytes.
 * 
 * Not thread safe!
 * 
 * The read operations will either:
 *   a) succeed
 *   b) throw a InsufficientBytesException, which you should probably retry later when there are more bytes (automatically calls reset)
 *   c) throw a ClosedChannelException, which means the socket was closed, 
 *   d) other IOException
 *   
 * If you don't complete reading the object, and want to start from the beginning of the cache, call reset().
 * 
 * If you complete reading the object, but want to reuse the SocketInputBuffer, call clear().
 * 
 * Note that the bytesRemaining() field always returns UNKNOWN because Java's socket api doesn't give us this information
 * 
 * To find the size of the cache call size().  This is the amount of usable bytes in the cache, not the capacity.
 * 
 * SocketInputBuffer automatically grows.
 * 
 * Implementation notes: readPtr/writePtr are operating on the same byte[] called cache
 * 
 * @author Jeff Hoye
 *
 */
public class SocketInputBuffer implements InputBuffer {
  P2PSocket socket;
  /**
   * readPtr/writePtr are essentially pointers to the byte[] cache.  
   * 
   * writePtr points to where we are writing data into the cache, from the socket
   * readPtr points to where the user is reading from, and is where we are reading in from the method call
   * 
   * we throw an InsufficientBytesException if the readPtr would cross past the writePtr 
   * (after we read what we could from the socket)
   */
  ByteBuffer readPtr, writePtr;  
  byte[] cache;
  ByteBuffer one, two, four, eight; // to reduce object allocation, reuse common sizes.  Lazily constructed
  int initialSize;
  
  DataInputStream dis;
  public SocketInputBuffer(P2PSocket socket) {
    this(socket,1024);
  }
  
  public SocketInputBuffer(P2PSocket socket, int size) {
    this.socket = socket;
    initialSize = size;
    cache = new byte[size];
    readPtr = ByteBuffer.wrap(cache);
    writePtr = ByteBuffer.wrap(cache);
    resetDis();
  }
  
  /**
   * Handle the case if DataInputStream.markSupported() is false.
   */
  private void resetDis() {
    if (dis != null && dis.markSupported()) {
      try {
        dis.reset();
      } catch (IOException ioe) {
        dis = null;
        resetDis();        
      }
      return;
    }
    dis = new DataInputStream(new InputStream() {
      
      @Override
      public int read(byte[] b) throws IOException {
        return readInternal(b);
      }
    
      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        return readInternal(b, off, len);
      }
    
      @Override
      public int read() throws IOException {
        return readInternal();
      }    
    });    
  }
  
  public int bytesRemaining() {
    return UNKNOWN;
  }

  /**
   * Resets the read pointer to the beginning of the cache.
   */
  public void reset() {
    readPtr.clear(); 
  }  
    
  /**
   * Note that this is not the number of bytes that can be read without throwing
   * an exception, since some of these bytes may already have been consumed.  
   * This is mostly useful for debugging.
   * 
   * @return the number of useful bytes in the cache
   */
  public int size() {
    return writePtr.position(); 
  }

  /**
   * 
   * 
   * @param b
   * @param off
   * @param len
   * @return the number of bytes read
   * @throws IOException
   */
  public int readInternal(byte[] b, int off, int len) throws IOException {
    int bytesToRead = needBytes(len, false);
    readPtr.get(b, off, bytesToRead);
    return bytesToRead;
  }

  /**
   * 
   * 
   * @param b
   * @return the number of bytes read
   * @throws IOException
   */
  public int readInternal(byte[] b) throws IOException {
    int bytesToRead = needBytes(b.length, false);
    readPtr.get(b, 0, bytesToRead);
    return bytesToRead;
  }

  /**
   * 
   * 
   * @return the value of one byte
   * @throws IOException
   */
  public int readInternal() throws IOException {
    needBytes(1, true);
    return (readPtr.get() & 0xFF);
  }


  public int read(byte[] b, int off, int len) throws IOException {
    return dis.read(b, off, len);
  }

  public int read(byte[] b) throws IOException {
    return dis.read(b);
  }

  public byte readByte() throws IOException {
    return dis.readByte();
  }  
  
  public boolean readBoolean() throws IOException {
    return dis.readBoolean();
  }

  public char readChar() throws IOException {
    return dis.readChar();
  }

  public double readDouble() throws IOException {
    return dis.readDouble();
  }

  public float readFloat() throws IOException {
    return dis.readFloat();    
  }

  public int readInt() throws IOException {
    return dis.readInt();
  }

  public long readLong() throws IOException {
    return dis.readLong();
  }

  public short readShort() throws IOException {
    return dis.readShort();
  }

  public String readUTF() throws IOException {
    return dis.readUTF();
  }

  /**
   * Returns the number of bytes available in the cache,
   * reads bytes from socket into cache if need be
   * 
   * @param num the number of bytes you need
   * @param fail true if you want it to throw an exception if there aren't enough bytes
   * @return the actual number of bytes read
   * @throws IOException
   */
  private int needBytes(int num, boolean fail) throws IOException {
    int bytesToReadIntoCache = num - (writePtr.position() - readPtr.position());
    if (bytesToReadIntoCache > 0) {
      readBytesIntoCache(bytesToReadIntoCache);
    }
    int ret = writePtr.position()-readPtr.position();
    if (ret > num) ret = num;
    if (fail && ret < num) {
      reset();
      throw new InsufficientBytesException(num, ret);
    }
    return ret;
  }
  
  /**
   * Reads this many bytes into the cache, grows the cache if needed
   * 
   * Increases the writeBB
   * 
   * @param num
   * @return
   * @throws IOException
   */
  private int readBytesIntoCache(int num) throws IOException {
    ByteBuffer in;
    switch(num) {
    case 0: 
      return 0;
    case 1:
      if (one == null) one = ByteBuffer.allocate(num);
      one.clear();      
      in = one;
      break;
    case 2:      
      if (two == null) two = ByteBuffer.allocate(num);
      two.clear();      
      in = two;
      break;
    case 4:      
      if (four == null) four = ByteBuffer.allocate(num);
      four.clear();      
      in = four;
      break;
    case 8:      
      if (eight == null) eight = ByteBuffer.allocate(num);
      eight.clear();      
      in = eight;
      break;
    default:
      in = ByteBuffer.allocate(num);      
    }
    
    int ret = (int)socket.read(in);
    if (ret == -1) throw new ClosedChannelException("Socket "+socket+" is already closed. (during read)"); 
    in.flip();
    while (writePtr.remaining() < ret) {
      grow(); 
    }
    writePtr.put(in);
    
    return ret;
  }
  
  /**
   * Clears the cache from memory, resetting it to the initial size.  This is a 
   * good thing to do after you read an object.  
   */
  public void clear() throws IOException {
    if (cache.length > initialSize)
      cache = new byte[initialSize];
    readPtr = ByteBuffer.wrap(cache);
    writePtr = ByteBuffer.wrap(cache);
    resetDis();
  }  
  
  private void grow() {
    byte[] newCache = new byte[cache.length];
    System.arraycopy(cache, 0, newCache, 0, cache.length);
    ByteBuffer newReadPtr = ByteBuffer.wrap(newCache);
    ByteBuffer newWritePtr = ByteBuffer.wrap(newCache);
    
    newReadPtr.position(readPtr.position()); 
    newWritePtr.position(writePtr.position()); 
    
    cache = newCache;
    readPtr = newReadPtr;
    writePtr = newWritePtr;
  }
}
