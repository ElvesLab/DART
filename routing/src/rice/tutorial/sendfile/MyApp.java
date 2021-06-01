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
 * Created on Feb 15, 2005
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package rice.tutorial.sendfile;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.mpisws.p2p.filetransfer.BBReceipt;
import org.mpisws.p2p.filetransfer.FileReceipt;
import org.mpisws.p2p.filetransfer.FileTransfer;
import org.mpisws.p2p.filetransfer.FileTransferCallback;
import org.mpisws.p2p.filetransfer.FileTransferImpl;
import org.mpisws.p2p.filetransfer.FileTransferListener;
import org.mpisws.p2p.filetransfer.Receipt;

import rice.Continuation;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.appsocket.*;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * A very simple application.
 * 
 * @author Jeff Hoye
 */
public class MyApp implements Application {
  /**
   * The Endpoint represents the underlieing node.  By making calls on the 
   * Endpoint, it assures that the message will be delivered to a MyApp on whichever
   * node the message is intended for.
   */
  protected Endpoint endpoint;
  
  /**
   * The node we were constructed on.
   */
  protected Node node;

  protected FileTransfer fileTransfer;
    
  public MyApp(Node node, final IdFactory factory) {
    // register the endpoint
    this.endpoint = node.buildEndpoint(this, "myinstance");
    this.node = node;
    
    // example receiver interface
    endpoint.accept(new AppSocketReceiver() {
      /**
       * When we accept a new socket.
       */
      public void receiveSocket(AppSocket socket) {
        fileTransfer = new FileTransferImpl(socket,new FileTransferCallback() {
        
          public void messageReceived(ByteBuffer bb) {
            System.out.println("Message received: "+bb);
          }
        
          public void fileReceived(File f, ByteBuffer metadata) {
            try {
              String originalFileName = new SimpleInputBuffer(metadata).readUTF();
              File dest = new File("delme2.txt");
              System.out.println("Moving "+f+" to "+dest+" original:"+originalFileName);
              System.out.println(f.renameTo(dest));
            } catch (IOException ioe) {
              System.out.println("Error deserializing file name. "+ioe);
            }
          }
        
          public void receiveException(Exception ioe) {
            System.out.println("FTC.receiveException() "+ioe);
          }
        },MyApp.this.node.getEnvironment());
        
        fileTransfer.addListener(new MyFileListener());
        
        // it's critical to call this to be able to accept multiple times
        endpoint.accept(this);
      }    

      /**
       * Called when the socket is ready for reading or writing.
       */
      public void receiveSelectResult(AppSocket socket, boolean canRead, boolean canWrite) {
        throw new RuntimeException("Shouldn't be called.");
      }
    
      /**
       * Called if we have a problem.
       */
      public void receiveException(AppSocket socket, Exception e) {
        e.printStackTrace();
      }    
    });
    
    // register after we have set the AppSocketReceiver
    endpoint.register();
  }

  /**
   * This listener just prints every time a method is called.  It uses the incoming
   * flag to specify Downloaded/Uploaded.
   * 
   * @author Jeff Hoye
   *
   */
  class MyFileListener implements FileTransferListener {
    public void fileTransferred(FileReceipt receipt,
        long bytesTransferred, long total, boolean incoming) {
      String s;
      if (incoming) {
        s = " Downloaded ";
      } else {
        s = " Uploaded ";              
      }
      double percent = 100.0*bytesTransferred/total;
      System.out.println(MyApp.this+s+percent+"% of "+receipt);
    }

    public void msgTransferred(BBReceipt receipt, int bytesTransferred,
        int total, boolean incoming) {
      String s;
      if (incoming) {
        s = " Downloaded ";
      } else {
        s = " Uploaded ";              
      }
      double percent = 100.0*bytesTransferred/total;
      System.out.println(MyApp.this+s+percent+"% of "+receipt);
    }

    public void transferCancelled(Receipt receipt, boolean incoming) {
      String s;
      if (incoming) {
        s = "download";
      } else {
        s = "upload";              
      }
      System.out.println(MyApp.this+": Cancelled "+s+" of "+receipt);
    }
    
    public void transferFailed(Receipt receipt, boolean incoming) {
      String s;
      if (incoming) {
        s = "download";
      } else {
        s = "upload";              
      }
      System.out.println(MyApp.this+": Transfer Failed "+s+" of "+receipt);
    }
  }
  
  /**
   * Getter for the node.
   */
  public Node getNode() {
    return node;
  }
  
  /**
   * Called to directly send a message to the nh
   */
  public void sendMyMsgDirect(NodeHandle nh) {
    System.out.println(this+" opening to "+nh);    
    endpoint.connect(nh, new AppSocketReceiver() {
      
      /**
       * Called when the socket comes available.
       */
      public void receiveSocket(AppSocket socket) {        
        // create the FileTransfer object
        FileTransfer sender = new FileTransferImpl(socket, null, node.getEnvironment());         
        
        // add the listener
        sender.addListener(new MyFileListener());
       
        // Create a simple 4 byte message
        ByteBuffer sendMe = ByteBuffer.allocate(4);
        sendMe.put((byte)1);
        sendMe.put((byte)2);
        sendMe.put((byte)3);
        sendMe.put((byte)4);
        
        // required when using a byteBuffer to both read and write
        sendMe.flip();
        
        // Send the message
        System.out.println("Sending "+sendMe);        
        sender.sendMsg(sendMe, (byte)1, null);
        
        try {
          // get the file
          final File f = new File("delme.txt");
          
          // make sure it exists
          if (!f.exists()) {
            System.err.println("File "+f+" does not exist.  Please create a file called "+f+" and run the tutorial again.");
            System.exit(1);
          }
          
          // serialize the filename
          SimpleOutputBuffer sob = new SimpleOutputBuffer();
          sob.writeUTF(f.getName());
          
          // request transfer of the file with priority 2
          sender.sendFile(f,sob.getByteBuffer(),(byte)2,new Continuation<FileReceipt, Exception>() {

            public void receiveException(Exception exception) {
              System.out.println("Error sending: "+f+" "+exception);
            }

            public void receiveResult(FileReceipt result) {
              System.out.println("Send complete: "+result);
            }
          });
          
        } catch (IOException ioe) {
          ioe.printStackTrace();
        }
      }    

      /**
       * Called if there is a problem.
       */
      public void receiveException(AppSocket socket, Exception e) {
        e.printStackTrace();
      }
      
      /**
       * Example of how to write some bytes
       */
      public void receiveSelectResult(AppSocket socket, boolean canRead, boolean canWrite) {   
        throw new RuntimeException("Shouldn't be called.");
      }
    }, 30000);
  }
    
  /**
   * Called when we receive a message.
   */
  public void deliver(Id id, Message message) {
    System.out.println(this+" received "+message);
  }

  /**
   * Called when you hear about a new neighbor.
   * Don't worry about this method for now.
   */
  public void update(NodeHandle handle, boolean joined) {
  }
  
  /**
   * Called a message travels along your path.
   * Don't worry about this method for now.
   */
  public boolean forward(RouteMessage message) {
    return true;
  }
  
  public String toString() {
    return "MyApp "+endpoint.getId();
  }

}
