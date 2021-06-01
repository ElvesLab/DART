import com.backblaze.erasure.ReedSolomon;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.past.Past;
import rice.p2p.past.PastContent;
import rice.p2p.past.PastImpl;
import rice.pastry.NodeIdFactory;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeFactory;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.standard.RandomNodeIdFactory;
import rice.persistence.LRUCache;
import rice.persistence.MemoryStorage;
import rice.persistence.Storage;
import rice.persistence.StorageManagerImpl;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Vector;

public class SampleEncoder {

    public static final int DATA_SHARDS = 4;
    public static final int PARITY_SHARDS = 2;
    public static final int TOTAL_SHARDS = 6;

    public static final int BYTES_IN_INT = 4;

    public static String inputFileName;

    //[PL20190911] PAST Logic
    /**
     * this will keep track of our Past applications
     */
    static Vector apps = new Vector();

    public static void PastTutorial(int bindport, InetSocketAddress bootaddress,
                        int numNodes, final Environment env) throws Exception {

        // Generate the NodeIds Randomly
        NodeIdFactory nidFactory = new RandomNodeIdFactory(env);

        // construct the PastryNodeFactory, this is how we use rice.pastry.socket
        PastryNodeFactory factory = new SocketPastryNodeFactory(nidFactory,
                bindport, env);

        // loop to construct the nodes/apps
        for (int curNode = 0; curNode < numNodes; curNode++) {
            // This will return null if we there is no node at that location
            NodeHandle bootHandle = ((SocketPastryNodeFactory) factory)
                    .getNodeHandle(bootaddress);

            // construct a node, passing the null boothandle on the first loop will
            // cause the node to start its own ring
            PastryNode node = factory.newNode((rice.pastry.NodeHandle) bootHandle);

            // the node may require sending several messages to fully boot into the ring
            synchronized(node) {
                while(!node.isReady() && !node.joinFailed()) {
                    // delay so we don't busy-wait
                    node.wait(500);

                    // abort if can't join
                    if (node.joinFailed()) {
                        throw new IOException("Could not join the FreePastry ring.  Reason:"+node.joinFailedReason());
                    }
                }
            }

            System.out.println("Finished creating new node " + node);


            // used for generating PastContent object Ids.
            // this implements the "hash function" for our DHT
            PastryIdFactory idf = new PastryIdFactory(env);

            // create a different storage root for each node
            String storageDirectory = "./storage"+node.getId().hashCode();

            // create the persistent part
            //      Storage stor = new PersistentStorage(idf, storageDirectory, 4 * 1024 * 1024, node
            //          .getEnvironment());
            Storage stor = new MemoryStorage(idf);
            Past app = new PastImpl(node, new StorageManagerImpl(idf, stor, new LRUCache(
                    new MemoryStorage(idf), 512 * 1024, node.getEnvironment())), 3, "");

            apps.add(app);
        }

        // wait 5 seconds
        env.getTimeSource().sleep(5000);

        // We could cache the idf from whichever app we use, but it doesn't matter
        PastryIdFactory localFactory = new PastryIdFactory(env);

        // Store 5 keys
        // let's do the "put" operation
        System.out.println("Storing 6 keys");
        Id[] storedKey = new Id[6];//[PL20190911]We need to store 6 files, which 4 original and 2 parity.
        for(int ctr = 0; ctr < storedKey.length; ctr++) {
            // these variables are final so that the continuation can access them
            // final String s = "test" + env.getRandomSource().nextInt();
            // final String s = new String(Files.readAllBytes(Paths.get("test.txt." + ctr)), "UTF-8");
            final String s = inputFileName + "." + ctr;
            // build the past content
            // final PastContent myContent = new MyPastContent(localFactory.buildId(s), s);
            // final PastContent myContent = new MyPastContent(localFactory.buildId(s), new FileInputStream("test.txt." + ctr));
            final PastContent myContent = new MyPastContent(localFactory.buildId(s), Files.readAllBytes(Paths.get(inputFileName + "." + ctr)));
            // store the key for a lookup at a later point
            storedKey[ctr] = myContent.getId();

            // pick a random past appl on a random node
            Past p = (Past)apps.get(env.getRandomSource().nextInt(numNodes));
            System.out.println("Inserting " + myContent + " at node "+p.getLocalNodeHandle());

            // insert the data
            p.insert(myContent, new Continuation() {
                // the result is an Array of Booleans for each insert
                public void receiveResult(Object result) {
                    Boolean[] results = ((Boolean[]) result);
                    int numSuccessfulStores = 0;
                    for (int ctr = 0; ctr < results.length; ctr++) {
                        if (results[ctr].booleanValue())
                            numSuccessfulStores++;
                    }
                    System.out.println(myContent + " successfully stored at " +
                            numSuccessfulStores + " locations.");
                }

                public void receiveException(Exception result) {
                    System.out.println("Error storing "+myContent);
                    result.printStackTrace();
                }
            });
        }

        // wait 5 seconds
        env.getTimeSource().sleep(5000);

//        // let's do the "get" operation
//        System.out.println("Looking up the 6 keys");
//
//        // for each stored key
//        for (int ctr = 0; ctr < storedKey.length; ctr++) {
//            final Id lookupKey = storedKey[ctr];
//
//            // pick a random past appl on a random node
//            Past p = (Past)apps.get(env.getRandomSource().nextInt(numNodes));
//
//            System.out.println("Looking up " + lookupKey + " at node "+p.getLocalNodeHandle());
//            p.lookup(lookupKey, new Continuation() {
//                public void receiveResult(Object result) {
//                    MyPastContent myPastContent = (MyPastContent) result;
//                    System.out.println("Successfully looked up " + myPastContent.toString() + " for key "+lookupKey+".");
//                }
//
//                public void receiveException(Exception result) {
//                    System.out.println("Error looking up "+lookupKey);
//                    result.printStackTrace();
//                }
//            });
//        }
//
//        // wait 5 seconds
//        env.getTimeSource().sleep(5000);
//
//        // now lets see what happens when we do a "get" when there is nothing at the key
//        System.out.println("Looking up a bogus key");
//        final Id bogusKey = localFactory.buildId("bogus");
//
//        // pick a random past appl on a random node
//        Past p = (Past)apps.get(env.getRandomSource().nextInt(numNodes));
//
//        System.out.println("Looking up bogus key " + bogusKey + " at node "+p.getLocalNodeHandle());
//        p.lookup(bogusKey, new Continuation() {
//            public void receiveResult(Object result) {
//                System.out.println("Successfully looked up " + result + " for key "+bogusKey+".  Notice that the result is null.");
//                env.destroy();
//            }
//
//            public void receiveException(Exception result) {
//                System.out.println("Error looking up "+bogusKey);
//                result.printStackTrace();
//                env.destroy();
//            }
//        });
    }

    public SampleEncoder(String fileName, String localbindport, String bootIP, String bootPort, String numNodesString) throws Exception  {

        final File inputFile = new File(fileName);
        if (!inputFile.exists()) {
            System.out.println("Cannot read input file: " + inputFile);
            return;
        }

        inputFileName = fileName;

        // Get the size of the input file.  (Files bigger that
        // Integer.MAX_VALUE will fail here!)
        final int fileSize = (int) inputFile.length();

        // Figure out how big each shard will be.  The total size stored
        // will be the file size (8 bytes) plus the file.
        final int storedSize = fileSize + BYTES_IN_INT;
        final int shardSize = (storedSize + DATA_SHARDS - 1) / DATA_SHARDS;

        // Create a buffer holding the file size, followed by
        // the contents of the file.
        final int bufferSize = shardSize * DATA_SHARDS;
        final byte [] allBytes = new byte[bufferSize];
        ByteBuffer.wrap(allBytes).putInt(fileSize);
        InputStream in = new FileInputStream(inputFile);
        int bytesRead = in.read(allBytes, BYTES_IN_INT, fileSize);
        if (bytesRead != fileSize) {
            throw new IOException("not enough bytes read");
        }
        in.close();

        // Make the buffers to hold the shards.
        byte [] [] shards = new byte [TOTAL_SHARDS] [shardSize];

        // Fill in the data shards
        for (int i = 0; i < DATA_SHARDS; i++) {
            System.arraycopy(allBytes, i * shardSize, shards[i], 0, shardSize);
        }

        // Use Reed-Solomon to calculate the parity.
        ReedSolomon reedSolomon = ReedSolomon.create(DATA_SHARDS, PARITY_SHARDS);
        reedSolomon.encodeParity(shards, 0, shardSize);

        // Write out the resulting files.
        for (int i = 0; i < TOTAL_SHARDS; i++) {
            File outputFile = new File(
                    inputFile.getParentFile(),
                    inputFile.getName() + "." + i);
            OutputStream out = new FileOutputStream(outputFile);
            out.write(shards[i]);
            out.close();
            System.out.println("wrote " + outputFile);
        }
        // Loads pastry configurations
        Environment env = new Environment();

        // disable the UPnP setting (in case you are testing this on a NATted LAN)
        env.getParameters().setString("nat_search_policy","never");

        try {
            // the port to use locally
            int bindport = Integer.parseInt(localbindport);

            // build the bootaddress from the command line args
            InetAddress bootaddr = InetAddress.getByName(bootIP);
            int bootport = Integer.parseInt(bootPort);
            InetSocketAddress bootaddress = new InetSocketAddress(bootaddr, bootport);

            // the port to use locally
            int numNodes = Integer.parseInt(numNodesString);

            // launch our node!
            PastTutorial(bindport, bootaddress, numNodes, env);
        } catch (Exception e) {
            // remind user how to use
            System.out.println("Main function exception happened!");
            throw e;
        }
    }
}
