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

public class SaveStateToPAST {

    public static String inputFileName;

    //[PL20190911] PAST Logic
    /**
     * this will keep track of our Past applications
     */
    static Vector apps = new Vector();

    /**
     * Based on the rice.tutorial.scribe.ScribeTutorial
     *
     * This constructor launches numNodes PastryNodes. They will bootstrap to an
     * existing ring if one exists at the specified location, otherwise it will
     * start a new ring.
     *
     * @param bindport the local port to bind to
     * @param bootaddress the IP:port of the node to boot from
     * @param numNodes the number of nodes to create in this JVM
     * @param env the Environment
     */
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

        // let's do the "put" operation
        System.out.println("Storing File");
        Id[] storedKey = new Id[1];//[PL20190911]We need to store 1 file.
        for(int ctr = 0; ctr < storedKey.length; ctr++) {
            // these variables are final so that the continuation can access them
            final String s = inputFileName;
            // build the past content
            // final PastContent myContent = new MyPastContent(localFactory.buildId(s), s);
            // final PastContent myContent = new MyPastContent(localFactory.buildId(s), new FileInputStream("test.txt." + ctr));
            final PastContent myContent = new MyPastContent(localFactory.buildId(s), Files.readAllBytes(Paths.get(inputFileName)));
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

    }
    public SaveStateToPAST(String fileName, String localbindport, String bootIP, String bootPort, String numNodesString) throws Exception  {

        final File inputFile = new File(fileName);
        if (!inputFile.exists()) {
            System.out.println("Cannot read input file: " + inputFile);
            return;
        }

        inputFileName = fileName;

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
