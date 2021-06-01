import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class MainTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("IntegerSpout", new IntegerSpout());
        builder.setBolt("MultiplierBolt", new MultiplierBolt()).shuffleGrouping("IntegerSpout");

        Config config = new Config();
        config.setDebug(false);

        //StormSubmitter.submitTopology("HelloTopology", config, builder.createTopology());

        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("HelloTopology", config, builder.createTopology());
            Thread.sleep(5000);
        } catch (Exception e){
            e.printStackTrace();
        }
        finally {
            cluster.shutdown();
        }
    }
}
