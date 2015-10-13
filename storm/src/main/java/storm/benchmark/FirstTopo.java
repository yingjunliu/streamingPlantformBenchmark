package storm.benchmark;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.*;

import java.util.UUID;

/**
 * Created by junjun on 2015/10/8.
 */
public class FirstTopo {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        BrokerHosts hosts = new ZkHosts("node219:2181");
        SpoutConfig spoutConfig = new SpoutConfig(hosts, "my.test", "/" + "my.test", UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        builder.setSpout("spout", kafkaSpout);
        builder.setBolt("bolt", new SenqueceBolt()).shuffleGrouping("spout");
        Config conf = new Config();
        conf.setDebug(false);
        if (args != null && args.length > 0) {
            //conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("firstTopo", conf, builder.createTopology());
            Utils.sleep(100000);
            cluster.killTopology("firstTopo");
            cluster.shutdown();
        }
    }
}
