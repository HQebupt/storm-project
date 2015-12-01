package com.alipay.dw.jstorm.example.sequence;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.alipay.dw.jstorm.example.sequence.bolt.TotalCount;
import com.alipay.dw.jstorm.example.sequence.spout.SequenceSpout;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SequenceTopology {

    public static void SetBuilder(TopologyBuilder builder, Map conf) {

        int spoutParal = Integer.valueOf(prop.getProperty("spout.parallel", "1"));
        builder.setSpout(SequenceTopologyDef.SEQUENCE_SPOUT_NAME,
            new SequenceSpout(), spoutParal);

        int boltParal = Integer.valueOf(prop.getProperty("bolt.parallel", "1"));
        builder.setBolt(SequenceTopologyDef.TOTAL_BOLT_NAME, new TotalCount(),
            boltParal).noneGrouping(SequenceTopologyDef.SEQUENCE_SPOUT_NAME); // noneGrouping和shuffleGrouping是基本一样的。

        conf.put(Config.TOPOLOGY_DEBUG, false);
        //        conf.put(ConfigExtension.TOPOLOGY_DEBUG_RECV_TUPLE, false);
        //        conf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        int ackerParal = Integer.valueOf(prop.getProperty("acker.parallel", "1"));
        conf.put(Config.TOPOLOGY_ACKERS, ackerParal);
        // conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 6);
        //        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 20);
        int pending = Integer.valueOf(prop.getProperty("msg.pending", "1"));
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, pending);

        int workerNum = Integer.valueOf(prop.getProperty("worker.num", "6"));
        conf.put(Config.TOPOLOGY_WORKERS, workerNum);

    }


    public static void SetRemoteTopology(String streamName)
        throws AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder builder = new TopologyBuilder();

        Map conf = new HashMap();

        SetBuilder(builder, conf);

        conf.put(Config.STORM_CLUSTER_MODE, "distributed");

        StormSubmitter.submitTopology(streamName, conf,
            builder.createTopology());

    }

    public static void SetDPRCTopology() throws AlreadyAliveException,
        InvalidTopologyException {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder(
            "exclamation");

        builder.addBolt(new TotalCount(), 3);

        Config conf = new Config();

        conf.setNumWorkers(3);
        StormSubmitter.submitTopology("rpc", conf,
            builder.createRemoteTopology());
    }

    private static Properties prop = new Properties();

    public static void readConf(String fileName) throws Exception {

        FileInputStream fileInputStream = new FileInputStream(fileName);

        prop.load(fileInputStream);

    }

    public static void main(String[] args) throws Exception {
        if (args == null || args.length == 0) {
            throw new RuntimeException("args is null.");
        }

        // 加载配置文件
        if (args.length == 2) {
            readConf(args[1]);
        }

        if (args[0] == "rpc") {// Run DRPC Topology
            SetDPRCTopology();
        } else { // Run SequenceTopology
            SetRemoteTopology(args[0]);
        }

    }
}
