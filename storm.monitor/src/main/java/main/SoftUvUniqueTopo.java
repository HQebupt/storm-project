package main;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.InterfaceSplit;
import bolts.SoftUVUnique;
import org.apache.log4j.Logger;
import spouts.FilesSpoutUnique;
import spouts.SignalUnique;
import util.FName;
import util.StreamId;

public class SoftUvUniqueTopo {
    static Logger log = Logger.getLogger(SoftUvUniqueTopo.class);

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        String dataPath = StormConf.SOFTPATH;
        String tableName[] = StormConf.SOFTUNITABLE;
        if (tableName.length != 1) {
            log.error("the tableName needed 1,but there is " + tableName.length
                + ".please check the configure file "
                + "argsTopo.properties" + ": softUniTable "
                + "\nLine:59 or so");
            return;
        }
        builder.setSpout(ID.filesSpout.name(), new FilesSpoutUnique(dataPath),
            1);
        builder.setSpout(ID.signalUnique.name(), new SignalUnique());
        builder.setBolt(ID.SPLIT.name(), new InterfaceSplit(), 4)
            .shuffleGrouping(ID.filesSpout.name());

        // 用户去重：总用户数
        builder.setBolt(ID.userClean.name(), new SoftUVUnique(tableName[0]), 1)
            .fieldsGrouping(ID.SPLIT.name(), StreamId.INTERFACEDATA.name(),
                new Fields(FName.MSISDN.name()))
            .allGrouping(ID.signalUnique.name(),
                StreamId.SIGNALUNIQUE.name());

        Config conf = new Config();
        conf.setNumWorkers(2);
        StormSubmitter.submitTopology(StormConf.SOFTUNITOPO, conf,
            builder.createTopology());
    }
}
