package main;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.InterfaceSplit;
import bolts.TotalPvUvSplit;
import bolts.TotalUVUnique;
import bolts.WapSplit;
import org.apache.log4j.Logger;
import spouts.FilesSpoutUnique;
import spouts.SignalUnique;
import util.FName;
import util.StreamId;

public class TotalUvUniqueTopo {
    static Logger log = Logger.getLogger(TotalUvUniqueTopo.class);

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        String[] dataPath = {StormConf.UESPATH, StormConf.SOFTPATH,
            StormConf.PAGEPATH, StormConf.CLIENTPATH, StormConf.SNSPATH};
        String tableName[] = StormConf.TOTALUNITABLE;
        if (tableName.length != 1) {
            log.error("the tableName needed 1,but there is " + tableName.length
                + ".please check the configure file "
                + "argsTopo.properties" + ": totalUniTable "
                + "\nLine:55 or so");
            return;
        }

        builder.setSpout(ID.wapFiles.name(), new FilesSpoutUnique(dataPath[0]),
            1);
        builder.setBolt(ID.wapSplit.name(), new WapSplit(), 2).shuffleGrouping(
            ID.wapFiles.name());
        builder.setSpout(ID.softFiles.name(),
            new FilesSpoutUnique(dataPath[1]), 1);
        builder.setBolt(ID.softSplit.name(), new InterfaceSplit(), 2)
            .shuffleGrouping(ID.softFiles.name());
        builder.setSpout(ID.pgvtFiles.name(),
            new FilesSpoutUnique(dataPath[2]), 1);
        builder.setBolt(ID.pgvtSplit.name(), new TotalPvUvSplit(), 2)
            .shuffleGrouping(ID.pgvtFiles.name());
        builder.setSpout(ID.cdwFiles.name(), new FilesSpoutUnique(dataPath[3]),
            1);
        builder.setBolt(ID.cdwSplit.name(), new TotalPvUvSplit(), 2)
            .shuffleGrouping(ID.cdwFiles.name());
        builder.setSpout(ID.snsFiles.name(), new FilesSpoutUnique(dataPath[4]),
            1);
        builder.setBolt(ID.snsSplit.name(), new TotalPvUvSplit(), 2)
            .shuffleGrouping(ID.snsFiles.name());

        // 信号
        builder.setSpout(ID.signalUnique.name(), new SignalUnique());

        // 1小时的总用户数
        builder.setBolt(ID.totalUser.name(), new TotalUVUnique(tableName[0]), 1)
            .fieldsGrouping(ID.wapSplit.name(),
                new Fields(FName.MSISDN.name()))
            .fieldsGrouping(ID.softSplit.name(),
                StreamId.INTERFACEDATA.name(),
                new Fields(FName.MSISDN.name()))
            .fieldsGrouping(ID.pgvtSplit.name(),
                new Fields(FName.MSISDN.name()))
            .fieldsGrouping(ID.cdwSplit.name(),
                new Fields(FName.MSISDN.name()))
            .fieldsGrouping(ID.snsSplit.name(),
                new Fields(FName.MSISDN.name()))
            .allGrouping(ID.signalUnique.name(),
                StreamId.SIGNALUNIQUE.name());

        Config conf = new Config();
        conf.setNumWorkers(4);
        StormSubmitter.submitTopology(StormConf.TOTALUNITOPO, conf,
            builder.createTopology());
    }
}
