package main;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.*;
import org.apache.log4j.Logger;
import spouts.FilesReadPeriod;
import spouts.Signal15min;
import spouts.Signal24h;
import spouts.SignalDB;
import util.FName;
import util.StreamId;

public class CellOrder {
    static Logger log = Logger.getLogger(CellOrder.class);

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        String dataPath = StormConf.CELLPATH;
        String tableName[] = StormConf.CELLTABLE;
        if (tableName.length != 2) {
            log.error("the tableName needed 2,but there is " + tableName.length
                + ".please check the configure file "
                + "argsTopo.properties" + ": cellTable "
                + "\nLine:46 or so");
            return;
        }
        builder.setSpout(ID.filesSpout.name(), new FilesReadPeriod(dataPath), 1);
        builder.setSpout(ID.signalDB.name(), new SignalDB());
        builder.setSpout(ID.signal15m.name(), new Signal15min());
        builder.setSpout(ID.signal24H.name(), new Signal24h());
        builder.setBolt(ID.SPLIT.name(), new CellSplit(), 8).shuffleGrouping(
            ID.filesSpout.name());

        // 总UV
        builder.setBolt(ID.userClean.name(), new CellUserClean(), 4)
            .fieldsGrouping(ID.SPLIT.name(),
                new Fields(FName.MSISDN.name()))
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.totalUser.name(), new Total(), 1)
            .shuffleGrouping(ID.userClean.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.totalUserDB.name(), new TotalDB(tableName[0]), 1)
            .shuffleGrouping(ID.totalUser.name())
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        // Order fee
        builder.setBolt(ID.fee.name(), new CellFeeSum(), 1)
            .shuffleGrouping(ID.SPLIT.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.feeDB.name(), new TotalDB(tableName[1]), 1)
            .shuffleGrouping(ID.fee.name())
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        Config conf = new Config();
        conf.setNumWorkers(2);
        StormSubmitter.submitTopology(StormConf.CELLTOPO, conf,
            builder.createTopology());
    }
}
