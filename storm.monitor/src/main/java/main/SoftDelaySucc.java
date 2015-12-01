package main;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.DelayAverage;
import bolts.DelayInterfaceSplit;
import bolts.InterfacePVSuccessRate;
import bolts.SoftDelayRatio;
import org.apache.log4j.Logger;
import spouts.FilesRead1min;
import spouts.Signal15min;
import util.FName;
import util.StreamId;

public class SoftDelaySucc {
    static Logger log = Logger.getLogger(SoftDelaySucc.class);

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        String dataPath = StormConf.DELAYPATH;
        String[] tableName = StormConf.DELAYSUCCTABLE;
        if (tableName.length != 3) {
            log.error("the tableName needed 3,but there is " + tableName.length
                + ".please check the configure file "
                + "argsTopo.properties" + ": delaySuccTable "
                + "\nLine:15 or so");
            return;
        }
        builder.setSpout(ID.filesSpout.name(), new FilesRead1min(dataPath), 1);
        builder.setSpout(ID.signal15m.name(), new Signal15min());
        builder.setBolt(ID.SPLIT.name(), new DelayInterfaceSplit(), 4)
            .shuffleGrouping(ID.filesSpout.name());

        // 时延平均值
        builder.setBolt(ID.delay.name(), new DelayAverage(tableName[0]), 4)
            .fieldsGrouping(ID.SPLIT.name(),
                new Fields(FName.ACTION.name()))
            .allGrouping(ID.signal15m.name(), StreamId.SIGNAL15MIN.name());

        // PV成功率
        builder.setBolt(ID.pvSuc.name(),
            new InterfacePVSuccessRate(tableName[1]), 4)
            .fieldsGrouping(ID.SPLIT.name(),
                new Fields(FName.ACTION.name()))
            .allGrouping(ID.signal15m.name(), StreamId.SIGNAL15MIN.name());

        // 时延平均值
        builder.setBolt(ID.delayRatio.name(), new SoftDelayRatio(tableName[2]), 4)
            .fieldsGrouping(ID.SPLIT.name(),
                new Fields(FName.ACTION.name()))
            .allGrouping(ID.signal15m.name(), StreamId.SIGNAL15MIN.name());


        Config conf = new Config();
        conf.setNumWorkers(3);
        StormSubmitter.submitTopology(StormConf.DELAYSUCCTOPO, conf,
            builder.createTopology());
    }
}
