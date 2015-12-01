package main;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.*;
import org.apache.log4j.Logger;
import spouts.FilesSpoutUnique;
import spouts.SignalUnique;
import spouts.SignalUpdate;
import util.FName;
import util.StreamId;

public class OrdUvUniqueTopo {
    static Logger log = Logger.getLogger(OrdUvUniqueTopo.class);

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        String dataPath = StormConf.ORDPATH;
        String tableName[] = StormConf.ORDUNITABLE;
        if (tableName.length != 2) {
            log.error("the tableName needed 2,but there is " + tableName.length
                + ".please check the configure file "
                + "argsTopo.properties" + ": ordUniTable "
                + "\nLine:51 or so");
            return;
        }
        builder.setSpout(ID.filesSpout.name(), new FilesSpoutUnique(dataPath),
            1);
        builder.setSpout(ID.signalUnique.name(), new SignalUnique());
        builder.setSpout(ID.signalUp.name(), new SignalUpdate());
        builder.setBolt(ID.SPLIT.name(), new OrderSplit(), 4).shuffleGrouping(
            ID.filesSpout.name());

        builder.setBolt(ID.clnMoblie.name(), new ClnMobile(), 1)
            .fieldsGrouping(ID.SPLIT.name(), StreamId.ORDER.name(),
                new Fields(FName.PRODUCT_ID.name()))
            .allGrouping(ID.signalUp.name(), StreamId.SIGNALUPDATE.name());

        // 总付费用户数
        builder.setBolt(ID.userClean.name(), new OrderUserClean(), 4)
            .fieldsGrouping(ID.clnMoblie.name(),
                new Fields(FName.MSISDN.name()))
            .allGrouping(ID.signalUnique.name(),
                StreamId.SIGNALUNIQUE.name());

        builder.setBolt(ID.totalUser.name(), new OrdUVUnique(tableName[0]), 1)
            .shuffleGrouping(ID.userClean.name())
            .allGrouping(ID.signalUnique.name(),
                StreamId.SIGNALUNIQUE.name());

        // 分平台用户数
        builder.setBolt(ID.plaUser.name(), new OrdPlatUVUnique(tableName[1]), 4)
            .fieldsGrouping(ID.userClean.name(),
                new Fields(FName.PLATFORM.name()))
            .allGrouping(ID.signalUnique.name(),
                StreamId.SIGNALUNIQUE.name());

        Config conf = new Config();
        conf.setNumWorkers(4);
        StormSubmitter.submitTopology(StormConf.ORDUNITOPO, conf,
            builder.createTopology());
    }
}
