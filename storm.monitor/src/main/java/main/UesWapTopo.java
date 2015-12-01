package main;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.*;
import org.apache.log4j.Logger;
import spouts.*;
import util.FName;
import util.StreamId;

public class UesWapTopo {
    static Logger log = Logger.getLogger(UesWapTopo.class);

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        String dataPath = StormConf.UESPATH;
        String tableName[] = StormConf.UESTABLE;
        if (tableName.length != 9) {
            log.error("the tableName needed 9,but there is " + tableName.length
                + ".please check the configure file "
                + "argsTopo.properties" + ": uesTable " + "\nLine:20 or so");
            return;
        }
        builder.setSpout(ID.filesSpout.name(), new FilesReadPeriod(dataPath), 1);
        builder.setSpout(ID.signalDB.name(), new SignalDB());
        builder.setSpout(ID.signal1m.name(), new Signal1min());
        builder.setSpout(ID.signal15m.name(), new Signal15min());
        builder.setSpout(ID.signal24H.name(), new Signal24h());
        builder.setBolt(ID.SPLIT.name(), new WapSplit(), 4).shuffleGrouping(
            ID.filesSpout.name());

        // 总PV
        builder.setBolt(ID.totalPV.name(), new Total(), 1)
            .shuffleGrouping(ID.SPLIT.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.totalPVDB.name(), new TotalDB(tableName[0]), 1)
            .shuffleGrouping(ID.totalPV.name())
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        // 分省PV
        builder.setBolt(ID.proCln.name(), new ProClean(), 1)// 分省clean
            .shuffleGrouping(ID.SPLIT.name());
        builder.setBolt(ID.proPV.name(), new ProvinceItem(), 2)
            .fieldsGrouping(ID.proCln.name(),
                new Fields(FName.PROVINCE_ID.name()))
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.proPVDB.name(), new ProvinceItemDB(tableName[1]), 2)
            .fieldsGrouping(ID.proPV.name(),
                new Fields(FName.PROVINCE_ID.name()))
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        // 秒PV
        builder.setBolt(ID.pvByTime.name(), new PVByTime(), 1)
            .shuffleGrouping(ID.SPLIT.name())
            .allGrouping(ID.signal1m.name(), StreamId.SIGNAL1MIN.name());
        builder.setBolt(ID.pvByTimeDB.name(), new PVByTimeDB(tableName[2]), 1)
            .shuffleGrouping(ID.pvByTime.name())
            .allGrouping(ID.signal15m.name(), StreamId.SIGNAL15MIN.name());

        // 用户去重：总、分省用户数
        builder.setBolt(ID.userClean.name(), new WapUserClean(), 2)
            .fieldsGrouping(ID.SPLIT.name(),
                new Fields(FName.MSISDN.name()))
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.totalUser.name(), new Total(), 1)
            .shuffleGrouping(ID.userClean.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.totalUserDB.name(), new TotalDB(tableName[3]), 1)
            .shuffleGrouping(ID.totalUser.name())
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        // 分省clean
        builder.setBolt(ID.proClnUV.name(), new ProClean(), 1).shuffleGrouping(
            ID.userClean.name());
        builder.setBolt(ID.proUser.name(), new ProvinceItem(), 2)
            .fieldsGrouping(ID.proClnUV.name(),
                new Fields(FName.PROVINCE_ID.name()))
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.proUserDB.name(), new ProvinceItemDB(tableName[4]),
            2)
            .fieldsGrouping(ID.proUser.name(),
                new Fields(FName.PROVINCE_ID.name()))
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        // PV占比
        builder.setBolt(ID.pvRate.name(), new PVRate(tableName[5]), 1)
            .fieldsGrouping(ID.SPLIT.name(),
                new Fields(FName.PAGENAME.name()))
            .allGrouping(ID.signal15m.name(), StreamId.SIGNAL15MIN.name());
        // PV成功率
        builder.setBolt(ID.pvSuc.name(), new WapPVSuccessRate(tableName[6]), 2)
            .fieldsGrouping(ID.SPLIT.name(),
                new Fields(FName.PAGENAME.name()))
            .allGrouping(ID.signal15m.name(), StreamId.SIGNAL15MIN.name());
        // 时延平均值
        builder.setBolt(ID.delay.name(), new WapDelayAve(tableName[7]), 2)
            .fieldsGrouping(ID.SPLIT.name(),
                new Fields(FName.PAGENAME.name()))
            .allGrouping(ID.signal15m.name(), StreamId.SIGNAL15MIN.name());
        // 时延占比
        builder.setBolt(ID.delayRatio.name(), new WapDelayRatio(tableName[8]), 4)
            .fieldsGrouping(ID.SPLIT.name(), new Fields(FName.PAGENAME.name()))
            .allGrouping(ID.signal15m.name(), StreamId.SIGNAL15MIN.name());

        Config conf = new Config();
        conf.setNumWorkers(8);
        StormSubmitter.submitTopology(StormConf.UESTOPO, conf,
            builder.createTopology());
    }
}
