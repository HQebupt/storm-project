package main;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.*;
import org.apache.log4j.Logger;
import spouts.FilesRead1min;
import spouts.Signal24h;
import spouts.SignalDB;
import util.FName;
import util.StreamId;

//与TotalPVUV不同的是，这个过滤的并行度是1，为了话单的2个字段。
public class TotalProPVUV {
    static Logger log = Logger.getLogger(TotalProPVUV.class);

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        String[] dataPath = {StormConf.UESPATH, StormConf.SOFTPATH,
            StormConf.PAGEPATH, StormConf.CLIENTPATH, StormConf.SNSPATH};
        String tableName[] = StormConf.TOTALTABLE;
        if (tableName.length != 6) {
            log.error("the tableName needed 6,but there is " + tableName.length
                + ".please check the configure file "
                + "argsTopo.properties" + ": totalTable "
                + "\nLine:36 or so");
            return;
        }

        builder.setSpout(ID.wapFiles.name(), new FilesRead1min(dataPath[0]), 1);
        builder.setBolt(ID.wapSplit.name(), new WapSplit(), 2).shuffleGrouping(
            ID.wapFiles.name());
        builder.setSpout(ID.softFiles.name(), new FilesRead1min(dataPath[1]), 1);
        builder.setBolt(ID.softSplit.name(), new InterfaceSplit(), 2)
            .shuffleGrouping(ID.softFiles.name());
        builder.setSpout(ID.pgvtFiles.name(), new FilesRead1min(dataPath[2]), 1);
        builder.setBolt(ID.pgvtSplit.name(), new TotalPvUvSplit(), 2)
            .shuffleGrouping(ID.pgvtFiles.name());
        builder.setSpout(ID.cdwFiles.name(), new FilesRead1min(dataPath[3]), 1);
        builder.setBolt(ID.cdwSplit.name(), new TotalPvUvSplit(), 2)
            .shuffleGrouping(ID.cdwFiles.name());
        builder.setSpout(ID.snsFiles.name(), new FilesRead1min(dataPath[4]), 1);
        builder.setBolt(ID.snsSplit.name(), new TotalPvUvSplit(), 2)
            .shuffleGrouping(ID.snsFiles.name());

        // 信号
        builder.setSpout(ID.signalDB.name(), new SignalDB());
        builder.setSpout(ID.signal24H.name(), new Signal24h());

        // 总PV
        builder.setBolt(ID.totalPV.name(), new Total(), 1)
            .shuffleGrouping(ID.wapSplit.name())
            .shuffleGrouping(ID.softSplit.name(),
                StreamId.INTERFACEDATA.name())
            .shuffleGrouping(ID.pgvtSplit.name())
            .shuffleGrouping(ID.cdwSplit.name())
            .shuffleGrouping(ID.snsSplit.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.totalPVDB.name(), new TotalDB(tableName[0]), 1)
            .shuffleGrouping(ID.totalPV.name())
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        // 用户去重：总用户数
        builder.setBolt(ID.userClean.name(), new TotalUVClean(), 1)
            // 修改以说明算法的正确性，验证一下是不是这个的原因。已经验证是正确的。
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
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.totalUser.name(), new Total(), 1)
            .shuffleGrouping(ID.userClean.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.totalUserDB.name(), new TotalDB(tableName[1]), 1)
            .shuffleGrouping(ID.totalUser.name())
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        // 分省PV
        builder.setBolt(ID.proCln.name(), new ProClean(), 1)
            // 分省clean
            .shuffleGrouping(ID.wapSplit.name())
            .shuffleGrouping(ID.softSplit.name(),
                StreamId.INTERFACEDATA.name())
            .shuffleGrouping(ID.pgvtSplit.name())
            .shuffleGrouping(ID.cdwSplit.name())
            .shuffleGrouping(ID.snsSplit.name());
        builder.setBolt(ID.proPV.name(), new ProvinceItem(), 2)
            .fieldsGrouping(ID.proCln.name(),
                new Fields(FName.PROVINCE_ID.name()))
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.proPVDB.name(), new ProvinceItemDB(tableName[2]), 2)
            .fieldsGrouping(ID.proPV.name(),
                new Fields(FName.PROVINCE_ID.name()))
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        // 分省用户数
        // 分省clean
        builder.setBolt(ID.proClnUV.name(), new ProClean(), 1).shuffleGrouping(
            ID.userClean.name());
        builder.setBolt(ID.proUser.name(), new ProvinceItem(), 4)
            .fieldsGrouping(ID.proClnUV.name(),
                new Fields(FName.PROVINCE_ID.name()))
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.proUserDB.name(), new ProvinceItemDB(tableName[3]),
            4)
            .fieldsGrouping(ID.proUser.name(),
                new Fields(FName.PROVINCE_ID.name()))
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        // 总平台分地市访问用户数 total_pro_city_uv
        builder.setBolt(ID.proCityCleanUV.name(), new ProCityClean(), 1)
            .shuffleGrouping(ID.userClean.name());
        builder.setBolt(ID.proCityDBUV.name(), new ProCityDB(tableName[4]), 4)
            .fieldsGrouping(ID.proCityCleanUV.name(),
                new Fields(FName.PROCITY.name()))
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        // 总平台分地市访问PV total_pro_city_pv
        builder.setBolt(ID.proCityCleanPV.name(), new ProCityClean(), 1)
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
                new Fields(FName.MSISDN.name()));
        builder.setBolt(ID.proCityDBPV.name(), new ProCityDB(tableName[5]), 4)
            .fieldsGrouping(ID.proCityCleanPV.name(),
                new Fields(FName.PROCITY.name()))
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        Config conf = new Config();
        conf.setNumWorkers(8);
        StormSubmitter.submitTopology(StormConf.TOTALTOPO, conf,
            builder.createTopology());
    }
}
