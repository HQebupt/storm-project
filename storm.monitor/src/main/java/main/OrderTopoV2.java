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
import spouts.SignalUpdate;
import util.FName;
import util.StreamId;

public class OrderTopoV2 {
    static Logger log = Logger.getLogger(OrderTopoV2.class);

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        String dataPath = StormConf.ORDPATH;
        String tableName[] = StormConf.ORDTABLE;
        if (tableName.length != 15) {
            log.error("the tableName needed 15,but there is "
                + tableName.length + ".please check the configure file "
                + "argsTopo.properties" + ": ordTable " + "\nLine:28 or so");
            return;
        }
        builder.setSpout(ID.filesSpout.name(), new FilesRead1min(dataPath), 1);
        builder.setSpout(ID.signalDB.name(), new SignalDB());
        builder.setSpout(ID.signal24H.name(), new Signal24h());
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
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.totalUser.name(), new Total(), 1)
            .shuffleGrouping(ID.userClean.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.totalUserDB.name(), new TotalDB(tableName[0]), 1)
            .shuffleGrouping(ID.totalUser.name())
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        // 分平台用户数
        builder.setBolt(ID.plaUser.name(), new OrderPlatformUser(), 4)
            .fieldsGrouping(ID.userClean.name(),
                new Fields(FName.PLATFORM.name()))
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.plaUserDB.name(), new ProvinceItemDB(tableName[1]),
            4)
            .fieldsGrouping(ID.plaUser.name(),
                new Fields(FName.PROVINCE_ID.name()))
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        // 按次、按月订购费用
        builder.setBolt(ID.totalFee.name(), new OrderTotalInfoFee(), 1)
            .shuffleGrouping(ID.clnMoblie.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.totalFeeDB.name(),
            new OrderTotalInfoFeeDB(tableName[2], tableName[3]), 1)
            .shuffleGrouping(ID.totalFee.name())
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        // 分省clean
        builder.setBolt(ID.proCln.name(), new ProCleanOrd(), 1)
            .shuffleGrouping(ID.clnMoblie.name());

        // 分省按次、按月订购费用
        builder.setBolt(ID.proFee.name(), new OrderProvinceInfoFee(), 4)
            .fieldsGrouping(ID.proCln.name(),
                new Fields(FName.PROVINCE_ID.name()))
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.proFeeDB.name(),
            new OrderProvinceInfoFeeDB(tableName[4], tableName[5]), 4)
            .fieldsGrouping(ID.proFee.name(),
                new Fields(FName.PROVINCE_ID.name()))
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        // 分省分平台用户数
        builder.setBolt(ID.proplatUv.name(), new OrdProPlatUV(), 4)
            .fieldsGrouping(ID.userClean.name(),
                new Fields(FName.PROVINCE_ID.name()))
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.proplatUvDB.name(),
            new OrdProPlatUVDB(tableName[6]), 4)
            .fieldsGrouping(ID.proplatUv.name(),
                new Fields(FName.PROVINCE_ID.name()))
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        // 分省订购用户数
        builder.setBolt(ID.proUser.name(), new ProvinceItem(), 4)
            .fieldsGrouping(ID.userClean.name(),
                new Fields(FName.PROVINCE_ID.name()))
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.proUserDB.name(), new ProvinceItemDB(tableName[11]),
            4)
            .fieldsGrouping(ID.proUser.name(),
                new Fields(FName.PROVINCE_ID.name()))
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        // 总平台分地市付费用户数 ord_pro_city_uv
        builder.setBolt(ID.proCityClean.name(), new ProCityClean(), 1)
            .shuffleGrouping(ID.userClean.name());
        builder.setBolt(ID.proCityDB.name(), new ProCityDB(tableName[12]), 4)
            .fieldsGrouping(ID.proCityClean.name(),
                new Fields(FName.PROCITY.name()))
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        // 总平台分地市信息费 ord_pro_city_fee
        builder.setBolt(ID.proCityCleanOrd.name(), new ProCityCleanOrd(), 1)
            .shuffleGrouping(ID.clnMoblie.name());
        builder.setBolt(ID.ordProCityFeeDB.name(),
            new OrdProCityFeeDB(tableName[13]), 4)
            .fieldsGrouping(ID.proCityCleanOrd.name(),
                new Fields(FName.PROCITY.name()))
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        // 分地市包月用户数 ord_pro_city_monthUv
        builder.setBolt(ID.OrdUserMonthClean.name(), new OrdUserMonthClean(), 4)
            .fieldsGrouping(ID.clnMoblie.name(),
                new Fields(FName.MSISDN.name()))
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.proCityCleanMonth.name(), new ProCityClean(), 1)
            .shuffleGrouping(ID.OrdUserMonthClean.name());
        builder.setBolt(ID.proCityMonthDB.name(), new ProCityDB(tableName[14]),
            4)
            .fieldsGrouping(ID.proCityCleanMonth.name(),
                new Fields(FName.PROCITY.name()))
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        Config conf = new Config();
        conf.setNumWorkers(8);
        StormSubmitter.submitTopology(StormConf.ORDTOPO, conf,
            builder.createTopology());
    }
}
