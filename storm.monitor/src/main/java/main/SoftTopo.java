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

public class SoftTopo {
    static Logger log = Logger.getLogger(SoftTopo.class);

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        String dataPath = StormConf.SOFTPATH;
        String uesPath = StormConf.UESPATH;
        String snsPath = StormConf.SNSPATH;
        String clidownPath = StormConf.CLIENTPATH;
        String tableName[] = StormConf.SOFTTABLE;
        if (tableName.length != 10) {
            log.error("the tableName needed 10,but there is "
                + tableName.length + ".please check the configure file "
                + "argsTopo.properties" + ": softTable "
                + "\nLine:10 or so");
            return;
        }
        builder.setSpout(ID.filesSpout1m.name(), new FilesReadPeriod(dataPath),
            1);
        builder.setSpout(ID.signalDB.name(), new SignalDB());
        builder.setSpout(ID.signal1m.name(), new Signal1min());
        builder.setSpout(ID.signal15m.name(), new Signal15min());
        builder.setSpout(ID.signal24H.name(), new Signal24h());
        builder.setBolt(ID.SPLIT1MIN.name(), new InterfaceSplit(), 4)
            .shuffleGrouping(ID.filesSpout1m.name());

        // 获取sns话单中soft的data
        builder.setSpout(ID.snsfilesSpout.name(), new FilesReadPeriod(snsPath), 1);
        builder.setBolt(ID.snsSPLIT.name(), new SnsCliSplit(), 2).shuffleGrouping(
            ID.snsfilesSpout.name());
        builder.setBolt(ID.snsCleanForSoft.name(), new SnsCleanForSoft(),
            2)
            .fieldsGrouping(ID.snsSPLIT.name(),
                new Fields(FName.MSISDN.name()))
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        // 获取client down话单中soft的data
        builder.setSpout(ID.clidownfilesSpout.name(), new FilesReadPeriod(clidownPath), 1);
        builder.setBolt(ID.clidownSPLIT.name(), new SnsCliSplit(), 2).shuffleGrouping(
            ID.clidownfilesSpout.name());
        builder.setBolt(ID.clidownCleanForSoft.name(), new CliCleanForSoft(),
            2)
            .fieldsGrouping(ID.clidownSPLIT.name(),
                new Fields(FName.MSISDN.name()))
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        // 获取ues话单中soft的data
        builder.setSpout(ID.filesSpout.name(), new FilesReadPeriod(uesPath), 1);
        builder.setBolt(ID.SPLIT.name(), new WapSplit(), 4).shuffleGrouping(
            ID.filesSpout.name());
        builder.setBolt(ID.userCleanForSoft.name(), new WapUserCleanForSoft(),
            2)
            .fieldsGrouping(ID.SPLIT.name(),
                new Fields(FName.MSISDN.name()))
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        // 总PV
        builder.setBolt(ID.totalPV.name(), new Total(), 1)
            .shuffleGrouping(ID.SPLIT1MIN.name(),
                StreamId.INTERFACEDATA.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.totalPVDB.name(), new TotalDB(tableName[0]), 1)
            .shuffleGrouping(ID.totalPV.name())
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        // 分省PV
        builder.setBolt(ID.proCln.name(), new ProClean(), 1).shuffleGrouping(
            ID.SPLIT1MIN.name(), StreamId.INTERFACEDATA.name());
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
            .shuffleGrouping(ID.SPLIT1MIN.name(),
                StreamId.INTERFACEDATA.name())
            .allGrouping(ID.signal1m.name(), StreamId.SIGNAL1MIN.name());
        builder.setBolt(ID.pvByTimeDB.name(), new PVByTimeDB(tableName[2]), 1)
            .shuffleGrouping(ID.pvByTime.name())
            .allGrouping(ID.signal15m.name(), StreamId.SIGNAL15MIN.name());

        // 用户去重：总、分省用户数
        builder.setBolt(ID.userClean.name(), new UserClean(), 2)
            .fieldsGrouping(ID.SPLIT1MIN.name(),
                StreamId.INTERFACEDATA.name(),
                new Fields(FName.MSISDN.name()))
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.userCleanForUV.name(), new UserCleanForUV(), 4)
            .fieldsGrouping(ID.userClean.name(), new Fields(FName.MSISDN.name()))
            .fieldsGrouping(ID.userCleanForSoft.name(), new Fields(FName.MSISDN.name()))
            .fieldsGrouping(ID.snsCleanForSoft.name(), new Fields(FName.MSISDN.name()))
            .fieldsGrouping(ID.clidownCleanForSoft.name(), new Fields(FName.MSISDN.name()))
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        builder.setBolt(ID.totalUser.name(), new Total(), 1)
            .shuffleGrouping(ID.userCleanForUV.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.totalUserDB.name(), new TotalDB(tableName[3]), 1)
            .shuffleGrouping(ID.totalUser.name())
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        // 分省用户数
        builder.setBolt(ID.proClnUV.name(), new ProClean(), 1)
            .shuffleGrouping(ID.userCleanForUV.name());
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
            .fieldsGrouping(ID.SPLIT1MIN.name(),
                StreamId.INTERFACEDATA.name(),
                new Fields(FName.ACTION.name()))
            .allGrouping(ID.signal15m.name(), StreamId.SIGNAL15MIN.name());

        // tableName[6]是成功率的表。

        // tableName[7]是时延的表。

        // 分接入点秒PV统计
        builder.setBolt(ID.Beartype.name(), new Beartype(), 4).shuffleGrouping(
            ID.SPLIT1MIN.name(), StreamId.INTERFACEDATA.name());
        builder.setBolt(ID.BtPVRate.name(), new BtPVRate(), 1)
            .fieldsGrouping(ID.Beartype.name(),
                new Fields(FName.BEARTYPE.name()))
            .allGrouping(ID.signal1m.name(), StreamId.SIGNAL1MIN.name());
        builder.setBolt(ID.BtPVRateDB.name(), new BtPVRateDB(tableName[8]), 1)
            .fieldsGrouping(ID.BtPVRate.name(),
                new Fields(FName.BEARTYPE.name()))
            .allGrouping(ID.signal15m.name(), StreamId.SIGNAL15MIN.name());

        // 分地市客户端付费用户数
        builder.setBolt(ID.proCityClean.name(), new ProCityClean(), 1)
            .shuffleGrouping(ID.userCleanForUV.name());
        builder.setBolt(ID.proCityDB.name(), new ProCityDB(tableName[9]), 4)
            .fieldsGrouping(ID.proCityClean.name(),
                new Fields(FName.PROCITY.name()))
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        Config conf = new Config();
        conf.setNumWorkers(8);
        StormSubmitter.submitTopology(StormConf.SOFTTOPO, conf,
            builder.createTopology());
    }
}
