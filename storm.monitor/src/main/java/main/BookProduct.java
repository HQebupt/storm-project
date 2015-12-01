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

public class BookProduct {
    static Logger log = Logger.getLogger(BookProduct.class);

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        String dataPath = StormConf.ORDPATH;
        String tableName[] = StormConf.ORDBKPDTABLE;
        String bookInfoDir = StormConf.BOOKPATH;
        String productInfoDir = StormConf.PRODUCTPATH;
        if (tableName.length != 6) {
            log.error("the tableName needed 6,but there is " + tableName.length
                + ".please check the configure file "
                + "argsTopo.properties" + ": ordBkPdTable "
                + "\nLine:29 or so");
            return;
        }
        builder.setSpout(ID.filesSpout.name(), new FilesRead1min(dataPath), 1);
        builder.setSpout(ID.signalDB.name(), new SignalDB());
        builder.setSpout(ID.signalUp.name(), new SignalUpdate());
        builder.setSpout(ID.signal24H.name(), new Signal24h());
        builder.setBolt(ID.SPLIT.name(), new OrderSplit(), 4).shuffleGrouping(
            ID.filesSpout.name());

        // 分省clean
        builder.setBolt(ID.proCln.name(), new ProCleanOrd(), 1)
            .shuffleGrouping(ID.SPLIT.name(), StreamId.ORDER.name());

        // 总的、分省计费图书类型
        builder.setBolt(ID.totalBook.name(), new BookTotal(), 4)
            .fieldsGrouping(ID.SPLIT.name(), StreamId.ORDER.name(),
                new Fields(FName.BOOK_ID.name()))
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.totalBookDB.name(),
            new BkPtDB(tableName[0], bookInfoDir), 1)
            .shuffleGrouping(ID.totalBook.name(), StreamId.ITEMTYPE.name())
            .allGrouping(ID.signalUp.name(), StreamId.SIGNALUPDATE.name())
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        // 分省
        builder.setBolt(ID.proBook.name(), new BookProvince(), 4)
            .fieldsGrouping(ID.proCln.name(),
                new Fields(FName.PROVINCE_ID.name()))
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.proBookDB.name(),
            new BkPtProDB(tableName[1], bookInfoDir), 1)
            .fieldsGrouping(ID.proBook.name(),
                StreamId.PROVINCEITEM.name(),
                new Fields(FName.PROVINCEITEM.name()))
            .allGrouping(ID.signalUp.name(), StreamId.SIGNALUPDATE.name())
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        // 总的、分省计费产品类型
        builder.setBolt(ID.totalProduct.name(), new ProductTotal(), 4)
            .fieldsGrouping(ID.SPLIT.name(), StreamId.ORDER.name(),
                new Fields(FName.PRODUCT_ID.name()))
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.totalProductDB.name(),
            new BkPtDB(tableName[2], productInfoDir), 1)
            .shuffleGrouping(ID.totalProduct.name(),
                StreamId.ITEMTYPE.name())
            .allGrouping(ID.signalUp.name(), StreamId.SIGNALUPDATE.name())
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        // 分省
        builder.setBolt(ID.proProduct.name(), new ProductProvince(), 4)
            .fieldsGrouping(ID.proCln.name(),
                new Fields(FName.PROVINCE_ID.name()))
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.proProductDB.name(),
            new BkPtProDB(tableName[3], productInfoDir), 1)
            .fieldsGrouping(ID.proProduct.name(),
                StreamId.PROVINCEITEM.name(),
                new Fields(FName.PROVINCEITEM.name()))
            .allGrouping(ID.signalUp.name(), StreamId.SIGNALUPDATE.name())
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        // 分省City clean
        builder.setBolt(ID.proCityCln.name(), new ProCityCleanOrd(), 1)
            .shuffleGrouping(ID.SPLIT.name(), StreamId.ORDER.name());

        // 分地市图书订购
        builder.setBolt(ID.proCityBook.name(), new BookProCity(), 4)
            .fieldsGrouping(ID.proCityCln.name(),
                new Fields(FName.PROCITY.name()))
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.proCityBookDB.name(),
            new BkPtProCityDB(tableName[4], bookInfoDir), 1)
            .fieldsGrouping(ID.proCityBook.name(),
                StreamId.PROVINCEITEM.name(),
                new Fields(FName.PROCITYITEM.name()))
            .allGrouping(ID.signalUp.name(), StreamId.SIGNALUPDATE.name())
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        // 分地市包月订购
        builder.setBolt(ID.proCityProduct.name(), new ProductProCity(), 4)
            .fieldsGrouping(ID.proCityCln.name(),
                new Fields(FName.PROCITY.name()))
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.proCityProductDB.name(),
            new BkPtProCityDB(tableName[5], productInfoDir), 1)
            .fieldsGrouping(ID.proCityProduct.name(),
                StreamId.PROVINCEITEM.name(),
                new Fields(FName.PROCITYITEM.name()))
            .allGrouping(ID.signalUp.name(), StreamId.SIGNALUPDATE.name())
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        Config conf = new Config();
        conf.setNumWorkers(8);
        StormSubmitter.submitTopology(StormConf.BKPDTOPO, conf,
            builder.createTopology());
    }
}
