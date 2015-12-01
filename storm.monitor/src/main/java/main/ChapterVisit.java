package main;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.*;
import org.apache.log4j.Logger;
import spouts.FilesReadPeriod;
import spouts.Signal24h;
import spouts.SignalDB;
import spouts.SignalUpdate;
import util.FName;
import util.StreamId;

public class ChapterVisit {
    static Logger log = Logger.getLogger(ChapterVisit.class);

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        String dataPath = StormConf.CHAPPATH;
        String tableName[] = StormConf.CHAPTABLE;
        String bookInfoDir = StormConf.BOOKPATH;
        if (tableName.length != 2) {
            log.error("the tableName needed 2,but there is " + tableName.length
                + ".please check the configure file "
                + "argsTopo.properties" + ": chapTable "
                + "\nLine:41 or so");
            return;
        }
        builder.setSpout(ID.filesSpout.name(), new FilesReadPeriod(dataPath), 1);
        builder.setSpout(ID.signalDB.name(), new SignalDB());
        // builder.setSpout("signal15m", new Signal15min());
        builder.setSpout(ID.signalUp.name(), new SignalUpdate());
        builder.setSpout(ID.signal24H.name(), new Signal24h());
        builder.setBolt(ID.SPLIT.name(), new ChapterSplit(), 4)
            .shuffleGrouping(ID.filesSpout.name());

        // book点播量
        builder.setBolt(ID.visitCln.name(), new ChapterClean(), 2)
            .fieldsGrouping(ID.SPLIT.name(),
                new Fields(FName.BOOK_ID.name()))
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.bkVisit.name(), new BookVisit(), 2)
            .fieldsGrouping(ID.visitCln.name(),
                new Fields(FName.BOOK_ID.name()))
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.bkVisitDB.name(), new BookVisitDB(tableName[0]), 1)
            .shuffleGrouping(ID.bkVisit.name())
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signalUp.name(), StreamId.SIGNALUPDATE.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        // book 分省 点播量
        builder.setBolt(ID.visitProCln.name(), new ChapterProClean(), 2)
            .fieldsGrouping(ID.SPLIT.name(),
                new Fields(FName.PROVINCE_ID.name()))
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.bkProVisit.name(), new BookProVisit(), 4)
            .fieldsGrouping(ID.visitProCln.name(),
                new Fields(FName.PROVINCE_ID.name()))
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());
        builder.setBolt(ID.bkProVisitDB.name(),
            new BookProVisitDB(tableName[1]), 1)
            .shuffleGrouping(ID.bkProVisit.name())
            .allGrouping(ID.signalDB.name(), StreamId.SIGNALDB.name())
            .allGrouping(ID.signalUp.name(), StreamId.SIGNALUPDATE.name())
            .allGrouping(ID.signal24H.name(), StreamId.SIGNAL24H.name());

        Config conf = new Config();
        conf.put(StormConf.BOOKINFO, bookInfoDir);
        conf.setNumWorkers(3);
        StormSubmitter.submitTopology(StormConf.CHAPTOPO, conf,
            builder.createTopology());
    }
}
