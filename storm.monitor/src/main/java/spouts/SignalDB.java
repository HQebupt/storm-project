package spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import util.FName;
import util.STime;
import util.StreamId;
import util.TimeConst;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

public class SignalDB extends BaseRichSpout {

    private static final long serialVersionUID = 5300018502390271046L;
    private SpoutOutputCollector collector;
    transient java.util.Calendar cal = null;
    java.text.SimpleDateFormat format = new java.text.SimpleDateFormat(
        TimeConst.YYMMDDHHMMSS);
    SimpleDateFormat ftime = new SimpleDateFormat(TimeConst.MMSS);
    Boolean timeflag = false;
    int lastT = -1;
    static Logger log = Logger.getLogger(SignalDB.class);

    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        buildStartTime();
        Calendar ctime = Calendar.getInstance();
        int min = Integer.parseInt(ftime.format(ctime.getTime()));
        int timedif = min - lastT;
        lastT = min;
        if ((timedif != 0)
            && ((min == TimeConst.SEC50[0]) || (min == TimeConst.SEC50[1])
            || (min == TimeConst.SEC50[2])
            || (min == TimeConst.SEC50[3])
            || (min == TimeConst.SEC50[4])
            || (min == TimeConst.SEC50[5])
            || (min == TimeConst.SEC50[6])
            || (min == TimeConst.SEC50[7])
            || (min == TimeConst.SEC50[8]) || (min == TimeConst.SEC50[9]))) {
            log.info("signalDB:" + min + "  lastT:" + lastT + " timedif: "
                + timedif + "  timeflag: " + timeflag);
            cal.add(Calendar.MINUTE, +TimeConst.PERIODTIME);
            String datePath = format.format(cal.getTime());
            log.info("signals：" + datePath + ", time by min:" + min);
            collector.emit(StreamId.SIGNALDB.name(), new Values(datePath));
        }
        try {
            Thread.sleep(TimeConst.SigTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void buildStartTime() {
        if (cal == null) {
            log.info("SignalsSpout initialize the Calendar cal！");
            cal = STime.buildSig();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamId.SIGNALDB.name(), new Fields(FName.ACTION.name()));
    }

}
