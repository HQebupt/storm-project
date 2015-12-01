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

public class Signal15min extends BaseRichSpout {

    private static final long serialVersionUID = 1L;
    private SpoutOutputCollector collector;
    transient java.util.Calendar cal = null;
    java.text.SimpleDateFormat format = new java.text.SimpleDateFormat(
        TimeConst.YYMMDDHHMMSS);
    SimpleDateFormat ftime = new SimpleDateFormat(TimeConst.MM);
    Boolean timeflag = false;
    int lastT = -1;
    static Logger log = Logger.getLogger(Signal15min.class);

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
        if (min % TimeConst.PERIODTIME15 == 0) {
            timeflag = true;
        } else {
            timeflag = false;
        }
        int timedif = min - lastT;
        lastT = min;
        if ((timedif != 0) && (timeflag == true)) {
            log.info("signal15min:" + min + "  lastT:" + lastT + " timedif: "
                + timedif + "  timeflag: " + timeflag);
            cal.add(Calendar.MINUTE, +TimeConst.PERIODTIME15);
            String datePath = format.format(cal.getTime());
            log.info("signal15min：" + datePath + "time:" + min);
            collector.emit(StreamId.SIGNAL15MIN.name(), new Values(datePath));
        }
        try {
            Thread.sleep(TimeConst.SigTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void buildStartTime() {
        if (cal == null) {
            log.info("initialize the Calendar cal！");
            cal = STime.buildSig();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamId.SIGNAL15MIN.name(), new Fields(
            FName.ACTION15MIN.name()));
    }

}
