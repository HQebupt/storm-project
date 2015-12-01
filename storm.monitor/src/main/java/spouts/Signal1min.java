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

public class Signal1min extends BaseRichSpout {

    private static final long serialVersionUID = 1L;
    private SpoutOutputCollector collector;
    java.util.Calendar cal;
    java.text.SimpleDateFormat format = new java.text.SimpleDateFormat(
        TimeConst.YYMMDDHHMMSS);
    SimpleDateFormat ftime = new SimpleDateFormat(TimeConst.SS);
    int lastT = -1;
    static Logger log = Logger.getLogger(Signal1min.class);

    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        buildStartTime();
        Calendar ctime = Calendar.getInstance();
        int ss = Integer.parseInt(ftime.format(ctime.getTime()));
        int timedif = ss - lastT;
        lastT = ss;
        if ((timedif != 0) && (ss == 50)) {
            log.info("signal1min 50s:" + ss + "  lastT:" + lastT + " timedif: "
                + timedif);
            cal.add(Calendar.MINUTE, +TimeConst.PTIME1);
            String datePath = format.format(cal.getTime());
            log.info("1min 50s ：" + datePath + ",time sec:" + ss);
            collector.emit(StreamId.SIGNAL1MIN.name(), new Values(datePath));
        }
        try {
            Thread.sleep(TimeConst.SigTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void buildStartTime() {
        if (cal == null) {
            log.info("Signals1minSpout initialize the Calendar cal！");
            cal = STime.buildSig1min();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamId.SIGNAL1MIN.name(), new Fields(
            FName.ACTION1MIN.name()));
    }

}
