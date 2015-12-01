package spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import util.FName;
import util.StreamId;
import util.TimeConst;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

public class SignalUpdate extends BaseRichSpout {

    private static final long serialVersionUID = 1L;
    private SpoutOutputCollector collector;
    SimpleDateFormat fmat = new SimpleDateFormat(TimeConst.YYMMDDHHMMSS);
    SimpleDateFormat ft24h = new SimpleDateFormat(TimeConst.HHMM);
    Boolean timeflag = false;
    long lastT = -1;
    static Logger log = Logger.getLogger(SignalUpdate.class);

    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        Calendar calUp = Calendar.getInstance();
        String h24 = ft24h.format(calUp.getTime());
        long time = System.currentTimeMillis();
        long min = time / 1000 / 60;
        if (h24.equalsIgnoreCase(TimeConst.UPDATE)) {
            timeflag = true;
        } else {
            timeflag = false;
        }
        long timedif = min - lastT;
        lastT = min;
        if ((timedif != 0) && timeflag) {
            log.info("Update min:" + min + "  lastT:" + lastT + " timedif: "
                + timedif + "  timeflag: " + timeflag);
            String date = fmat.format(calUp.getTime());
            log.info("SignalsUpdate is comming：" + date);
            collector.emit(StreamId.SIGNALUPDATE.name(), new Values(date));
            timeflag = false;
        }
        try {
            Thread.sleep(TimeConst.SigTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamId.SIGNALUPDATE.name(), new Fields(
            FName.ACTIONUPDATE.name()));
    }
}
