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

public class Signal24h extends BaseRichSpout {

    private static final long serialVersionUID = 1L;
    private SpoutOutputCollector collector;
    SimpleDateFormat fmat = new SimpleDateFormat(TimeConst.YYMMDDHHMMSS);
    SimpleDateFormat ft24h = new SimpleDateFormat(TimeConst.HHMM);
    Boolean timeflag = false;
    long lastT = -1;
    static Logger log = Logger.getLogger(Signal24h.class);

    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        Calendar cal24h = Calendar.getInstance();
        String h24 = ft24h.format(cal24h.getTime());
        long time = System.currentTimeMillis();
        long min = time / 1000 / 60;
        if (h24.equalsIgnoreCase(TimeConst.Cls0Time)) {
            timeflag = true;
        } else {
            timeflag = false;
        }
        long timedif = min - lastT;
        lastT = min;
        if ((timedif != 0) && (timeflag == true)) {
            log.info("signal24H min:" + min + "  lastT:" + lastT + " timedif: "
                + timedif + "  timeflag: " + timeflag);
            String date = fmat.format(cal24h.getTime());
            log.info("24H Signals24Hour is comming：" + date);
            collector.emit(StreamId.SIGNAL24H.name(), new Values(date));
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
        declarer.declareStream(StreamId.SIGNAL24H.name(), new Fields(FName.ACTION24H.name()));
    }
}
