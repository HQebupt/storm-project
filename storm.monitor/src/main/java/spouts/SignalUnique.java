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

public class SignalUnique extends BaseRichSpout {

    private static final long serialVersionUID = 1L;
    private SpoutOutputCollector collector;
    SimpleDateFormat fmat = new SimpleDateFormat(TimeConst.YYMMDDHHMMSS);
    SimpleDateFormat ft = new SimpleDateFormat(TimeConst.MM);
    Boolean timeflag = false;
    long lastT = -1;
    static Logger log = Logger.getLogger(SignalUnique.class);

    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        Calendar cal = Calendar.getInstance();
        String halfHour = ft.format(cal.getTime());
        long time = System.currentTimeMillis();
        long min = time / 1000 / 60;
        if (halfHour.equalsIgnoreCase(TimeConst.HALFHOUR)) {
            timeflag = true;
        } else {
            timeflag = false;
        }
        long timedif = min - lastT;
        lastT = min;
        if ((timedif != 0) && (timeflag == true)) {
            log.info("signalHalf1H min:" + min + "  lastT:" + lastT
                + " timedif: " + timedif + "  timeflag: " + timeflag);
            cal.add(Calendar.MINUTE, -TimeConst.PERIODTIME30);
            String date = fmat.format(cal.getTime());
            log.info("半小时点计算清0信号发射-SignalUnique：" + date);
            collector.emit(StreamId.SIGNALUNIQUE.name(), new Values(date));
            timeflag = false;
        }
        try {
            Thread.sleep(TimeConst.SigTime * 10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamId.SIGNALUNIQUE.name(), new Fields(
            FName.ACTION.name()));
    }
}
