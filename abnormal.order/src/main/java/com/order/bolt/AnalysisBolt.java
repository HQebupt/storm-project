package com.order.bolt;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.order.util.StormConf;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class AnalysisBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    // Map<String, Integer> provinceItem = new HashMap<String, Integer>();
    static Logger log = Logger.getLogger(AnalysisBolt.class);

    transient Timer timer = null; // isDaemon

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        int tickFrequencyInSeconds = StormConf.TICKSECONDS;
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
        return conf;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        if (isTickTuple(tuple)) {
            timer = getTimer();
            // 触发一个定时任务
            TimerTask task = new TimerTask() {
                public void run() {
                    log.info("This momment is perfect: " + new Date());
                }
            };
            timer.schedule(task, 1000 * StormConf.SCHEDULESECONDS);
        } else {
            // 正常统计
        }
    }

    private Timer getTimer() {
        if (this.timer == null) {
            return new Timer();
        }
        return this.timer;
    }

    public static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
            && tuple.getSourceStreamId().equals(
            Constants.SYSTEM_TICK_STREAM_ID);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
