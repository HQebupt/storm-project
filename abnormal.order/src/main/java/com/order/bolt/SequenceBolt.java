package com.order.bolt;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.order.util.StormConf;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class SequenceBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 0l;
    static Logger log = Logger.getLogger(SequenceBolt.class);

    String name;
    Map<Long, String> values = new HashMap<Long, String>();
    long id = 0l;
    public static final String STRING_SCHEME_KEY = "str";

    public SequenceBolt(String name) {
        this.name = name;
    }

    public SequenceBolt() {
        this.name = "default";
    }

    @Override
    public void prepare(Map conf, TopologyContext context) {
        super.prepare(conf, context);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        final int tickFrequencyInSeconds = StormConf.TICKSECONDS;
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
        return conf;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (isTickTuple(input)) {
            // 定时触发
            log.info(this.name + ": Time is comming.");
            for (Entry<Long, String> entry : values.entrySet()) {
                log.info("id: " + entry.getKey() + "\nvalue: "
                    + entry.getValue());
            }
            values.clear();
        } else {
            String word = input.getStringByField(STRING_SCHEME_KEY);
            id++;
            values.put(id, word);
        }
    }

    public static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
            && tuple.getSourceStreamId().equals(
            Constants.SYSTEM_TICK_STREAM_ID);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
