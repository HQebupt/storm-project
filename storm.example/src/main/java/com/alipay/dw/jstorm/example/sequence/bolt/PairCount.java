package com.alipay.dw.jstorm.example.sequence.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alipay.dw.jstorm.example.TpsCounter;
import com.alipay.dw.jstorm.example.sequence.bean.Pair;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;


public class PairCount implements IBasicBolt {
    /**
     *
     */
    private static final long serialVersionUID = 7346295981904929419L;

    public static final Logger LOG = Logger.getLogger(PairCount.class);

    private AtomicLong sum = new AtomicLong(0);

    private TpsCounter tpsCounter;

    public void prepare(Map conf, TopologyContext context) {
        tpsCounter = new TpsCounter(context.getThisComponentId() +
            ":" + context.getThisTaskId());

        LOG.info("Successfully do parepare " + context.getThisComponentId());
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        tpsCounter.count();

        Long tupleId = tuple.getLong(0);
        Pair pair = (Pair) tuple.getValue(1);

        sum.addAndGet(pair.getValue());

        collector.emit(new Values(tupleId, pair));

    }

    public void cleanup() {
        tpsCounter.cleanup();
        LOG.info("Total receive value :" + sum);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ID", "PAIR"));
    }

    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
}