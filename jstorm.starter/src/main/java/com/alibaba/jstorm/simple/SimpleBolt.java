package com.alibaba.jstorm.simple;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author von gosling
 */
public class SimpleBolt extends BaseRichBolt {
  private static final long serialVersionUID = -8191633148794433081L;

  private static final Logger LOG = LoggerFactory.getLogger(SimpleBolt.class);

  @SuppressWarnings("unused")
  private OutputCollector collector;

  @Override
  public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
                      OutputCollector collector) {
    this.collector = collector;
    LOG.error("Preparing bolt !");
    InnerLogger.getInstance().resetLogger(conf);
  }

  @Override
  public void execute(Tuple input) {
    Object randomNumber = input.getValue(0);

    LOG.debug("Receive number {}", randomNumber);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    // TODO
  }

}
