package com.alibaba.jstorm.simple;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @author von gosling
 */
public class SimpleSpout extends BaseRichSpout {
  private static final long serialVersionUID = -506726549287286650L;

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private SpoutOutputCollector collector;

  private final Random randomContainer = new Random(5);

  private BlockingQueue<Integer> channel = new LinkedBlockingQueue<Integer>();

  @Override
  public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
                   SpoutOutputCollector collector) {
    this.collector = collector;
    ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
    service.scheduleAtFixedRate(new ScheduleEmitter(), 60, 60, TimeUnit.SECONDS);
    logger.error("Preparing spout !");
    InnerLogger.getInstance().resetLogger(conf);
  }

  @Override
  public void activate() {
    logger.info("Activate spout !");
  }

  @Override
  public void deactivate() {
    logger.info("Deactivate spout !");
  }

  @Override
  public void nextTuple() {
    Integer number = null;
    try {
      number = channel.take();
    } catch (InterruptedException e) {
      return;
    }
    if (number == null) {
      logger.warn("Null number !");
      return;
    }
    collector.emit(new Values(number));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    Fields fields = new Fields("RandomNumber");
    declarer.declare(fields);
  }

  class ScheduleEmitter implements Runnable {
    @Override
    public void run() {
      channel.offer(randomContainer.nextInt());
    }

  }
}
