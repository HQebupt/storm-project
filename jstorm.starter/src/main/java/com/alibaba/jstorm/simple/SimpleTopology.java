package com.alibaba.jstorm.simple;

import java.io.InputStream;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.TopologyAssignException;
import backtype.storm.topology.TopologyBuilder;

import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.utils.JStormUtils;

/**
 * @author von gosling
 */
public class SimpleTopology {
  private static final String CONF = "topology.yaml";

  private static String topologyName;

  private static Map<String, String> conf;

  @SuppressWarnings("unchecked")
  private static void loadYaml() {
    Yaml yaml = new Yaml();
    try {
      InputStream stream = Thread.currentThread().getContextClassLoader()
          .getResourceAsStream(CONF);
      conf = (Map<String, String>) yaml.load(stream);
      if (conf == null || conf.isEmpty() == true) {
        throw new RuntimeException("Failed to read config file");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static TopologyBuilder setBuilder() {
    topologyName = (String) conf.get(Config.TOPOLOGY_NAME);

    TopologyBuilder topologyBuilder = new TopologyBuilder();

    int spoutParallel = JStormUtils.parseInt(conf.get("topology.spout.parallel"), 1);
    int boltParallel = JStormUtils.parseInt(conf.get("topology.bolt.parallel"), 1);

    topologyBuilder.setSpout("Spout", new SimpleSpout(), spoutParallel);
    topologyBuilder.setBolt("Bolt", new SimpleBolt(), boltParallel).shuffleGrouping("Spout");

    return topologyBuilder;
  }

  public static void setLocalTopology() throws Exception {
    TopologyBuilder builder = setBuilder();

    LocalCluster cluster = new LocalCluster();

    //InnerLogger.getInstance().resetLogger(conf);

    cluster.submitTopology(topologyName, conf, builder.createTopology());

    Thread.sleep(60000);

    cluster.shutdown();
  }

  public static void setRemoteTopology() throws AlreadyAliveException, InvalidTopologyException,
      TopologyAssignException {
    TopologyBuilder builder = setBuilder();

    StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
  }

  public static void main(String[] args) throws Exception {
    loadYaml();

    boolean isLocal = StormConfig.local_mode(conf);

    if (isLocal) {
      setLocalTopology();
    } else {
      setRemoteTopology();
    }
  }

}
