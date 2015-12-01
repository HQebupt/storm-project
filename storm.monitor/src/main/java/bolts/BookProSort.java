package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import util.Comp;
import util.FName;
import util.StreamId;
import util.TimeConst;

import java.util.*;
import java.util.Map.Entry;

public class BookProSort extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    private Map<String, Integer> itemCount = new HashMap<String, Integer>();
    private Comp cmp = new Comp();
    List<Map.Entry<String, Integer>> itemSort;
    java.util.Calendar cal;
    java.text.SimpleDateFormat format = new java.text.SimpleDateFormat(
        TimeConst.YYMMDDHHMMSS);
    static Logger log = Logger.getLogger(BookProSort.class);

    public BookProSort(String startTime) {
        buildStartTime(startTime);
    }

    private void buildStartTime(String startTime) {
        int year = Integer.valueOf(startTime.substring(0, 4));
        int month = Integer.valueOf(startTime.substring(4, 6)) - 1;
        int day = Integer.valueOf(startTime.substring(6, 8));
        int hour = Integer.valueOf(startTime.substring(8, 10));
        int min = Integer.valueOf(startTime.substring(10, 12));
        cal = new java.util.GregorianCalendar(year, month, day, hour, min, 0);
        cal.add(Calendar.MINUTE, -TimeConst.PERIODTIME);
    }

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String itemid = null;
        try {
            itemid = input.getStringByField(FName.PROVINCEITEM.name());
            Integer count = input.getIntegerByField(FName.COUNT.name());
            itemCount.put(itemid, count);
        } catch (IllegalArgumentException e) {
            String streamId = input.getSourceStreamId();
            if (streamId.equals(StreamId.SIGNALSORT.name())) {
                log.info("pro SIGNALSORT is coming!");
                itemSort = new ArrayList<Map.Entry<String, Integer>>(
                    itemCount.entrySet());
                log.info("pro sort start!");
                Collections.sort(itemSort, cmp);
                log.info("pro sort complete! Ready to go!");
                emitData();
            } else if (streamId.equals(StreamId.SIGNAL24H.name())) {
                log.info("24Hour is coming.");
                itemCount.clear();
            }
        }
        collector.ack(input);
    }

    private void emitData() {
        cal.add(Calendar.MINUTE, +TimeConst.PERIODTIME);
        String reTime = format.format(cal.getTime());
        if (itemSort == null) {
            log.info("itemSort is NULL!");
        } else {
            int sz = itemSort.size();
            log.info("pro size：" + sz);
            if (sz > TimeConst.TOPN) {
                sz = TimeConst.TOPN;
            }
            log.info("pro size after comparing to TOPN：" + sz);
            for (int i = 0; i < sz; i++) {
                Entry<String, Integer> entry = itemSort.get(i);
                String key = entry.getKey();
                Integer count = entry.getValue();
                collector.emit(new Values(key, count, reTime));
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FName.KEY.name(), FName.COUNT.name(), FName.RETIME.name()));
    }

    public void cleanup() {
    }
}
