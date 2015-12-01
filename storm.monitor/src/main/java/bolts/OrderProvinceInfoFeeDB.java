package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import db.DB;
import org.apache.log4j.Logger;
import util.FName;
import util.StreamId;

import java.util.HashMap;
import java.util.Map;

public class OrderProvinceInfoFeeDB extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    Map<String, Long> provinceTimeInfoFee = new HashMap<String, Long>();
    Map<String, Long> provinceMonthInfoFee = new HashMap<String, Long>();
    private String tableNameByTime;
    private String tableNameByMonth;
    private DB db = new DB();
    static Logger log = Logger.getLogger(OrderProvinceInfoFeeDB.class);

    public OrderProvinceInfoFeeDB(String tableNameByTime,
                                  String tableNameByMonth) {
        this.tableNameByTime = tableNameByTime;
        this.tableNameByMonth = tableNameByMonth;
    }

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String orderType = null;
        try {
            orderType = input.getStringByField(FName.ORDERTYPE.name());
        } catch (IllegalArgumentException e) {
        }
        if (orderType != null) {
            Long inforFeeCurrent = input
                .getLongByField(FName.REALINFORFEE.name());
            String province_id = input.getStringByField(FName.PROVINCE_ID
                .name());
            int type = 0;
            if (!orderType.equalsIgnoreCase("")) {
                type = Integer.valueOf(orderType);
            }
            if ((type == 4) || (type == 5)) {
                provinceMonthInfoFee.put(province_id, inforFeeCurrent);
            } else {
                provinceTimeInfoFee.put(province_id, inforFeeCurrent);
            }
        } else if (input.getSourceStreamId().equals(StreamId.SIGNALDB.name())) {
            orderType = input.getStringByField(FName.ACTION.name());
            String timePeriod = orderType.trim();
            downloadToDB(timePeriod);
        } else if (input.getSourceStreamId().equals(StreamId.SIGNAL24H.name())) {
            log.info("24Hour is coming.");
            provinceTimeInfoFee.clear();
            provinceMonthInfoFee.clear();
        }
        collector.ack(input);
    }

    private void downloadToDB(String timePeriod) {

        for (Map.Entry<String, Long> entry : provinceTimeInfoFee.entrySet()) {
            db.insertUser(tableNameByTime, timePeriod, entry.getKey(), entry
                .getValue().toString());
        }
        for (Map.Entry<String, Long> entry : provinceMonthInfoFee.entrySet()) {
            db.insertUser(tableNameByMonth, timePeriod, entry.getKey(), entry
                .getValue().toString());
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    public void cleanup() {
    }
}
