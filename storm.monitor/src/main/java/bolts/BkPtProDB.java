package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import db.DB;
import db.DBConstant;
import org.apache.log4j.Logger;
import util.FName;
import util.InfoUpdate;
import util.StreamId;
import util.TimeConst;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class BkPtProDB extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    private Map<String, Integer> itemCount = new HashMap<String, Integer>();
    private static Map<String, String> itemInfo = new HashMap<String, String>();
    private String tableName;
    private DB db = new DB();
    private String path;
    static Logger log = Logger.getLogger(BkPtProDB.class);

    public BkPtProDB(String tableName, String path) {
        this.tableName = tableName;
        this.path = path;
    }

    public class ItemInner implements Comparable<ItemInner> {
        String pro, id, type;
        int num;

        public ItemInner(String pro, String id, String type, int num) {
            this.pro = pro;
            this.id = id;
            this.num = num;
            this.type = type;
        }

        public String getPro() {
            return pro;
        }

        public String getId() {
            return id;
        }

        public int getNum() {
            return num;
        }

        public String getType() {
            return type;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (!(obj instanceof ItemInner))
                return false;
            final ItemInner other = (ItemInner) obj;
            if (this.getPro().equals(other.getPro())
                && this.getId().equals(other.getId())
                && this.getNum() == other.getNum()
                && this.getType().equals(other.getType()))
                return true;
            else
                return false;
        }

        @Override
        public int compareTo(ItemInner o) {
            if (this.num > o.getNum())
                return 1;
            if (this.num < o.getNum())
                return -1;
            return 0;
        }
    }

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        log.info("Initializing the class BkPtProDB.java. The bookInfo's path is :"
            + path);
        InfoUpdate.updateBookInfo(path, itemInfo);
    }

    public void execute(Tuple input) {
        try {
            String itemID = input.getStringByField(FName.PROVINCEITEM.name());
            Integer count = input.getIntegerByField(FName.COUNT.name());
            itemCount.put(itemID, count);
        } catch (IllegalArgumentException e) {
            String streamId = input.getSourceStreamId();
            if (streamId.equals(StreamId.SIGNALDB.name())) {
                String timePeriod = input.getStringByField(FName.ACTION.name());
                ArrayList<ItemInner> arrSort = sort();
                log.info("after sort,the arrSort's size is:" + arrSort.size());
                HashMap<String, Set<ItemInner>> topMap = getTopN(arrSort);
                downloadToDB(timePeriod, topMap);
            } else if (streamId.equals(StreamId.SIGNALUPDATE.name())) {
                InfoUpdate.updateBookInfo(path, itemInfo);
            } else if (streamId.equals(StreamId.SIGNAL24H.name())) {
                log.info("24Hour is coming.");
                itemCount.clear();
            }
        }
        collector.ack(input);
    }

    private HashMap<String, Set<ItemInner>> getTopN(ArrayList<ItemInner> arrSort) {
        HashMap<String, Set<ItemInner>> topMap = new HashMap<String, Set<ItemInner>>(
            31);
        int size = arrSort.size();
        for (int i = size - 1; i >= 0; i--) {
            ItemInner item = arrSort.get(i);
            String pro = item.getPro();
            String type = item.getType();
            addItem(topMap, pro + "|" + type, item);
        }
        log.info("in getTopN method,topMap is:" + topMap.size());
        return topMap;
    }

    private void addItem(HashMap<String, Set<ItemInner>> topMap,
                         String proType, ItemInner e) {
        Set<ItemInner> set = topMap.get(proType);
        int size = 0;
        if (set != null) {
            size = set.size();
        } else {
            set = new HashSet<BkPtProDB.ItemInner>();
        }
        if (size < TimeConst.TOPN) {
            set.add(e);
            topMap.put(proType, set);
        }
        // log.info("in addItem method,topMap size :"+topMap.size());
    }

    private ArrayList<ItemInner> sort() {
        int infosize = itemInfo.size();
        if (infosize == 0) {
            InfoUpdate.updateBookInfo(path, itemInfo);
        }
        log.info("bookinfo's size: " + infosize);
        ArrayList<ItemInner> arr = new ArrayList<ItemInner>(5000);
        log.info("the bookProcut total Map size is:" + itemCount.size());
        for (Map.Entry<String, Integer> entry : itemCount.entrySet()) {
            String key = entry.getKey();
            Integer count = entry.getValue();
            String[] words = key.split("\\|", -1);
            String provinceid = words[0];
            String itemid = words[1];
            String type = itemInfo.get(itemid);
            arr.add(new ItemInner(provinceid, itemid, type, count));
        }
        Collections.sort(arr);
        log.info("in sort method,the arrSort's size:" + arr.size());
        return arr;
    }

    private void downloadToDB(String timePeriod,
                              HashMap<String, Set<ItemInner>> topMap) {
        log.info("start to write to DB!");
        // int infosize = itemInfo.size();
        // if (infosize == 0) {
        // InfoUpdate.updateBookInfo(path, itemInfo);
        // }
        // log.info("bookinfo's size: " + infosize);
        Map<String, String> parameters = db.parameters;
        String fields = parameters.get(tableName);
        try {
            long startTime = System.currentTimeMillis();
            String sql = "insert into " + this.tableName + "(" + fields + ")"
                + " values(?,?,?,?,?)";
            log.info("Sql:" + sql + ".record_time: " + timePeriod);
            Class.forName("oracle.jdbc.driver.OracleDriver");
            Connection con = DriverManager.getConnection(DBConstant.DBURL,
                DBConstant.DBUSER, DBConstant.DBPASSWORD);
            con.setAutoCommit(false);
            PreparedStatement pst = con.prepareStatement(sql);
            log.info("topMap'size :" + topMap.size());
            for (Set<ItemInner> s : topMap.values()) {
                for (ItemInner e : s) {
                    String provinceid = e.getPro();
                    String itemid = e.getId();
                    int count = e.getNum();
                    String type = e.getType();
                    pst.setString(1, timePeriod);
                    pst.setString(2, itemid);
                    pst.setString(3, provinceid);
                    pst.setString(4, type);
                    pst.setInt(5, count);
                    pst.addBatch();
                }
            }
            pst.executeBatch();
            con.commit();
            pst.close();
            con.close();
            long endTime = System.currentTimeMillis();
            log.info("the patch insert taked time is ：" + (endTime - startTime)
                + "ms");
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("insert data to DB is failed.");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            log.error("the class is Not Found!");
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    public void cleanup() {
    }
}
