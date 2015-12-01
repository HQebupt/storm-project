package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import main.StormConf;
import org.apache.log4j.Logger;
import util.FName;
import util.StreamId;
import util.TimeConst;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.*;

public class ClnMobile extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    Set<String> productSet = new HashSet<String>();
    Set<String> ticketSet = new HashSet<String>();
    String mobliePath = StormConf.MPAPERPATH;
    String ticketPath = StormConf.TICKETPATH;
    static Logger log = Logger.getLogger(ClnMobile.class);

    public ClnMobile() {
        getProductID();
        getTicket();
    }

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    private void getTicket() {
        SimpleDateFormat sf = new SimpleDateFormat(TimeConst.TICKETPATH);
        Calendar cal = new java.util.GregorianCalendar();
        String day = sf.format(cal.getTime());
        readTicket(day);
    }

    private void getProductID() {
        SimpleDateFormat sf = new SimpleDateFormat(TimeConst.PRODUCTIDPATH);
        Calendar cal = new java.util.GregorianCalendar();
        String day = sf.format(cal.getTime());
        readProduct(day);
    }

    public void execute(Tuple input) {
        try {
            String msisdn = input.getStringByField(FName.MSISDN.name());
            String platform = input.getStringByField(FName.PLATFORM.name());
            String ordertype = input.getStringByField(FName.ORDERTYPE.name());
            String product_id = input.getStringByField(FName.PRODUCT_ID.name());
            String book_id = input.getStringByField(FName.BOOK_ID.name());
            String province_id = input.getStringByField(FName.PROVINCE_ID
                .name());
            String realinforfee = input.getStringByField(FName.REALINFORFEE
                .name());
            String ticketid = input.getStringByField(FName.TICKETID.name());
            Boolean isdata = isMblData(product_id);
            Boolean ispay = true;
            if (!isdata) {
                if (!(ticketid == null || ticketid.equalsIgnoreCase("")))
                    ispay = isPay(ticketid);
                if (ispay)
                    collector.emit(new Values(msisdn, platform, ordertype,
                        product_id, book_id, province_id, realinforfee));
                else
                    log.info("useless ticketid : " + ticketid);
            } else {
                log.info("useless productID：" + product_id);
            }
        } catch (IllegalArgumentException e) {
            if (input.getSourceStreamId().equals(StreamId.SIGNALUPDATE.name())) {
                log.info("update moblie productID ....");
                getProductID();
                log.info("update ticketID ....");
                getTicket();
            }
        }
        collector.ack(input);
    }

    private Boolean isPay(String ticketid) {
        return !ticketSet.contains(ticketid);
    }

    private Boolean isMblData(String product_id) {
        return productSet.contains(product_id);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FName.MSISDN.name(), FName.PLATFORM.name(),
            FName.ORDERTYPE.name(), FName.PRODUCT_ID.name(), FName.BOOK_ID
            .name(), FName.PROVINCE_ID.name(), FName.REALINFORFEE
            .name()));
    }

    private void readProduct(String day) {
        ArrayList<File> files = new ArrayList<File>();
        String pathstr = mobliePath + day + TimeConst.PRODUCTIDMIN;
        File path = new File(pathstr);
        log.info("building path:" + pathstr);
        if (path.exists()) {
            listFiles(files, path);
            try {
                readProInfo(files);
                log.info("ProductID Info's size is :" + productSet.size());
                files.clear();
            } catch (Exception e) {
                log.error("ProductID FILE PATH：" + mobliePath
                    + " [ERROR] read file fail.");
            }
        }
    }

    private void readTicket(String day) {
        ArrayList<File> files = new ArrayList<File>();
        String pathstr = ticketPath + day + TimeConst.TICKETMIN;
        File path = new File(pathstr);
        log.info("building ticketid path:" + pathstr);
        if (path.exists()) {
            listFiles(files, path);
            try {
                readTicketInfo(files);
                log.info("ticketID Info's size is :" + ticketSet.size());
                files.clear();
            } catch (Exception e) {
                log.error("ticketID FILE PATH：" + ticketPath
                    + " [ERROR] read file fail.", e);
            }
        }
    }

    private void readTicketInfo(ArrayList<File> files) throws Exception {
        String id = "";
        int pay = 0;
        for (File file : files) {
            log.info("read TicketInfo's path:" + file.getAbsolutePath());
            FileReader fileReader = new FileReader(file);
            BufferedReader reader = new BufferedReader(fileReader);
            String str;
            while ((str = reader.readLine()) != null) {
                log.info("ticket id line:" + str);
                String[] words = str.split("\\|", -1);
                if (words.length >= 3) {
                    id = words[1];
                    try {
                        pay = Integer.parseInt(words[2].trim());
                    } catch (NumberFormatException e) {
                        log.error("ticket pay is not a number : " + words[2], e);
                    }
                    if (pay == 0) {
                        ticketSet.add(id);
                        log.info("ticketID :" + id);
                    }
                }
            }
            reader.close();
            fileReader.close();
        }
    }

    private void readProInfo(ArrayList<File> files) throws Exception {
        for (File file : files) {
            log.info("readProInfo's path:" + file.getAbsolutePath());
            FileReader fileReader = new FileReader(file);
            BufferedReader reader = new BufferedReader(fileReader);
            String str;
            while ((str = reader.readLine()) != null) {
                log.info("productid line:" + str);
                String[] words = str.split("\\|", -1);
                if (words.length >= 2) {
                    productSet.add(words[1]);
                    log.info("mobile productID:" + words[1]);
                }
            }
            reader.close();
            fileReader.close();
        }
    }

    private void listFiles(ArrayList<File> pathFiles, File path) {
        File[] lists = path.listFiles();
        int len = lists.length;
        for (int i = 0; i < len; i++) {
            File f = lists[i];
            if (f.isFile()) {
                pathFiles.add(f);
                log.info("productid filePath:" + f.getAbsolutePath());
            }
        }
    }

    public void cleanup() {
    }
}
