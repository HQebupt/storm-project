package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import util.FName;
import util.TimeConst;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ProClean extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    OutputCollector collector;
    Map<String, String> proInfo = new HashMap<String, String>();
    static Logger log = Logger.getLogger(ProClean.class);

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public ProClean() {
        readPro();
    }

    public void execute(Tuple input) {
        String msisdn = input.getStringByField(FName.MSISDN.name());
        if (msisdn.length() >= 7) {
            String ms = msisdn.substring(0, 7);
            String provinceID = proInfo.get(ms);
            if (provinceID != null) {
                collector.emit(new Values(msisdn, provinceID));
            }
        } else {
            log.info("msisdn length is less than 7,throw it：" + msisdn);
        }
        collector.ack(input);
    }

    private void readPro() {
        ArrayList<File> files = new ArrayList<File>();
        String proPath = TimeConst.PROPATH;
        File path = new File(proPath);
        if (path.exists()) {
            listFiles(files, path);
            try {
                readProInfo(files);
                log.info("ProvinceInfo's size is :" + proInfo.size());
                files.clear();
            } catch (Exception e) {
                log.error("proInfo FILE PATH：" + proPath
                    + " [ERROR] read file fail.");
            }
        }
    }

    private void readProInfo(ArrayList<File> files) throws Exception {
        for (File file : files) {
            FileReader fileReader = new FileReader(file);
            BufferedReader reader = new BufferedReader(fileReader);
            String str;
            while ((str = reader.readLine()) != null) {
                String[] words = str.split("\\|", -1);
                if (words.length >= 2) {
                    proInfo.put(words[0], words[1]);
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
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FName.MSISDN.name(), FName.PROVINCE_ID
            .name()));
    }
}
