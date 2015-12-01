package spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import util.FName;
import util.STime;
import util.TimeConst;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Map;

public class FilesRead1min extends BaseRichSpout {
    private static final long serialVersionUID = 1L;
    private SpoutOutputCollector collector;
    public String mainDirName;// main Dir name is like as ".../XXX/"
    transient Calendar cal = null;
    public SimpleDateFormat format = new SimpleDateFormat(TimeConst.DATEFORMAT);
    public int lastT = -1;
    public SimpleDateFormat ftime = new SimpleDateFormat(TimeConst.MM);
    Boolean first = false;
    static Logger log = Logger.getLogger(FilesRead1min.class);

    public FilesRead1min(String mainDirName) {
        this.mainDirName = mainDirName;
    }

    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
    }

    public void nextTuple() {
        buildStartTime();
        Calendar ctime = Calendar.getInstance();
        int min = Integer.parseInt(ftime.format(ctime.getTime()));
        if (min % TimeConst.PERIODTIME == 0) {
            first = true;
        }
        int timedif = min - lastT;
        lastT = min;
        if ((timedif != 0) && (first)) {
            log.info("secondNeed pv 1 coming:" + min + "  lastT:" + lastT
                + " timedif: " + timedif + "  first:" + first);
            ArrayList<File> files = new ArrayList<File>();
            String pathname = buildPathAdd(mainDirName);
            File path = new File(pathname);
            if (path.exists()) {
                listFiles(files, path);
                try {
                    readFileAndEmitData(files);
                    files.clear();
                } catch (Exception e) {
                    log.error("[ERROR] secondNeed pv read file fail.");
                }
            }
            log.info("secondNeed pv file read end");
        }
        try {
            Thread.sleep(TimeConst.FileDif);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void buildStartTime() {
        if (cal == null) {
            log.info("fileSpout1min initialize the Calendar cal！");
            cal = STime.buildCal();
        }
    }

    private void readFileAndEmitData(ArrayList<File> files) throws Exception {
        for (File file : files) {
            FileReader fileReader = new FileReader(file);
            BufferedReader reader = new BufferedReader(fileReader);
            String str;
            while ((str = reader.readLine()) != null) {
                this.collector.emit(new Values(str));
            }
            reader.close();
            fileReader.close();
        }
    }

    private String buildPathAdd(String mainDirName) {
        String datePath = mainDirName + format.format(cal.getTime());
        cal.add(Calendar.MINUTE, +1);
        return datePath;
    }

    // path is needed as like ".../XXX/20130106/0001/"
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
        declarer.declare(new Fields(FName.LINE.name()));
    }

    public void close() {
    }

    public void ack(Object msgId) {
        super.ack(msgId);
    }
}
