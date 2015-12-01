package util;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Map;
import java.util.Set;

//InfoUpdate.updateInfo(String path, Set set)
//InfoUpdate.updateInfo(String path, Map map)
public class InfoUpdate implements Serializable {

    private static final long serialVersionUID = 4889563566919102436L;
    static Logger log = Logger.getLogger(InfoUpdate.class);

    public static void updateBookInfo(String infoPath, Map<String, String> map) {
        log.info("updating...");
        SimpleDateFormat sf = new SimpleDateFormat("/yyyyMMdd/");
        Calendar cal = new java.util.GregorianCalendar();
        String day = sf.format(cal.getTime());
        ArrayList<File> files = new ArrayList<File>();
        String pathstr = infoPath + day;
        File path = new File(pathstr);
        log.info("building Info path:" + pathstr);
        if (path.exists()) {
            listFiles(files, path);
            try {
                readBookInfo(files, map);// volatility
                log.info("Info's size is :" + map.size());
                files.clear();
            } catch (Exception e) {
                log.info("Info file path：" + infoPath
                    + " [ERROR] read file fail.");
            }
        }
    }

    private static void readBookInfo(ArrayList<File> files,
                                     Map<String, String> map) throws Exception {
        for (File file : files) {
            log.info("readInfo's path:" + file.getAbsolutePath());
            FileReader fileReader = new FileReader(file);
            BufferedReader reader = new BufferedReader(fileReader);
            String str;
            while ((str = reader.readLine()) != null) {
                String[] words = str.split("\\|", -1);// volatility
                if (words.length >= 3) {// volatility
                    String itemID = words[1];
                    String itemType = words[2];
                    map.put(itemID, itemType);
                }
            }
            reader.close();
            fileReader.close();
        }
    }

    public static void updateChapterInfo(String infoPath,
                                         Map<String, String> map) {
        log.info("updating...");
        SimpleDateFormat sf = new SimpleDateFormat(TimeConst.PRODUCTIDPATH);
        Calendar cal = new java.util.GregorianCalendar();
        String day = sf.format(cal.getTime());
        ArrayList<File> files = new ArrayList<File>();
        String pathstr = infoPath + day;
        File path = new File(pathstr);
        log.info("building Info path:" + pathstr);
        if (path.exists()) {
            listFiles(files, path);
            try {
                readChapterInfo(files, map);// volatility
                log.info("Info's size is :" + map.size());
                files.clear();
            } catch (Exception e) {
                log.info("Info file path：" + infoPath
                    + " [ERROR] read file fail.");
            }
        }
    }

    private static void readChapterInfo(ArrayList<File> files,
                                        Map<String, String> map) throws Exception {
        for (File file : files) {
            log.info("readInfo's path:" + file.getAbsolutePath());
            FileReader fileReader = new FileReader(file);
            BufferedReader reader = new BufferedReader(fileReader);
            String str;
            while ((str = reader.readLine()) != null) {
                String[] words = str.split("\\|", -1);
                if (words.length >= 4) {
                    String book_id = words[1];
                    String type_id = words[2];
                    String class_id = words[3];
                    String com = type_id.trim() + "|" + class_id.trim();
                    map.put(book_id, com);
                }
            }
            reader.close();
            fileReader.close();
        }
    }

    public static void updateInfo(String infoPath, Set<String> set) {
        log.info("updating...");
        SimpleDateFormat sf = new SimpleDateFormat(TimeConst.PRODUCTIDPATH);
        Calendar cal = new java.util.GregorianCalendar();
        String day = sf.format(cal.getTime());
        ArrayList<File> files = new ArrayList<File>();
        String pathstr = infoPath + day;
        File path = new File(pathstr);
        log.info("building Info path:" + pathstr);
        if (path.exists()) {
            listFiles(files, path);
            try {
                readInfo(files, set);
                log.info("Info's size is :" + set.size());
                files.clear();
            } catch (Exception e) {
                log.info("Info file path：" + infoPath
                    + " [ERROR] read file fail.");
            }
        }
    }

    private static void readInfo(ArrayList<File> files, Set<String> set)
        throws Exception {
        for (File file : files) {
            log.info("readInfo's path:" + file.getAbsolutePath());
            FileReader fileReader = new FileReader(file);
            BufferedReader reader = new BufferedReader(fileReader);
            String str;
            while ((str = reader.readLine()) != null) {
                String[] words = str.split("\\|", -1);// volatility
                if (words.length >= 2) {// volatility
                    set.add(words[1]);
                }
            }
            reader.close();
            fileReader.close();
        }
    }

    // support only 2 deep level dir file
    private static void listFiles(ArrayList<File> pathFiles, File path) {
        File[] lists = path.listFiles();
        int len = lists.length;
        for (int i = 0; i < len; i++) {
            File f = lists[i];
            if (f.isFile()) {
                pathFiles.add(f);
                log.info("info filePath:" + f.getAbsolutePath());
            } else {
                File[] subs = f.listFiles();
                int flen = subs.length;
                for (int j = 0; j < flen; j++) {
                    File subf = subs[j];
                    if (subf.isFile()) {
                        pathFiles.add(subf);
                        log.info("info filePath:" + f.getAbsolutePath());
                    }
                }
            }
        }
    }
}
