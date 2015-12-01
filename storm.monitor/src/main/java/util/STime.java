package util;

import org.apache.log4j.Logger;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class STime implements Serializable {
    private static final long serialVersionUID = -8475597005247276139L;
    static Logger log = Logger.getLogger(STime.class);

    private static String getStartTime() {
        SimpleDateFormat ftime = new SimpleDateFormat(TimeConst.MM);
        SimpleDateFormat sf = new SimpleDateFormat(TimeConst.YYMMDDHHMM);
        Calendar cal = new java.util.GregorianCalendar();
        int min = Integer.parseInt(ftime.format(cal.getTime()));
        int sub = min % TimeConst.PERIODTIME + TimeConst.PERIODTIME;
        cal.add(Calendar.MINUTE, -sub);
        log.info("STime method is invoked once！");
        return sf.format(cal.getTime());
    }

    private static Calendar buildStartTime(final String startTime) {
        int year = Integer.valueOf(startTime.substring(0, 4));
        int month = Integer.valueOf(startTime.substring(4, 6)) - 1;
        int day = Integer.valueOf(startTime.substring(6, 8));
        int hour = Integer.valueOf(startTime.substring(8, 10));
        int min = Integer.valueOf(startTime.substring(10, 12));
        Calendar cal = new java.util.GregorianCalendar(year, month, day, hour,
            min);
        return cal;
    }

    // build fileSpout's start time.
    public static Calendar buildCal() {
        return STime.buildStartTime(STime.getStartTime());
    }

    private static Calendar bdSigTime(String startTime) {
        int year = Integer.valueOf(startTime.substring(0, 4));
        int month = Integer.valueOf(startTime.substring(4, 6)) - 1;
        int day = Integer.valueOf(startTime.substring(6, 8));
        int hour = Integer.valueOf(startTime.substring(8, 10));
        int min = Integer.valueOf(startTime.substring(10, 12));
        Calendar cal = new java.util.GregorianCalendar(year, month, day, hour,
            min, 0);
        cal.add(Calendar.MINUTE, -TimeConst.PERIODTIME);
        return cal;
    }

    // build signal's start time. include signalDB,signal15min,signalSort. and
    // the
    // signalDB and signal15min should be the same forever;
    public static Calendar buildSig() {
        return STime.bdSigTime(STime.getStartTime());
    }

    private static Calendar bdSig1min(String startTime) {
        int year = Integer.valueOf(startTime.substring(0, 4));
        int month = Integer.valueOf(startTime.substring(4, 6)) - 1;
        int day = Integer.valueOf(startTime.substring(6, 8));
        int hour = Integer.valueOf(startTime.substring(8, 10));
        int min = Integer.valueOf(startTime.substring(10, 12));
        Calendar cal = new java.util.GregorianCalendar(year, month, day, hour,
            min, 0);
        cal.add(Calendar.MINUTE, -TimeConst.PTIME1);
        return cal;
    }

    // build signal 1min 's start time.
    public static Calendar buildSig1min() {
        return STime.bdSig1min(STime.getStartTime());
    }

}
