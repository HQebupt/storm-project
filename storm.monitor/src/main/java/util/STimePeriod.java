package util;

import org.apache.log4j.Logger;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class STimePeriod implements Serializable {
    private static final long serialVersionUID = -8475597005247276139L;
    static Logger log = Logger.getLogger(STimePeriod.class);

    private static String getStartTime() {
        SimpleDateFormat ftime = new SimpleDateFormat(TimeConst.MM);
        SimpleDateFormat sf = new SimpleDateFormat(TimeConst.YYMMDDHHMM);
        Calendar cal = new java.util.GregorianCalendar();
        int min = Integer.parseInt(ftime.format(cal.getTime()));
        int sub = min % TimeConst.PERIODUNIQUE + TimeConst.PERIODUNIQUE;
        cal.add(Calendar.MINUTE, -sub);
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
        String time = STimePeriod.getStartTime();
        log.info("startTime:" + time);
        return STimePeriod.buildStartTime(time);
    }

    private static Calendar bdSigTime(String startTime) {
        int year = Integer.valueOf(startTime.substring(0, 4));
        int month = Integer.valueOf(startTime.substring(4, 6)) - 1;
        int day = Integer.valueOf(startTime.substring(6, 8));
        int hour = Integer.valueOf(startTime.substring(8, 10));
        int min = Integer.valueOf(startTime.substring(10, 12));
        Calendar cal = new java.util.GregorianCalendar(year, month, day, hour,
            min, 0);
        cal.add(Calendar.MINUTE, -TimeConst.PERIODUNIQUE);
        return cal;
    }

    public static Calendar buildSig() {
        String time = STimePeriod.getStartTime();
        log.info("startTime:" + time);
        return STimePeriod.bdSigTime(time);
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

    public static Calendar buildSig1min() {
        String time = STimePeriod.getStartTime();
        log.info("startTime:" + time);
        return STimePeriod.bdSig1min(time);
    }

}
