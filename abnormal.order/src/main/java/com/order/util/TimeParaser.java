package com.order.util;

import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by LiMingji on 2015/5/26.
 */
public class TimeParaser {
    private static Logger log = Logger.getLogger(TimeParaser.class);

    /**
     * @param recordTime 输入样例 20150505165523
     * @return 从recordTime到1970年的毫秒数
     */
    public static long splitTime(String recordTime) {
        Calendar calendar = Calendar.getInstance();
        try {
            int year = Integer.parseInt(recordTime.substring(0, 4));
            int month = Integer.parseInt(recordTime.substring(4, 6));
            int date = Integer.parseInt(recordTime.substring(6, 8));
            int hour = Integer.parseInt(recordTime.substring(8, 10));
            int minute = Integer.parseInt(recordTime.substring(10, 12));
            int seconds = Integer.parseInt(recordTime.substring(12, 14));

            calendar.set(year, month - 1, date, hour, minute, seconds);
            return calendar.getTimeInMillis();

        } catch (Exception e) {
            log.error("时间输入格式有问题: " + e);
        }
        return -1L;
    }

    //根据long型构造符合条件的日期格式
    public static String formatTimeInDay(Long inputTime) {
        SimpleDateFormat sFormat = new SimpleDateFormat("yyyyMMdd");
        Date date = new Date(inputTime);
        return sFormat.format(date);
    }

    public static String formatTimeInSeconds(Long inputTime) {
        SimpleDateFormat sFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        Date date = new Date(inputTime);
        return sFormat.format(date);
    }

    /**
     * 将输入时间提前一个小时
     *
     * @param time
     * @return
     */
    public static String OneHourAgo(Long time) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        calendar.add(Calendar.HOUR, -1);
        Date date = calendar.getTime();
        SimpleDateFormat sFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        return sFormat.format(date);
    }

    /**
     * 将时间调整为今天0点
     *
     * @param time
     * @return
     */
    public static String OneDayAgo(Long time) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        Date date = calendar.getTime();
        SimpleDateFormat sFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        return sFormat.format(date);
    }

    /**
     * 将时间调整为今天0点
     *
     * @param time
     * @return
     */
    public static String NormalHourAgo(Long time) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        Date date = calendar.getTime();
        SimpleDateFormat sFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        return sFormat.format(date);
    }

    //获取从当前时间到明天凌晨三天的毫秒数
    public static long getMillisFromNowToThreeOclock() {
        Calendar calendar = Calendar.getInstance();
        Calendar calendar3Oclock = Calendar.getInstance();
        System.out.println(calendar.getTime());
        calendar3Oclock.add(Calendar.DATE, 1);
        calendar3Oclock.set(Calendar.HOUR_OF_DAY, 3);
        calendar3Oclock.set(Calendar.MINUTE, 0);
        calendar3Oclock.set(Calendar.SECOND, 0);
        calendar3Oclock.set(Calendar.MILLISECOND, 0);
        System.out.println(calendar3Oclock.getTime());
        return calendar3Oclock.getTimeInMillis() - calendar.getTimeInMillis();
    }

    public static long getMillisFromTimeToNetFiveMinutes(long time) {
        Calendar now = Calendar.getInstance();
        Calendar next = Calendar.getInstance();
        next.setTimeInMillis(time);
        next.add(Calendar.MINUTE, 5);
        return next.getTimeInMillis() - now.getTimeInMillis();
    }

    public static boolean isTimeToClearData(long time) {
        Calendar now = Calendar.getInstance();
        now.setTimeInMillis(time);
        int hour = now.get(Calendar.HOUR);
        int minute = now.get(Calendar.MINUTE);
        return hour == 0 && minute == 0;
    }

    public static void main(String[] args) {
//        Long time =1433494523823L;
//        System.out.println(formatTimeInDay(time));
//        System.out.println(formatTimeInSeconds(time));

//        System.out.println(getMillisFromNowToThreeOclock());

//        System.out.println(getMillisFromTimeToNetFiveMinutes(System.currentTimeMillis()));
//        System.out.println(NormalHourAgo(System.currentTimeMillis()));

        System.out.println(isTimeToClearData(System.currentTimeMillis()));

    }
}
