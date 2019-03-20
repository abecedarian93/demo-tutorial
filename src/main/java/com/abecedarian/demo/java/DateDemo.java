package com.abecedarian.demo.java;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by abecedarian on 2019/3/20
 */
public class DateDemo {

    public static void main(String[] args) {

    }

    //获取今天日期
    public static Date getToday() {

        Calendar cal = Calendar.getInstance();
        return cal.getTime();
    }

    //日期转字符串
    public static String date2Str(Date dt, String format) {

        return new SimpleDateFormat(format).format(dt);
    }

    //字符串转日期
    public static Date str2Date(String str, String format) throws ParseException {

        return new SimpleDateFormat(format).parse(str);
    }

    //获取指定日期星期几
    public static String getWeek(Date dt) {

        String[] weekDays = {"星期日", "星期一", "星期二", "星期三", "星期四", "星期五", "星期六"};
        Calendar cal = Calendar.getInstance();
        cal.setTime(dt);
        int w = cal.get(Calendar.DAY_OF_WEEK) - 1;
        if (w < 0)
            w = 0;
        return weekDays[w];
    }

    //两个日期时间间隔
    public static long interival(Date dt1, Date dt2) {

        return dt1.getTime() - dt2.getTime();
    }

    //指定日期增减天数
    public static Date addDay(Date dt, int day) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(dt);

        cal.add(Calendar.DATE, day);

        return cal.getTime();
    }

    //指定日期增减月份
    public static Date addMonth(Date dt, int month) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(dt);

        cal.add(Calendar.MONTH, month);
        return cal.getTime();
    }


}
