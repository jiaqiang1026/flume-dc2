package com.datax.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import org.apache.log4j.Logger;

/**
 * 时间处理工具类
 * @author jiaqiang
 * @date 2013.13.23
 */
public class DateUtil {

	private static Logger log = Logger.getLogger(DateUtil.class);
	
	/**
	 * 对日期进行格式化
	 * @param pattern 模式
	 * @param d 日期
	 * @return
	 */
	public static String format(String pattern, Date d) {
		String rtn = null;
		
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(pattern);
			rtn = sdf.format(d);
		} catch (Exception ex) {
			log.debug("解析日期异常", ex);
			ex.printStackTrace();
			rtn = null;
		}
		
		return rtn;
	}
	
	public static Date parse(String pattern, String str) {
		Date d = null;
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(pattern);
			d = sdf.parse(str);
		} catch (Exception ex) {
			log.debug("解析日期异常", ex);
			d = null;
		}
		
		return d;
	}
	
	public static Date parse(String pattern, Locale l, String str) {
		Date d = null;
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(pattern, l);
			d = sdf.parse(str);
		} catch (Exception ex) {
			log.debug("解析日期异常", ex);
			d = null;
		}
		
		return d;
	}
	
	public static String yesterday(String pattern) {
		Calendar c = Calendar.getInstance();
		c.add(Calendar.DAY_OF_MONTH, -1);
		
		return format(pattern, c.getTime());
	}
	
	public static int getCurrHour() {
		Calendar c = Calendar.getInstance();
		return c.get(Calendar.HOUR_OF_DAY);
	}

	//获取日期带小时，格式:"yyyy-MM-dd HH"
	public static String getDateHour() {
		return format("yyyy-MM-dd HH", new Date());
	}
	
	//获取日期带小时，格式:"yyyy-MM-dd HH"
	public static String getDateHour(Date d) {
		return format("yyyy-MM-dd HH", d);
	}
	
	//获取日期带小时，格式:"yyyy-MM-dd HH"
	public static String getDateHour(String pattern, String date) {
		return format("yyyy-MM-dd HH", parse(pattern,date));
	}
	
	//获取dateHour对应的下一个 日期小时
	//currDateHour格式是yyyy-MM-dd HH
	public static String nextDateHour(String currDateHour) {
		Date d = parse("yyyy-MM-dd HH", currDateHour);
		Calendar c = Calendar.getInstance();
		c.setTime(d);
		
		c.add(Calendar.HOUR_OF_DAY, 1);
		
		return format("yyyy-MM-dd HH", c.getTime());
	}
	
	
	//获取dateHour对应的下一个 日期小时
	//currDateHour格式是yyyy-MM-dd HH
	public static String nextDateHour(String pattern, String currDateHour) {
		Date d = parse(pattern, currDateHour);
		Calendar c = Calendar.getInstance();
		c.setTime(d);
		
		c.add(Calendar.HOUR_OF_DAY, 1);
		
		return format(pattern, c.getTime());
	}
	
	//获取dateHour对应的下一个 日期小时
	//param:currDateHour格式是yyyy-MM-dd HH
	//param: back后退小时数
	public static String backDateHour(String currDateHour, int back) {
		Date d = parse("yyyy-MM-dd HH", currDateHour);
		Calendar c = Calendar.getInstance();
		c.setTime(d);
		
		c.add(Calendar.HOUR_OF_DAY, -back);
		
		return format("yyyy-MM-dd HH", c.getTime());
	}
	
	
	//获取dateHour对应的下一个 日期小时
	//param:currDateHour格式是yyyy-MM-dd HH
	//param: back后退小时数
	public static String preDateHour(String pattern, String currDateHour, int back) {
		Date d = parse(pattern, currDateHour);
		Calendar c = Calendar.getInstance();
		c.setTime(d);
		
		c.add(Calendar.HOUR_OF_DAY, -back);
		
		return format(pattern, c.getTime());
	}
		
	
	/**
	 * 获取currDate的前back天日期，
	 * @param currDate 格式:yyyy-MM-dd
	 * @param back 回退天数
	 * @return yyyy-MM-dd格式的日期
	 */
	public static String preDate(String currDate, int back) {
		Date d = parse("yyyy-MM-dd", currDate);
		Calendar c = Calendar.getInstance();
		c.setTime(d);
		
		c.add(Calendar.DAY_OF_MONTH, -back);
		
		return format("yyyy-MM-dd", c.getTime());
	}
	

	/**
	 * 获取currDate的前back天日期
	 * @param pattern 日期格式
	 * @param currDate 格式:yyyy-MM-dd
	 * @param back 回退天数
	 * @return yyyy-MM-dd格式的日期
	 */
	public static String preDate(String pattern, String currDate, int back) {
		Date d = parse(pattern, currDate);
		Calendar c = Calendar.getInstance();
		c.setTime(d);
		
		c.add(Calendar.DAY_OF_MONTH, -back);
		
		return format(pattern, c.getTime());
	}
	
	//获取date对应的下一个 日期
	//currDate格式是yyyy-MM-dd
	public static String nextDate(String currDate) {
		Date d = parse("yyyy-MM-dd", currDate);
		Calendar c = Calendar.getInstance();
		c.setTime(d);
		
		c.add(Calendar.DAY_OF_MONTH, 1);
		
		return format("yyyy-MM-dd", c.getTime());
	}
	
	//currDate格式是yyyy-MM-dd
	public static String nextDate(String pattern, String currDate) {
		Date d = parse(pattern, currDate);
		Calendar c = Calendar.getInstance();
		c.setTime(d);
		
		c.add(Calendar.DAY_OF_MONTH, 1);
		
		return format(pattern, c.getTime());
	}
	
	public static void sleep(int seconds) {
		try {
			Thread.currentThread().sleep(seconds * 1000);
		} catch (InterruptedException e) {
		}
	}
	
	public static void main(String[] args) throws ParseException {
//		System.out.println(DateUtil.format("yyyy-MM-dd HH", new Date()));
//		System.out.println(DateUtil.parse("yyyy-MM-dd", "2013-12-23"));
//		System.out.println(DateUtil.getCurrHour());
//		System.out.println(DateUtil.nextDateHour("2014-01-07 23"));
		//System.out.println(DateUtil.backDateHour("2014-01-07 00", 1));
		//System.out.println(DateUtil.format("yyyy-MM-dd HH:mm:ss", new Date()));
		
		
		
		String s = "25/Nov/2014:18:52:39 +0800";
//		String s = "25/Nov/2014:18:52:39";
		SimpleDateFormat format = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
//		format.setTimeZone(TimeZone.getTimeZone("GMT+8")); 
		Date d = format.parse(s);
		
		System.out.println(DateUtil.format("yyyyMMdd HH:mm:ss", d));
	}
	
	
}
