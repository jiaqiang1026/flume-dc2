package com.datax.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * 配置文件读取工具类
 * @author jiaqiang
 * 2013.08.27
 */
public class ConfigUtil {

	private static Logger log = Logger.getLogger(ConfigUtil.class);
	
	//发送者手机号
	public static final String FETION_FROM_PHONE_NUM = "fetion.from.phone.number";
	
	//发送者飞信密码
	public static final String FETION_FROM_PHONE_PWD = "fetion.from.phone.pwd";
		
	//接收者手机号列表
	public static final String FETION_TO_PHONE_NUM_LIST = "fetion.to.phone.number.list";
	
	//datanode最大使用空间占比
	public static final String DATANODE_MAX_USED_SPACE_PERCENT = "datanode.max.used.space.percent";

	//集群最大使用空间占比
	public static final String CLUSTER_MAX_USED_SPACE_PERCENT = "cluster.max.used.space.percent";
		
	private static Properties p = new Properties();
	
//	static {
//		//加载用户自定义配置文件
//		boolean succ = loadUserConf();
//		if (!succ) { //
//			loadDefaultConf();
//		}
//		
//		log.info("加载配置文件[" + (succ ? "成功]": "失败]"));
//	}
	
	/**
	 * 加载用户自定义的配置(/data/report/conf/reporter.properties)
	 */
	private static boolean loadUserConf() {
		boolean succ = true;
		File confFile = new File("/data/report/conf/reporter.properties");
		if (!confFile.exists()) {
			log.debug("用户自定义hadoop监控配置文件[" + confFile.getAbsolutePath() + "]不存在！");
			return false;
		}
		
		InputStream in = null;
		try {
			in = new FileInputStream(confFile);
			p.load(in);
		} catch (IOException ex) {
			ex.printStackTrace();
			succ = false;
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return succ;
	}
	
	/**
	 * 加载默认配置,类路径
	 */
	private static void loadDefaultConf() {
		InputStream in = ConfigUtil.class.getClassLoader().getResourceAsStream("reporter.properties");
		
		try {
			p.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		} 
		
		if (in != null) {
			try {
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static String getString(String name) {
		return p.getProperty(name);
	}
	
	/**
	 * 获取某属性值，无则返回空字符串
	 * @param name
	 * @return
	 */
	public static String getProperty(String name) {
		return p.getProperty(name,"");
	}
	
	/**
	 * 获取某属性值，无则返回空字符串
	 * @param name
	 * @return
	 */
	public static String getProperty(String name, String charset) {
		String value = p.getProperty(name,"");
		
		try {
			return new String(value.getBytes(),charset);
		} catch (UnsupportedEncodingException e) {
			log.error(e);
		}
		
		return value;
	}
	
	/**
	 * 获取某属性值，无则返回空字符串
	 * @param name
	 * @return
	 */
	public static String getProperty(String name, String inCharset, String outCharset) {
		String value = p.getProperty(name,"");
		
		try {
			return new String(value.getBytes(inCharset), outCharset);
		} catch (UnsupportedEncodingException e) {
			log.error(e);
		}
		
		return value;
	}
	
	/**
	 * 获取某属性对应的整型值
	 * @param name
	 * @return 
	 */
	public static Integer getInt(String name) {
		String value = getProperty(name);
		
		Integer rtn = null;
		
		value = value.trim();
		if (value.length() == 0) {
			rtn = null;
		} else {
			try {
				rtn = Integer.parseInt(value);
			} catch (Exception ex) {
				ex.printStackTrace();
				rtn = null;
			}
		}
		
		return rtn;
	}
	
	/**
	 * 获取某属性对应的长整型值
	 * @param name
	 * @return 
	 */
	public static long getLong(String name) {
		String value = getProperty(name);
		
		long rtn = 0;
		
		value = value.trim();
		if (value.length() == 0) {
			rtn = 0;
		} else {
			try {
				rtn = Long.parseLong(value);
			} catch (Exception ex) {
				ex.printStackTrace();
				rtn = 0;
			}
		}
		
		return rtn;
	}

	/**
	 * 获取某属性对应的浮点值
	 * @param name
	 * @return 
	 */
	public static Double getDouble(String name) {
		String value = p.getProperty(name);
		if (value == null) {
			return null;
		}
		
		Double rtn = null;
		
		value = value.trim();
		if (value.length() == 0) {
			rtn = null;
		} else {
			try {
				rtn = Double.parseDouble(value);
			} catch (Exception ex) {
				log.debug("解析属性[" + name + "]为浮点值失败", ex);
				rtn = null;
			}
		}
		
		return rtn;
	}
	
	/**
	 * 获取某属性对应的浮点值
	 * @param name
	 * @return 
	 */
	public static Float getFloat(String name) {
		String value = p.getProperty(name, "1.0");
		
		Float rtn = null;
		
		value = value.trim();
		if (value.length() == 0) {
			rtn = null;
		} else {
			try {
				rtn = Float.parseFloat(value);
			} catch (Exception ex) {
				log.debug("解析属性[" + name + "]为浮点值失败", ex);
				rtn = null;
			}
		}
		
		return rtn;
	}
	
	/**
	 * 获取某属性对应的浮点值
	 * @param name 
	 * @return 非"true"返回false
	 */
	public static Boolean getBoolean(String name) {
		if (name == null) {
			return null;
		}
		
		name = name.trim();
		if (name.length() == 0) {
			return null;
		} 
	
		String v = getProperty(name).trim();
		if (v.length() == 0) {
			return null;
		}
		
		try {
			Boolean b = new Boolean(v);
			return b;
		} catch (Exception ex) {
			log.debug("解析属性[" + name + "]为Boolean失败", ex);
		}
	
		return null;
	}
	
	/**
	 * 加载指定的配置文件
	 * @param confFile
	 * @return
	 */
	public static boolean loadConfigFile(String confFile) {
		boolean succ = true;
		File cf = new File(confFile);
		if (!cf.exists()) {
			log.debug("加载的配置文件[" + confFile + "]不存在！");
			return false;
		}
		
		InputStream in = null;
		try {
			in = new FileInputStream(cf);
			p.load(in);
		} catch (IOException ex) {
			ex.printStackTrace();
			succ = false;
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return succ;
	}
	
	/**
	 * 加载属性文件
	 * @param confFile
	 * @param prop
	 * @return
	 */
	public static boolean loadConfigFile(String confFile, Properties prop) {
		boolean succ = true;
		File cf = new File(confFile);
		if (!cf.exists()) {
			log.debug("加载的配置文件[" + confFile + "]不存在！");
			return false;
		}
		
		InputStream in = null;
		try {
			in = new FileInputStream(cf);
			prop.load(in);
		} catch (IOException ex) {
			ex.printStackTrace();
			succ = false;
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return succ;
	}
	
	public static String getHostname() {
		String hostname = "localhost";
		
		try {
			InetAddress ia = InetAddress.getLocalHost();
			hostname = ia.getHostName();
		} catch (UnknownHostException e) {
			log.error(e);
		}
		
		return hostname;
	}
	
	public static void main(String[] args) throws UnsupportedEncodingException {
		System.out.println(ConfigUtil.getString("hiveserver_passwd"));
	}
}
