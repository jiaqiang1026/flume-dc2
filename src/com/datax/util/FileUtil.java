package com.datax.util;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.log4j.Logger;

/**
 * 文件操作工具类
 * @author jiaqiang
 * @date 2013.12.17
 */
public class FileUtil {
	
	static Logger log = Logger.getLogger(FileUtil.class);
	
	/**
	 * 将lines行内容写入文件path中
	 * @param path   文件路径
	 * @param lines  内容行数
	 * @return true|false
	 */
	public static boolean writeLines(File path, List<String> lines, boolean append) {
		if (path == null || lines == null || lines.size() == 0) {
			log.debug("FileUtil::write(File,List<String>,append)参数为空.");
			return false;
		} 
		
		boolean succ = true;
		PrintWriter writer = null;
		
		try {
			writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path, append), "utf-8"), append);
			String line = null;
			for (int i = 0,size=lines.size(); i < size; i++) {
				line = lines.get(i);
				//write
				writer.println(line);
				writer.flush();
			}
		} catch (Exception ex) {
			log.error(ex);
			succ = false;
		} finally {
			if (writer != null) {
				writer.close();
			}
		}
		
		return succ;
	}
	
	/**
	 * 将lines行内容写入文件path中
	 * @param path   文件路径
	 * @param lines  内容行数
	 * @return true|false
	 */
	public static boolean writeLine(File path, String line, boolean append) {
		if (path == null || line == null) {
			log.debug("FileUtil::write(File,String)参数为空.");
			return false;
		}
		
		List<String> lines = new ArrayList<String>();
		lines.add(line);
		
		boolean succ = true;
		succ =  writeLines(path, lines, append);
		lines.clear();
		lines = null;
		
		return succ;
	}
	
	
	/**
	 * 读取文件，返回内容行
	 * @param path  文件绝对路径
	 * @return 内容
	 */
	public static List<String> readLines(String path) {
		if (path == null || path.trim().length() == 0) {
			log.debug("FileUtil::readLines(String)参数为空.");
			return null;
		}
		
		File filePath = new File(path);
		if (!filePath.exists()) {
			log.debug("FileUtil::readLines(String) 文件[" + path + "]不存在");
			return null;
		}
		
		List<String> rtn = new ArrayList<String>(200);
		BufferedReader in = null;
		
		try {
			in = new BufferedReader(new FileReader(filePath));
			String line = null;
			
			while ((line = in.readLine()) != null) {
				rtn.add(line);
			}
		} catch (IOException ex) {
			log.error("FileUtil::readLines(String) 异常",ex);
			rtn.clear();
			rtn = null;
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return rtn;
	}
	
	
	/**
	 * 读取文件，返回内容行
	 * @param path  文件绝对路径
	 * @return 内容
	 */
	public static List<String> readLines(File path) {
		if (path == null) {
			log.debug("FileUtil::readLines(File)参数为空.");
			return null;
		}
		
		return readLines(path.getAbsolutePath());
	}
	
	/**
	 * 读取首行内容
	 * @param path
	 * @return
	 */
	public static String readFirstLine(File path) {
		if (path == null) {
			log.debug("FileUtil::readLine(File)参数为空.");
			return null;
		}
		
		List<String> rtn = readLines(path.getAbsolutePath());
		String line = (rtn == null ? null : rtn.get(0));
		
		if (rtn != null) {
			rtn.clear();
			rtn = null;
		}
		
		return line;
	}
	
	
	public static boolean createNewFile(String path) {
		if (path == null) {
			log.debug("FileUtil::createNewFile(String)参数为空.");
			return false;
		}
		
		boolean succ = true;
		File f = new File(path);
		if (!f.exists()) {
			try {
				f.createNewFile();
			} catch (IOException e) {
				log.error("创建文件["+path+"]失败", e);
				succ = false;
			}
		}
		
		return succ;
	}
	
	public static void close(InputStream in) {
		if (in != null) {
			try {
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void close(OutputStream out) {
		if (out != null) {
			try {
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void close(Reader in) {
		if (in != null) {
			try {
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static boolean writeLong(Long v, File f) {
		if (v == null || f == null) {
			throw new NullPointerException("param is null");
		}
		
		if (!f.exists()) {
			if (!createNewFile(f.getAbsolutePath())) {
				return false;
			}
		}
		
		boolean succ = true;
		DataOutputStream dout = null;
		
		try {
			dout = new DataOutputStream(new FileOutputStream(f));
			dout.writeLong(v);
		} catch (IOException e) {
			log.error(e);
			succ = false;
		} finally {
			close(dout);
		}
		
		return succ;
	}
	
	public static Long readLong(File f) {
		if (f == null) {
			throw new NullPointerException("param is null");
		}
		
		if (!f.exists()) {
			return null;
		}
		
		Long v = null;
		DataInputStream din = null;
		
		try {
			din = new DataInputStream(new FileInputStream(f));
			v = din.readLong();
		} catch (IOException e) {
			log.error(e);
		} finally {
			close(din);
		}
		
		return v;
	}
	
	/*
	 * ip STRING,
     * date_time STRING,
     * method STRING,
     * host STRING,
     * url STRING,
     * para STRING,
     * refer STRING,
     * http_ver STRING,
     * response_code STRING,
     * response_length STRING,
     * cookie STRING,
     * user_agent STRING
	 * sample recrod:222.175.197.45 - - [21/Mar/2016:10:51:37 +0800] "GET /crc?t=hw&m=0&a=0&v=2.0&s=AzM9smJzgzMwczMykTN1cTNzkjN4ITN4UDNxEjYj1jYjZCM9QmJwETPoRnJ1EDNzkjN4ITN4UDNx0DdmUzN0EjM4UjNyETPohmJt92YuoHe4pHZuc3d31Da1FC HTTP/1.1" 404 282 "http://www.dzxxz.com/" "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET4.0C; .NET4.0E; 360SE)"
	 */
	public static String parse(final String line) {
		StringBuilder buff = new StringBuilder(200);
		
		//ip
		int lIdx = line.indexOf(" ");
		String ip = line.substring(0, lIdx).trim();
		buff.append(ip).append("\u0001");
		
		lIdx = line.indexOf("[");
		int rIdx = line.indexOf("]");
		String t = line.substring(lIdx+1, rIdx);
		
		//请求时间
		String reqTime = DateUtil.format("yyyyMMdd HHmmss", DateUtil.parse("dd/MMM/yyyy:HH:mm:ss",Locale.ENGLISH, t));
		buff.append(reqTime).append("\u0001");
		
		String url = line.substring(line.indexOf("\"")+1);
		rIdx = url.indexOf("\"");
		
		url = url.substring(0, rIdx);
		
		lIdx = url.indexOf("/");		
		rIdx = url.indexOf("HTTP");
		String httpVersion = url.substring(rIdx);
		
		//http method,GET or POST
		buff.append(url.substring(0,lIdx).trim()).append("\u0001");
		
		url = url.substring(lIdx).trim();
		rIdx = url.indexOf("HTTP");
		
		//add host
		buff.append("domain").append("\u0001");
	
		//url&param
		lIdx = url.indexOf("?");
		if (lIdx == -1) { //无参数值
			buff.append(url.substring(0,rIdx)).append("\u0001").append("").append("\u0001");
		} else { // url & param
			buff.append(url.substring(0,lIdx)).append("\u0001").append(url.substring(lIdx+1,rIdx).trim()).append("\u0001");
		}
		
		//left line
		rIdx = line.indexOf("HTTP");
		url = line.substring(rIdx+9).trim();
		
		//响应码
		lIdx = url.indexOf(" ");
		String resCode = url.substring(0,lIdx);
		
		//响应长度
		url = url.substring(lIdx+1);
		lIdx = url.indexOf(" ");
		String resLength = url.substring(0,lIdx);
		
		url = url.substring(lIdx+1);
		lIdx = url.indexOf(" ");
		String referer = url.substring(0,lIdx).replace("\"", "");
		String ua = url.substring(lIdx+1).replace("\"", "");
		
		//add referer
		buff.append(referer).append("\u0001");
		
		//add http version
		buff.append(httpVersion).append("\u0001");
		
		//add response_code
		buff.append(resCode).append("\u0001");
	
		//add response length
		buff.append(resLength).append("\u0001");
		
		//add cookie
		buff.append("").append("\u0001");
		
		//add ua
		buff.append(ua);
		
		return buff.toString();
	}
	
	public static void main(String[] args) {
		String s = "222.175.197.45 - - [21/Mar/2016:10:51:37 +0800] \"GET /crc?t=hw&m=0&a=0&v=2.0&s=AzM9smJzgzMwczMykTN1cTNzkjN4ITN4UDNxEjYj1jYjZCM9QmJwETPoRnJ1EDNzkjN4ITN4UDNx0DdmUzN0EjM4UjNyETPohmJt92YuoHe4pHZuc3d31Da1FC HTTP/1.1\" 404 282 \"http://www.dzxxz.com/\" \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET4.0C; .NET4.0E; 360SE)\"";
		
		System.out.println(FileUtil.parse(s));
	}
}
