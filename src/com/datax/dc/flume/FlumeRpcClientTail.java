package com.datax.dc.flume;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.apache.log4j.Logger;

import com.datax.dc.tail.LogFileTailer;
import com.datax.dc.tail.LogFileTailerListener;
import com.datax.util.ConfigUtil;
import com.datax.util.DateUtil;

/**
 * Flume RcpClent By Tail
 * @author jiaqiang
 * 2016.03.12
 */
public class FlumeRpcClientTail implements LogFileTailerListener {

	private Logger log = Logger.getLogger(FlumeRpcClientTail.class);
	
	private LogFileTailer tailer;
	
	private String logFile;
	
	private RpcClient client;
	
	private Properties clientProp = new Properties();
	
	private Integer stopMonitorPort = 5100;
	
	private Boolean startAtFilePointer;
	
	private String filePointerFile;
	
	// 网站主域
	private String domain;
	
	private Boolean sendBatch = false;
	private Integer sendBatchSize = 100;
	private List<String> dataBatchList = new ArrayList<String>(100);
	
	//发送间隔时间(s)
	private Integer sendInterval = 0;
	private boolean sendFlag = true;
	private long ts;
	
	public FlumeRpcClientTail() {
		if (!init()) {
			throw new RuntimeException("FlumeRpcClientTail init failed.");
		}
		
		try {	    	
	    	this.client = RpcClientFactory.getInstance(clientProp);
	    } catch (Exception ex) {
	    	ex.printStackTrace();
	    	log.error(ex);
	    }
	}
	
	public void start() {
		tailer = new LogFileTailer(new File(this.logFile), 1000, this.startAtFilePointer, new File(this.filePointerFile));
	    tailer.addLogFileTailerListener(this);
	    tailer.start();
	    
	    try {
			new StopTailMonitorThread().start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	
	private boolean init() {
		//任务名称
		String name = System.getProperty("name");
		if (name == null) {
			log.error("add -Dname=<your task name> option.");
			return false;
		}
		name = name.trim();
		
		//配置文件路径
		String config = System.getProperty("config");
		if (config == null) {
			log.error("add -Dconfig=<your config file path> option.");
			return false;
		} else { //指定的配置文件路径
			//加载配置文件
			if (!ConfigUtil.loadConfigFile(config)) {
				log.error("加载配置文件[" + config + "]失败");
				return false;
			}
		}
		
		// tail的日志文件
		this.logFile = ConfigUtil.getString(name + "_log_file");
		if (this.logFile == null) {
			log.error("lose config property[" + name + "_log_file" + "]");
			return false;
		}
		
		this.stopMonitorPort = ConfigUtil.getInt(name + "_stop_tail_monitor_port");
		if (this.stopMonitorPort == null) {
			log.error("lose config property[" + name + "_stop_tail_monitor_port" + "]");
			return false;
		}
		
		this.startAtFilePointer = ConfigUtil.getBoolean(name + "_start_at_file_pointer");
		if (this.startAtFilePointer == null) {
			log.error("lose config property[" + name + "_start_at_file_pointer" + "]");
			return false;
		}
	
		this.filePointerFile = ConfigUtil.getString(name + "_file_pointer_file");
		if (this.filePointerFile == null) {
			log.error("lose config property[" + name + "_file_pointer_file" + "]");
			return false;
		}
		
		String cpf = ConfigUtil.getString(name + "_rpc_client_prop_file");
		if (cpf == null) {
			log.error("lose config property[" + name + "_rpc_client_prop_file" + "]");
			return false;
		}
		if (!ConfigUtil.loadConfigFile(cpf, this.clientProp)) {
			log.error("load config property file [" + cpf + "] failed");
			return false;
		}
		
		this.domain = ConfigUtil.getString(name + "_domain");
		if (this.domain == null) {
			log.error("lose config property[" + name + "_domain" + "]");
			return false;
		}
		
		this.sendBatch = ConfigUtil.getBoolean(name + "_send_batch");
		if (this.sendBatch == null) {
			log.error("lose config property[" + name + "_send_batch" + "]");
			return false;
		}
		if (this.sendBatch) {
			this.sendBatchSize = ConfigUtil.getInt(name + "_send_batch_size");
			if (this.sendBatchSize == null) {
				log.error("lose config property[" + name + "_send_batch_size" + "]");
				return false;
			}
			if (this.sendBatchSize <= 0) { //有效验证
				this.sendBatchSize = 10;
			}
			
			//发送的时间间隔
			this.sendInterval = ConfigUtil.getInt(name + "_send_interval_second");
			if (this.sendInterval == null) { //default 1 minutes
				this.sendInterval = 60;
			}
		}
		
		return true;
	}
	
	@Override
	public void newLogFileLine(String line) throws Exception {
		//System.out.println("line:[" + line + "]");
		//LogFormat "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"" combined
		//parse line and send to flume
		if (line.trim().length() > 0) {
			String parseLine = parse(line);
			if (parseLine != null) {
				if (this.sendBatch) {
					dataBatchList.add(parseLine);
					
					if (sendFlag) { //first add
						this.ts = System.currentTimeMillis();
						sendFlag = false;
					}
					
					//时长
					long t = (System.currentTimeMillis()-this.ts)/1000 - sendInterval;
					
					if (dataBatchList.size() >= this.sendBatchSize || t >= 0) { //达到批量值或超过发送时间阀值
						if (sendDataToFlume(dataBatchList)) { //发送成功
							dataBatchList.clear();
							this.sendFlag = true;
						} else {
							throw new Exception("send data to flume agent failed.");
						}
					}
				} else {
					sendDataToFlume(parseLine);	
				}
			}
		}	
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
	private String parse(final String line) {
		StringBuilder buff = new StringBuilder(200);
		
		try {
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
			buff.append(this.domain).append("\u0001");
		
			//url&param
			lIdx = url.indexOf("?");
			if (lIdx == -1) { //无参数值
				buff.append(url.substring(0,rIdx).trim()).append("\u0001").append("").append("\u0001");
			} else { // url & param
				buff.append(url.substring(0,lIdx).trim()).append("\u0001").append(url.substring(lIdx+1,rIdx).trim()).append("\u0001");
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
		} catch (Exception ex) {
			log.error(ex);
			log.error("parse line error[" + line + "]");
			return null;
		}
		
		return buff.toString();
	}
	
	public void sendDataToFlume(String data) throws Exception {
	    // Create a Flume Event object that encapsulates the sample data
	    Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));
	    
	    if (client == null) { //maybe server stopped
	    	this.client = RpcClientFactory.getInstance(clientProp);
	    }
	    
	    // Send the event
	    try {
	    	client.append(event);
	    } catch (EventDeliveryException ex) {
	    	log.error(ex);
	    	if (client != null) {
	    		client.close();
	    	}
	    	client = null;
	    	
	    	throw new Exception(ex);
	    }
	}

	public boolean sendDataToFlume(List<String> dataList) {
		boolean succ = true;
		
		try {
			if (client == null) { //maybe server stopped
				this.client = RpcClientFactory.getInstance(clientProp);
			}
			
			List<Event> eventList = new ArrayList<Event>(200);
			for (String data : dataList) {
				// Create a Flume Event object that encapsulates the sample data
				Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));
				eventList.add(event);
			}
		
		    // Send the events
		    try {
		    	client.appendBatch(eventList);
		    } catch (EventDeliveryException ex) {
		    	log.error(ex.getMessage(),ex);
		    	if (client != null) {
		    		client.close();
		    	}
		    	client = null;
		    	eventList.clear();
		    	eventList = null;
		    	succ = false;
		    }
		} catch (Exception ex) {
			log.error(ex);
			succ = false;
		}
	    
	    return succ;
	}
	
	public void cleanUp() {
		// Close the RPC connection
	    if (client != null) {
	    	client.close();
	    }
	}

	/*
	 * 停止tail监控线程
	 */
	class StopTailMonitorThread extends Thread {
		ServerSocket ss;
		
		public StopTailMonitorThread() throws IOException {
			ss = new ServerSocket(FlumeRpcClientTail.this.stopMonitorPort);
		}
		
		public void run() {
			try {
				// wait for connect
				Socket s = ss.accept();
				InputStream inStream = s.getInputStream();
				byte[] cmdArr = new byte[1024];
				int len = inStream.read(cmdArr);
				String cmd = new String(cmdArr,0,len);
				if (cmd.equals("exit")) {
					FlumeRpcClientTail.this.tailer.stopTailing();
					FlumeRpcClientTail.this.cleanUp();
					
					log.debug("flume is stopping....");
				} else { // close
					try {
						s.close();
					} catch (IOException e) {
						log.error(e);
					}
				}
			} catch (IOException e) {
				log.error(e);
			} finally {
				if (ss != null && !ss.isClosed()) {
					try {
						ss.close();
					} catch (IOException e) {
						log.error(e);
					}
				}
			}
		}
	}

	public static void main(String[] args) {		
		FlumeRpcClientTail tail = new FlumeRpcClientTail();
		tail.start();
	}
}
