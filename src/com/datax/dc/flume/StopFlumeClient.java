package com.datax.dc.flume;

import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Logger;

import com.datax.util.ConfigUtil;

/**
 * 向flume采集client发送退出指令
 * @author jiaqiang
 * 2016.05.17
 */
public class StopFlumeClient {
	
	private Logger log = Logger.getLogger(StopFlumeClient.class);
	
	//stop监控服务端口号
	private Integer stopPort;
	
	public StopFlumeClient() throws Exception {
		if (!init()) {
			log.error("StopFlumeClient init failed.");
			throw new Exception("StopFlumeClient init failed.");
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
		} 
		
		//加载配置文件
		if (!ConfigUtil.loadConfigFile(config)) {
			log.error("加载配置文件[" + config + "]失败");
			return false;
		}				
		
		this.stopPort = ConfigUtil.getInt(name + "_stop_tail_monitor_port");
		if (this.stopPort == null) {
			log.error("lose config property[" + name + "_stop_tail_monitor_port" + "]");
			return false;
		}
		
		return true;
	}
	
	private void stop() throws Exception {
		Socket s = null;
		
		try {
			s = new Socket("127.0.0.1", this.stopPort);
			if (s.isConnected()) {
				OutputStream outStream = s.getOutputStream();
				outStream.write("exit".getBytes());
				outStream.flush();
			}
		} finally {
			if (s != null) {
				s.close();
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		StopFlumeClient stopClient = new StopFlumeClient();
		stopClient.stop();
	}

}
