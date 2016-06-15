package com.datax.dc.tail;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;

import com.datax.util.FileUtil;

/**
 * 监控日志文件(被观察者)，并在有新行产生时通知监听者
 * @author jiaqiang 
 * 2016.03.12
 */
public class LogFileTailer extends Thread {
	
	private Logger log = Logger.getLogger(LogFileTailer.class);

	// 检查间隔时长(单位毫秒),默认5s
	private long checkInterval = 5000;
	
	// 读取的日志
	private File logFile;
	
	// file_pointer(last read) position
	private Long filePointer = 0L;
	
	// 保存file_pointer文件
	private File filePointerFile;

	// 是否从file_pointer(last read)位置开始读
	private boolean startAtFilePointer = false;

	private volatile boolean tailing = false;
	
	private Set listeners = new HashSet();

	public LogFileTailer(File logFile, long checkInterval, boolean startAtFilePointer, File filePointerFile) {
		this.logFile = logFile;
		this.checkInterval = checkInterval;
		this.startAtFilePointer = startAtFilePointer;
		this.filePointerFile = filePointerFile;
	}

	public void addLogFileTailerListener(LogFileTailerListener l) {
		this.listeners.add(l);
	}

	public void removeLogFileTailerListener(LogFileTailerListener l) {
	    this.listeners.remove(l);
	}

	protected void fireNewLogFileLine(String line) throws Exception {
		for (Iterator i=this.listeners.iterator(); i.hasNext();) {
			LogFileTailerListener l = (LogFileTailerListener) i.next();
			l.newLogFileLine(line);
	    }
	}
	
	/*
	 * 停止 tail 文件
	 */
	public void stopTailing() {
	    this.tailing = false;
	    
	    // 保存最后读取的file_pointer
	    if (!FileUtil.writeLong(this.filePointer, this.filePointerFile)) {
	    	log.debug("save file_pointer[" + this.filePointer + "] to file[" + this.filePointerFile.getAbsolutePath() + "] failed.");
	    }
	}

	@Override
	public void run() {
	    RandomAccessFile file = null;
	    
	    // Determine start point
	    if (this.startAtFilePointer) { // 从上次断点开始
	    	this.filePointer = FileUtil.readLong(this.filePointerFile);
	    	if (this.filePointer == null) {
	    		 filePointer = this.logFile.length();
	    	}
	    } else { // 从文件尾开始
	    	 filePointer = this.logFile.length();
	    }

	    try {
	    	// Start tailing
	    	this.tailing = true;
	    	file = new RandomAccessFile(logFile, "r");
	    	
	    	while (this.tailing) {
		        try {
		        	// Compare the length of the file to the file pointer
		        	long fileLength = this.logFile.length();
		        	if (fileLength < filePointer) {
		        		// Log file must have been rotated or deleted; 
		        		// reopen the file and reset the file pointer
		        		file = new RandomAccessFile(logFile, "r");
		        		filePointer = 0L;
		        	}
	
		        	if (fileLength > filePointer) {
		        		// There is data to read
		        		file.seek(filePointer);
		        		String line = null;
		        		while ((line=file.readLine()) != null) { 
		        			this.fireNewLogFileLine(line);
		        		}
		        		
		        		//update fp
		        		filePointer = file.getFilePointer();
		        	}
	
		        	// Sleep for the specified interval
		        	this.currentThread().sleep(checkInterval);
		        } catch(Exception e) {
		        	e.printStackTrace();
		        	FileUtil.writeLong(this.filePointer, this.filePointerFile);
		        	log.error(e);
		        	
		        	// sleep 10s
		        	this.currentThread().sleep(10000);
		        }
	    	} // end while tailing
	    } catch(Exception e) {
	    	log.error(e);
	    } finally {
	    	if (file != null) {
	    		try {
					file.close();
				} catch (IOException e) {
					log.error(e);
				}
	    	}
	    }
	}
}
