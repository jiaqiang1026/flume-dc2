package com.datax.dc.tail;


/**
 * 提供监听者方法(当tailed日志发生变更时)
 * @author jiaqiang
 * 2016.03.12
 */
public interface LogFileTailerListener {
	 
	/**
	 * A new line has been added to the tailed log file
	 * @param line   The new line that has been added to the tailed log file
	*/
	public void newLogFileLine(String line) throws Exception;
}
