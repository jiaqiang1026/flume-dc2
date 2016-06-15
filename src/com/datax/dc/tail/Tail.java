package com.datax.dc.tail;

import java.io.File;

public class Tail implements LogFileTailerListener {

	private LogFileTailer tailer;
	
	/**
	 * Creates a new Tail instance to follow the specified file
	 */
	public Tail(String filename) {
		tailer = new LogFileTailer(new File(filename), 1000, false, null);
	    tailer.addLogFileTailerListener(this);
	    tailer.start();
	}
	
	@Override
	public void newLogFileLine(String line) {
		System.out.println("line:[" + line + "]");
	}

	public static void main(String[] args) {
		if( args.length < 1 ) {
			System.out.println("Usage: com.datax.dc.tail.Tail <filename>" );
			System.exit( 0 );
	    }
	    
		new Tail(args[0]);
	}
}
