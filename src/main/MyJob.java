package main;

import org.apache.hadoop.util.Tool;

public interface MyJob extends Tool {

	public String getOuputDir();
	
}
