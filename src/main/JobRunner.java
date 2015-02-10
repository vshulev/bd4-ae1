package main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import task1.Task1;
import task2.Task2;

public class JobRunner {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("file:///users/level4/1103834s/Desktop/bd4-hadoop/conf/core-site.xml"));
		conf.set("mapred.jar", "file:///users/level4/1103834s/Desktop/task1.jar");
		
		System.exit(ToolRunner.run(conf, new Task1(), args));
	}
	
}
