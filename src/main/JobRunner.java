package main;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import task1.Task1;
import task2.Task2;
import task3.Task3;

public class JobRunner {

	public static final long MAX_ID = 15070781;
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path(
				"file:///users/level4/1103834s/Desktop/bd4-hadoop/conf/core-site.xml"));
		conf.set("mapred.jar",
				"file:///users/level4/1103834s/Desktop/task1.jar");

		MyJob task = new Task3();
		
		// job(s) ran successfully
		if (ToolRunner.run(conf, task, args) == 0) {
			// output results
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] status = fs.listStatus(new Path(task.getOuputDir()));
			long totalBytes = 0;
			for (int i = 0; i < status.length; i++) {
				// skip files which are not produced by the reducers
				if(!status[i].getPath().toString().contains("part-r-"))
					continue;
				BufferedReader br = new BufferedReader(
						new InputStreamReader(fs.open(status[i].getPath())));
				String line;
				line = br.readLine();
				while (line != null) {
					System.out.println(line);
					totalBytes += (line  + "\n").getBytes().length;
					line = br.readLine();
				}
			}
			System.out.println("Number of bytes transferred " + totalBytes);
			System.out.println("***");
		}
	}
}
