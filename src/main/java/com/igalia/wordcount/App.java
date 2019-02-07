package com.igalia.wordcount;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class App {

	public static void main(String args[]) throws IOException, InterruptedException {
		UserGroupInformation ugi
				= UserGroupInformation.createRemoteUser("ec2-user");
		ugi.doAs(new PrivilegedExceptionAction<Object>() {
			@Override
			public Void run() throws Exception {
				String dirPath = "/user/ec2-user/input/file01";
//		FileSystemOperations client = new FileSystemOperations();
				String hdfsPath = "hdfs://" + "18.215.62.194" + ":" + 9000;
				System.out.println(hdfsPath);
				Configuration conf = new Configuration();
				conf.set("fs.defaultFS", hdfsPath);

//		client.readFile(dirPath, conf);

				FileSystem fs = FileSystem.get(conf);
				FileStatus[] fsStatus = fs.listStatus(new Path("/user/ec2-user"));
				for(int i = 0; i < fsStatus.length; i++){
					System.out.println(fsStatus[i].getPath().toString());
				}
				
				FSDataInputStream inputStream = fs.open(new Path("/user/ec2-user/input/file01"));
				String out = IOUtils.toString(inputStream,"UTF8");
				System.out.println(out);
				inputStream.close();

//				OutputStream outputStream = fs.create(new Path("/user/ec2-user/tuan2"));
//				IOUtils.write("TTTTTT", outputStream, "UTF8");
//				outputStream.flush();
//				outputStream.close();
				return null;
			}
		});


	}
}