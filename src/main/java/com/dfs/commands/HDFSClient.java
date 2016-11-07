package com.dfs.commands;
import java.io.BufferedInputStream;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.mapred.JobConf;
import java.nio.file.Files;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class HDFSClient {
	//HDFSClient client = new HDFSClient();
	static Configuration config = new Configuration();
	public HDFSClient() {
		config.set("fs.default.name", "master:8020");
		 
	}
	
	
		public static void printUsage(){
		System.out.println("Usage: hdfsclient add" + "<local_path> <hdfs_path>");
		System.out.println("Usage: hdfsclient read" + "<hdfs_path>");
		System.out.println("Usage: hdfsclient delete" + "<hdfs_path>");
		System.out.println("Usage: hdfsclient mkdir" + "<hdfs_path>");
		System.out.println("Usage: hdfsclient copyfromlocal" + "<local_path> <hdfs_path>");
		System.out.println("Usage: hdfsclient copytolocal" + " <hdfs_path> <local_path> ");
		System.out.println("Usage: hdfsclient modificationtime" + "<hdfs_path>");
		System.out.println("Usage: hdfsclient getblocklocations" + "<hdfs_path>");
		System.out.println("Usage: hdfsclient gethostnames");
		
		}
		 
		public boolean ifExists (Path source) throws IOException{
		 
				 
		FileSystem hdfs = FileSystem.get(config);
		boolean isExists = hdfs.exists(source);
		return isExists;
		}
		 
		public void getHostnames () throws IOException{
				 
		FileSystem fs = FileSystem.get(config);
		DistributedFileSystem hdfs = (DistributedFileSystem) fs;
		DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
		 
		String[] names = new String[dataNodeStats.length];
		for (int i = 0; i < dataNodeStats.length; i++) {
		names[i] = dataNodeStats[i].getHostName();
		System.out.println((dataNodeStats[i].getHostName()));
		}
		}
		 
		public void getBlockLocations(String source) throws IOException{
		 
		
		 
		FileSystem fileSystem = FileSystem.get(config);
		Path srcPath = new Path(source);
		 
		// Check if the file already exists
		if (!(ifExists(srcPath))) {
		System.out.println("No such destination " + srcPath);
		return;
		}
		// Get the filename out of the file path
		String filename = source.substring(source.lastIndexOf('/') + 1, source.length());
		 
		FileStatus fileStatus = fileSystem.getFileStatus(srcPath);
		 
		BlockLocation[] blkLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
		int blkCount = blkLocations.length;
		 
		System.out.println("File :" + filename + "stored at:");
		for (int i=0; i < blkCount; i++) {
		String[] hosts = blkLocations[i].getHosts();
		System.out.format("Host %d: %s %n", i, hosts);
		}
		 
		}
		 
		public void getModificationTime(String source) throws IOException{
		 		
		FileSystem fileSystem = FileSystem.get(config);
		Path srcPath = new Path(source);
		 
		// Check if the file already exists
		if (!(fileSystem.exists(srcPath))) {
		System.out.println("No such destination " + srcPath);
		return;
		}
		// Get the filename out of the file path
		String filename = source.substring(source.lastIndexOf('/') + 1, source.length());
		 
		FileStatus fileStatus = fileSystem.getFileStatus(srcPath);
		long modificationTime = fileStatus.getModificationTime();
		 
		System.out.format("File %s; Modification time : %0.2f %n",filename,modificationTime);
		 
		}
		 
		public void copyFromLocal (String source, String dest) throws IOException {
		 
		
		/*conf.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"));
		conf.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"));
		conf.addResource(new Path("/home/hadoop/hadoop/conf/mapred-site.xml"));*/
		 
		FileSystem fileSystem = FileSystem.get(config);
		Path srcPath = new Path(source);
		 
		Path dstPath = new Path(dest);
		// Check if the file already exists
		if (!(fileSystem.exists(dstPath))) {
		System.out.println("No such destination " + dstPath);
		return;
		}
		String filename = source.substring(source.lastIndexOf('/') + 1, source.length());
		 
		try{
			if(fileSystem.exists(dstPath))
			{
				System.out.println(dstPath +" : File exists");
			}
			else
			{
				fileSystem.copyFromLocalFile(srcPath, dstPath);
				System.out.println("File " + filename + "copied to " + dest);
			}
		
		}catch(Exception e){
		System.err.println("Exception caught! :" + e);
		System.exit(1);
		}finally{
		fileSystem.close();
		}
		}
		
		public void moveFromLocal (String source, String dest) throws IOException {
			 
			
			/*conf.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"));
			conf.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"));
			conf.addResource(new Path("/home/hadoop/hadoop/conf/mapred-site.xml"));*/
			 
			FileSystem fileSystem = FileSystem.get(config);
			Path srcPath = new Path(source);
			 
			Path dstPath = new Path(dest);
			// Check if the file already exists
			if (!(fileSystem.exists(dstPath))) {
			System.out.println("No such destination " + dstPath);
			return;
			}
		 
		// Get the filename out of the file path
		String filename = source.substring(source.lastIndexOf('/') + 1, source.length());
		 
		try{
		fileSystem.moveFromLocalFile(srcPath, dstPath);
		System.out.println("File " + filename + "moved to " + dest);
		}catch(Exception e){
		System.err.println("Exception caught! :" + e);
		System.exit(1);
		}finally{
		fileSystem.close();
		}
		}
		
		
		public void put (String source, String dest) throws IOException {
			 
			
			/*conf.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"));
			conf.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"));
			conf.addResource(new Path("/home/hadoop/hadoop/conf/mapred-site.xml"));*/
			 
			FileSystem fileSystem = FileSystem.get(config);
			Path srcPath = new Path(source);
			 
			Path dstPath = new Path(dest);
			// Check if the file already exists
			if (!(fileSystem.exists(dstPath))) {
			System.out.println("No such destination " + dstPath);
			return;
			}
			String filename = source.substring(source.lastIndexOf('/') + 1, source.length());
			 
			try{
				if(fileSystem.exists(dstPath))
				{
					System.out.println(dstPath +" : File exists");
				}
				else
				{
					fileSystem.copyFromLocalFile(srcPath, dstPath);
					System.out.println("File " + filename + "copied to " + dest);
				}
			
			}catch(Exception e){
			System.err.println("Exception caught! :" + e);
			System.exit(1);
			}finally{
			fileSystem.close();
			}
			
		}
		public void cat(String loc) throws IOException
		{
			FileSystem fileSystem = FileSystem.get(config);
			Path srcPath = new Path(loc);
			if (!(fileSystem.exists(srcPath))) {
				System.out.println("No such destination " + loc);
				return;
				}
			String filename = loc.substring(loc.lastIndexOf('/') + 1, loc.length());
			try{
				//fileSystem.append(srcPath,dstPath);
				//FSDataInputStream in=fileSystem.open(srcPath);s
				//os.read();
				//String msgIn = os.readUTF();
				//System.out.print(msgIn);
				//IOUtils.copyBytes(in, System.out, 4096, false);
				BufferedReader br=new BufferedReader(new InputStreamReader(fileSystem.open(srcPath)));
                String line;
                line=br.readLine();
                while (line != null){
                        System.out.println(line);
                        line=br.readLine();
                }
			}catch(Exception e){
				System.err.println("Exception caught! :" + e);
				System.exit(1);
			}finally{
				fileSystem.close();
			}
		}
		public void checkSum(String loc) throws IOException
		{
			FileSystem fileSystem = FileSystem.get(config);
			Path srcPath = new Path(loc);
			if (!(fileSystem.exists(srcPath))) {
				System.out.println("No such destination " + loc);
				return;
				}
			String filename = loc.substring(loc.lastIndexOf('/') + 1, loc.length());
			try{
				System.out.print(fileSystem.getFileChecksum(srcPath));
			}catch(Exception e){
				System.err.println("Exception caught! :" + e);
				System.exit(1);
			}finally{
				fileSystem.close();
			}
		}
		public void copyToLocal (String source, String dest) throws IOException { 
		FileSystem fileSystem = FileSystem.get(config);
		Path srcPath = new Path(source);
		 
		Path dstPath = new Path(dest);
		// Check if the file already exists
		if (!(fileSystem.exists(srcPath))) {
		System.out.println("No such destination " + srcPath);
		return;
		}
		 
		// Get the filename out of the file path
		String filename = source.substring(source.lastIndexOf('/') + 1, source.length());
		//String desti=dstPath.toString()+"/"+filename;
		//System.out.println(desti);
		// Path newPath=new Path(filename);
		try{
			if(ifExists(dstPath))
			{
				System.out.println(filename+" File already exists in the path "+dstPath);
			}
			else
			{
		fileSystem.copyToLocalFile(srcPath, dstPath);
		System.out.println("File " + filename + " copied to " + dest);
			}
		}catch(Exception e){
		System.err.println("Exception caught! :" + e);
		System.exit(1);
		}finally{
		fileSystem.close();
		}
		}
		 
		public void get(String source, String dest) throws IOException { 
			FileSystem fileSystem = FileSystem.get(config);
			Path srcPath = new Path(source);
			 
			Path dstPath = new Path(dest);
			// Check if the file already exists
			if (!(fileSystem.exists(srcPath))) {
			System.out.println("No such destination " + srcPath);
			return;
			}
			 
			// Get the filename out of the file path
			String filename = source.substring(source.lastIndexOf('/') + 1, source.length());
			 
			try{
			fileSystem.copyToLocalFile(srcPath, dstPath);
			System.out.println("File " + filename + "copied to " + dest);
			}catch(Exception e){
			System.err.println("Exception caught! :" + e);
			System.exit(1);
			}finally{
			fileSystem.close();
			}
			}
		
		public void moveToLocal(String source, String dest) throws IOException { 
			FileSystem fileSystem = FileSystem.get(config);
			Path srcPath = new Path(source);
			 
			Path dstPath = new Path(dest);
			// Check if the file already exists
			if (!(fileSystem.exists(srcPath))) {
			System.out.println("No such destination " + srcPath);
			return;
			}
			 
			// Get the filename out of the file path
			String filename = source.substring(source.lastIndexOf('/') + 1, source.length());
			 
			try{
			fileSystem.moveToLocalFile(srcPath, dstPath);
			System.out.println("File " + filename + "moved to " + dest);
			}catch(Exception e){
			System.err.println("Exception caught! :" + e);
			System.exit(1);
			}finally{
			fileSystem.close();
			}
			}
		
		public void move(String source, String dest) throws IOException { 
			FileSystem fileSystem = FileSystem.get(config);
			Path srcPath = new Path(source);
			 
			Path dstPath = new Path(dest);
			// Check if the file already exists
			if (!(fileSystem.exists(srcPath))) {
			System.out.println("No such destination " + srcPath);
			return;
			}
			 
			// Get the filename out of the file path
			String filename = source.substring(source.lastIndexOf('/') + 1, source.length());
			 
			try{
			fileSystem.rename(srcPath, dstPath);
			System.out.println("File " + filename + "moved to " + dest);
			}catch(Exception e){
			System.err.println("Exception caught! :" + e);
			System.exit(1);
			}finally{
			fileSystem.close();
			}
			}
		
		public void copy(String source, String dest) throws IOException { 
			FileSystem fs = FileSystem.get(config);
			Path srcPath = new Path(source);
			FileUtil fl=new FileUtil(); 
			Path dstPath = new Path(dest);
			// Check if the file already exists
			if (!(fs.exists(srcPath))) {
			System.out.println("No such destination " + srcPath);
			return;
			}
			 
			// Get the filename out of the file path
			String filename = source.substring(source.lastIndexOf('/') + 1, source.length());
			 
			try{
			//fileSystem.rename(srcPath, dstPath);
				//fl.copy(srcPath,fileSystem , dstPath, false, fileSystem);
			fl.copy(fs, srcPath, fs, dstPath, false, config);
			System.out.println("File " + filename + " copied to " + dest);
			}catch(Exception e){
			System.err.println("Exception caught! :" + e);
			System.exit(1);
			}finally{
			fs.close();
			}
			}
		
		public void renameFile (String fromthis, String tothis) throws IOException{
		
		FileSystem fileSystem = FileSystem.get(config);
		Path fromPath = new Path(fromthis);
		Path toPath = new Path(tothis);
		 
		if (!(fileSystem.exists(fromPath))) {
		System.out.println("No such destination " + fromPath);
		return;
		}
		 
		if (fileSystem.exists(toPath)) {
		System.out.println("Already exists! " + toPath);
		return;
		}
		 
		try{
		boolean isRenamed = fileSystem.rename(fromPath, toPath);
		if(isRenamed){
		System.out.println("Renamed from " + fromthis + "to " + tothis);
		}
		}catch(Exception e){
		System.out.println("Exception :" + e);
		System.exit(1);
		}finally{
		fileSystem.close();
		}
		 
		}
		 
		public void addFile(String source, String dest) throws IOException {
		 
		// Conf object will read the HDFS configuration parameters
				 
		FileSystem fileSystem = FileSystem.get(config);
		 
		// Get the filename out of the file path
		String filename = source.substring(source.lastIndexOf('/') + 1, source.length());
		 
		// Create the destination path including the filename.
		if (dest.charAt(dest.length() - 1) != '/') {
		dest = dest + "/" + filename;
		} else {
		dest = dest + filename;
		}
		 
		// Check if the file already exists
		Path path = new Path(dest);
		if (fileSystem.exists(path)) {
		System.out.println("File " + dest + " already exists");
		return;
		}
		 
		// Create a new file and write data to it.
		FSDataOutputStream out = fileSystem.create(path);
		InputStream in = new BufferedInputStream(new FileInputStream(
		new File(source)));
		 
		byte[] b = new byte[1024];
		int numBytes = 0;
		while ((numBytes = in.read(b)) > 0) {
		out.write(b, 0, numBytes);
		}
		 
		// Close all the file descripters
		in.close();
		out.close();
		fileSystem.close();
		}
		 
		public void readFile(String file) throws IOException {
				 
		FileSystem fileSystem = FileSystem.get(config);
		 
		Path path = new Path(file);
		if (!fileSystem.exists(path)) {
		System.out.println("File " + file + " does not exists");
		return;
		}
		 
		FSDataInputStream in = fileSystem.open(path);
		 
		String filename = file.substring(file.lastIndexOf('/') + 1,
		file.length());
		 
		OutputStream out = new BufferedOutputStream(new FileOutputStream(
		new File(filename)));
		 
		byte[] b = new byte[1024];
		int numBytes = 0;
		while ((numBytes = in.read(b)) > 0) {
		out.write(b, 0, numBytes);
		}
		 
		in.close();
		out.close();
		fileSystem.close();
		}
		 
		public void deleteFile(String file) throws IOException {
		
		FileSystem fileSystem = FileSystem.get(config);
		 
		Path path = new Path(file);
		if (!fileSystem.exists(path)) {
		System.out.println("File " + file + " does not exists");
		return;
		}
		 
		fileSystem.delete(new Path(file), true);
		 
		fileSystem.close();
		}
		 
		public void mkdir(String dir) throws IOException {
		
		FileSystem fileSystem = FileSystem.get(config);
		 
		Path path = new Path(dir);
		if (fileSystem.exists(path)) {
		System.out.println("Dir " + dir + " already exists!");
		return;
		}
		 
		fileSystem.mkdirs(path);
		System.out.println("The dirctory is created...!"); 
		fileSystem.close();
		}
		public void rmkdir(String dir) throws IOException {
			
			FileSystem fileSystem = FileSystem.get(config);
			 
			Path path = new Path(dir);
			if (!(fileSystem.exists(path))) {
			System.out.println("Dir " + dir + " does not exist...!");
			return;
			}
			else
			{
			fileSystem.delete(path);
			System.out.println("The directory "+path+" is deleted..!");
			fileSystem.close();
			}
			}
		public void testDir(String dir) throws IOException {
			
			FileSystem fileSystem = FileSystem.get(config);
			 
			Path path = new Path(dir);
			if (fileSystem.isDirectory(path)) {
			System.out.println("Dir " + dir + " is exist and it is a dirctory!");
			
			//System.out.println(fileSystem.getStatus());
			
			//fileSystem.getContentSummary(path)
			//return;
			}
			else
			{
				System.out.println("The URI "+path+" is not exist..!" );
			}
			
			if(fileSystem.getContentSummary(path).getLength()>0)
			{
				System.out.println("The the "+dir+" directory is not empy..!");
			}
			else
			{
				System.out.println("The the "+dir+" directory is empy..!");
			}
			 
				 fileSystem.close();
			}
			
		
		public void changeUserGroup(String user,String group,String source) throws IOException {
			  FileSystem fs = FileSystem.get(config);
			  Path srcPath=new Path(source);
			  
			  FsPermission changedPermission=new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL);
			    try
			    {
				if ((fs.isFile(srcPath))) {
			    fs.setOwner(srcPath,user,group);
			    fs.setPermission(srcPath,changedPermission);
			    System.out.print("Change group Done..");
			    }
				else
				{
					System.out.println("No such destination " + srcPath);
				}
			    }catch(Exception e){
					System.out.println("Exception :" + e);
					System.exit(1);
					}finally{
					fs.close();
					}
			}		
		public void changeMode(String source) throws IOException {
			  FileSystem fs = FileSystem.get(config);
			  Path srcPath=new Path(source);
			  
			  FsPermission changedPermission=new FsPermission(FsAction.READ,FsAction.WRITE,FsAction.EXECUTE);
			    try
			    {
				if ((fs.isFile(srcPath))) {
			    fs.setPermission(srcPath, changedPermission);
			    System.out.print("Change permission Done..");
			    }
				else
				{
					System.out.println("No such destination " + srcPath);
				}
			    }catch(Exception e){
					System.out.println("Exception :" + e);
					System.exit(1);
					}finally{
					fs.close();
					}
			}	
		public void createSnapshot (String source, String snsName) throws IOException {
			  FileSystem fs = FileSystem.get(config);
			  Path srcPath=new Path(source);
	
			  //FsPermission changedPermission=new FsPermission(FsAction.READ,FsAction.WRITE,FsAction.EXECUTE);
			    try
			    {
				if ((fs.isDirectory(srcPath))) {
				
			    fs.createSnapshot(srcPath,snsName);
			    System.out.print("Snapshot Created and Named as "+snsName+"...!!!");
			    }
				else
				{
					System.out.println("No such destination " + srcPath);
				}
			    }catch(Exception e){
					System.out.println("Exception :" + e);
					System.exit(1);
					}finally{
					fs.close();
					}
			}
		public void renameSnapshot (String source,String snsOldName,String snsNewName) throws IOException {
			  FileSystem fs = FileSystem.get(config);
			  Path srcPath=new Path(source);
	
			  //FsPermission changedPermission=new FsPermission(FsAction.READ,FsAction.WRITE,FsAction.EXECUTE);
			    try
			    {
				if ((fs.isDirectory(srcPath))) {
			    fs.renameSnapshot(srcPath,snsOldName,snsNewName);
			    System.out.print("Snapshot "+snsOldName+" is"+ " renamed to "+snsNewName+" ..!!!!");
			    }
				else
				{
					System.out.println("No such destination " + srcPath);
				}
			    }catch(Exception e){
					System.out.println("Exception :" + e);
					System.exit(1);
					}finally{
					fs.close();
					}
			}
		
		public void deleteSnapshot (String source) throws IOException {
			  FileSystem fs = FileSystem.get(config);
			  Path srcPath=new Path(source);
	
			  //FsPermission changedPermission=new FsPermission(FsAction.READ,FsAction.WRITE,FsAction.EXECUTE);
			    try
			    {
				if ((fs.isDirectory(srcPath))) {
				
			    fs.deleteSnapshot(srcPath,"S0");
			    System.out.print("Snapshot Deleted..");
			    }
				else
				{
					System.out.println("No such destination " + srcPath);
				}
			    }catch(Exception e){
					System.out.println("Exception :" + e);
					System.exit(1);
					}finally{
					fs.close();
					}
			}
		public void tailofFile (String source) throws IOException {
			  FileSystem fs = FileSystem.get(config);
			  Path srcPath=new Path(source);
	
			  //FsPermission changedPermission=new FsPermission(FsAction.READ,FsAction.WRITE,FsAction.EXECUTE);
			    try
			    {
				if (fs.isFile(srcPath)) {
				
			    }
				else
				{
					System.out.println("No such destination " + srcPath);
				}
			    }catch(Exception e){
					System.out.println("Exception :" + e);
					System.exit(1);
					}finally{
					fs.close();
					}
			}
		
		public void text(String source) throws IOException {
			  FileSystem fs = FileSystem.get(config);
			  Path srcPath=new Path(source);
	
			  //FsPermission changedPermission=new FsPermission(FsAction.READ,FsAction.WRITE,FsAction.EXECUTE);
			 
			    try
			    {
				if (fs.isFile(srcPath)) {
					 BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(srcPath)));
					 String line;
					  line=br.readLine();
					  while (line != null){
					    System.out.println(line);

					    // be sure to read the next line otherwise you'll get an infinite loop
					    line = br.readLine();
					  }
					  
				
			    }
					  else
						{
							System.out.println("No such destination " + srcPath);
						}
			    }catch(Exception e){
					System.out.println("Exception :" + e);
					System.exit(1);
					}finally{
					fs.close();
					}
			}
		public void du (String source) throws IOException {
			  FileSystem fs = FileSystem.get(config);
			  Path srcPath=new Path(source);
	
			  //FsPermission changedPermission=new FsPermission(FsAction.READ,FsAction.WRITE,FsAction.EXECUTE);
			    try
			    {
				if ((fs.isDirectory(srcPath))) {
				
					System.out.println("SIZE OF THE HDFS DIRECTORY : " + fs.getContentSummary(srcPath).getSpaceConsumed());
					System.out.println(fs.getContentSummary(srcPath));
			    }
				else
				{
					System.out.println("No such destination " + srcPath);
				}
			    }catch(Exception e){
					System.out.println("Exception :" + e);
					System.exit(1);
					}finally{
					fs.close();
					}
			}
		public void setRep(String source,int repfactor) throws IOException {
			  FileSystem fs = FileSystem.get(config);
			  Path srcPath=new Path(source);
	
			  //FsPermission changedPermission=new FsPermission(FsAction.READ,FsAction.WRITE,FsAction.EXECUTE);
			    try
			    {
				if (fs.exists(srcPath)) {
				fs.setReplication(srcPath, (short)repfactor);
				System.out.println("Replication factor is: "+fs.getReplication(srcPath));
				//System.out.println("Replication factor is set to "+repfactor);
					
			    }
				else
				{
					System.out.println("No such file exists " + srcPath);
				}
			    }catch(Exception e){
					System.out.println("Exception :" + e);
					System.exit(1);
					}finally{
					fs.close();
					}
			}
		public void status(String source) throws IOException {
			  FileSystem fs = FileSystem.get(config);
			  Path srcPath=new Path(source);
	
			  //FsPermission changedPermission=new FsPermission(FsAction.READ,FsAction.WRITE,FsAction.EXECUTE);
			    try
			    {
				if (fs.exists(srcPath)) {
					System.out.println("****Status of a path****");
					System.out.println(fs.getStatus(srcPath));
			    }
				else
				{
					System.out.println("No such path exists " + srcPath);
				}
			    }catch(Exception e){
					System.out.println("Exception :" + e);
					System.exit(1);
					}finally{
					fs.close();
					}
			}
		public void setfACL(String source) throws IOException {
			  FileSystem fs = FileSystem.get(config);
			  Path srcPath=new Path(source);
			  
			 //List<FsPermission> aclspec=ArrayList<FsPermission>;
			  //List<FsPermission> aclspec=new ArrayList<FsPermission>();
			  //aclspec.add(new FsPermission(FsAction.READ,FsAction.WRITE,FsAction.EXECUTE));
			  //FsPermission changedPermission=new FsPermission(FsAction.READ,FsAction.WRITE,FsAction.EXECUTE);
			  //user::rwx,user:foo:rw-,group::r--,other::---
			  List<AclEntry> parsedList = AclEntry.parseAclSpec(
				      "group::rwx,user:user1:rwx,user:user2:rw-,"
				          + "group:group1:rw-,default:group:group1:rw-", true);

				  AclEntry basicAcl = new AclEntry.Builder().setType(AclEntryType.GROUP)
				      .setPermission(FsAction.ALL).build();
				  AclEntry user1Acl = new AclEntry.Builder().setType(AclEntryType.USER)
				      .setPermission(FsAction.ALL).setName("user1").build();
				  AclEntry user2Acl = new AclEntry.Builder().setType(AclEntryType.USER)
				      .setPermission(FsAction.READ_WRITE).setName("user2").build();
				  AclEntry group1Acl = new AclEntry.Builder().setType(AclEntryType.GROUP)
				      .setPermission(FsAction.READ_WRITE).setName("group1").build();
				  AclEntry defaultAcl = new AclEntry.Builder().setType(AclEntryType.GROUP)
				      .setPermission(FsAction.READ_WRITE).setName("group1")
				      .setScope(AclEntryScope.DEFAULT).build();
				  List<AclEntry> expectedList = new ArrayList<AclEntry>();
				  expectedList.add(basicAcl);
				  expectedList.add(user1Acl);
				  expectedList.add(user2Acl);
				  expectedList.add(group1Acl);
				  expectedList.add(defaultAcl);
			    try
			    {
				if ((fs.isDirectory(srcPath)) || (fs.isFile(srcPath))) {
	            fs.setAcl(srcPath,expectedList);
	            
	           // System.out.println(fs.getXAttrs(srcPath));
	            
			    System.out.print("ACL is modified..!");
			    }
				else
				{
					System.out.println("No such destination " + srcPath);
				}
			    }catch(Exception e){
					System.out.println("Exception :" + e);
					System.exit(1);
					}finally{
					fs.close();
					}
			}
		
		public void getfACL(String source) throws IOException {
			  FileSystem fs = FileSystem.get(config);
			  Path srcPath=new Path(source);
	
			  //FsPermission changedPermission=new FsPermission(FsAction.READ,FsAction.WRITE,FsAction.EXECUTE);
			    try
			    {
				if ((fs.isDirectory(srcPath)) || (fs.isFile(srcPath))) {
	            fs.getAclStatus(srcPath);
	            
	           // System.out.println(fs.getXAttrs(srcPath));
	            
			    System.out.print("Got the ACL Details..");
			    }
				else
				{
					System.out.println("No such destination " + srcPath);
				}
			    }catch(Exception e){
					System.out.println("Exception :" + e);
					System.exit(1);
					}finally{
					fs.close();
					}
			}
		public void setfAttri(String source) throws IOException {
			  FileSystem fs = FileSystem.get(config);
			  Path srcPath=new Path(source);
	
			  //FsPermission changedPermission=new FsPermission(FsAction.READ,FsAction.WRITE,FsAction.EXECUTE);
			  byte arr[] = new byte[] {1, 6, 3};
			    try
			    {
				if ((fs.isDirectory(srcPath)) || (fs.isFile(srcPath))) {
					
	           //System.out.println(fs.getXAttrs(srcPath));
					//fs.setXAttr(srcPath, "user.a1", arr);
					fs.setXAttr(srcPath, "user.attr1", "value1".getBytes());
					 //fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
					 // fs.setXAttr(path, name2, value2, EnumSet.of(XAttrSetFlag.CREATE));
					  //fs.setXAttr(path, name3, null, EnumSet.of(XAttrSetFlag.CREATE));
					 //fs.setXAttr(path, name1, newValue1, EnumSet.of(XAttrSetFlag.REPLACE))
			    System.out.print("File Attribute is set for the file/path "+srcPath);
			    }
				else
				{
					System.out.println("No such destination " + srcPath);
				}
			    }catch(Exception e){
					System.out.println("Exception :" + e);
					System.exit(1);
					}finally{
					fs.close();
					}
			}
		public void touchz(String path) throws IOException {
			  FileSystem fs = FileSystem.get(config);
			  Path srcPath=new Path(path);
			  
			    try
			    {
				if (fs.exists(srcPath)) {
					
					System.out.println("The file name already exists");
				}
					
					else
					{
						fs.create(srcPath);
						System.out.println("The file created in the path "+srcPath);
					}
					
	        
			    }catch(Exception e){
					System.out.println("Exception :" + e);
					System.exit(1);
					}finally{
					fs.close();
					}
			}
		public void getfAttri(String source) throws IOException {
			  FileSystem fs = FileSystem.get(config);
			  Path srcPath=new Path(source);
			  //BasicFileAttributes attr=Files.readAttributes(srcPath, BasicFileAttributes.class);
			  //BasicFileAttributes basicAttr = Files.readAttributes(srcPath, BasicFileAttributes.class);
	
			  //FsPermission changedPermission=new FsPermission(FsAction.READ,FsAction.WRITE,FsAction.EXECUTE);
			    try
			    {
				if ((fs.isDirectory(srcPath)) || (fs.isFile(srcPath))) {
	           // fs.getAclStatus(srcPath);
					System.out.println(fs.listXAttrs(srcPath));
					System.out.println(fs.getXAttrs(srcPath));
	           // System.out.println(fs.getXAttrs(srcPath));
			    System.out.print("Got the File Attribute Details..");
			    }
				else
				{
					System.out.println("No such destination " + srcPath);
				}
			    }catch(Exception e){
					System.out.println("Exception :" + e);
					System.exit(1);
					}finally{
					fs.close();
					}
			}
		
		public Path mergeTextFiles(String inputFiles,String outputFile,boolean deleteSource,boolean deleteDestinationFileIfExist) throws IOException {
			  //JobConf conf=new JobConf(FileMerger.class);
			  FileSystem fs=FileSystem.get(config);
			  Path inputPath=new Path(inputFiles);
			  Path outputPath=new Path(outputFile);
			  try
			  {
			  if (deleteDestinationFileIfExist) {
			    if (fs.exists(outputPath)) {
			      fs.delete(outputPath,false);
			      System.out.println("Warning: remove destination file since it already exists...");
			    }
			  }
			 else {
			    Preconditions.checkArgument(!fs.exists(outputPath),new IOException("Destination file already exists..."));
			  }
			  FileUtil.copyMerge(fs,inputPath,fs,outputPath,deleteSource,config,"/");
			  System.out.println("Successfully merge " + inputPath.toString() + " to "+ outputFile);
			  //return outputPath;
			  }catch(Exception e){
					System.out.println("Exception :" + e);
					System.exit(1);
					}finally{
					fs.close();
					}
			return outputPath;
			  
			}
			  
		public void mergeFiles(String inputFiles,String outputFile) throws IOException { //make this clear
			  //JobConf conf=new JobConf(FileMerger.class);
			  FileSystem fs=FileSystem.get(config);
			  Path inputPath=new Path(inputFiles);
			  Path outputPath=new Path(outputFile);
			  if(fs.exists(outputPath))
			  {
				  fs.delete(outputPath, false);
				  
			  }
			  try
			  {
				  FileUtil.copyMerge(fs, inputPath, fs, outputPath, false, config, null);
				  System.out.println("Successfully Merged...!!!");
			  }catch(Exception e){
					System.out.println("Exception :" + e);
					System.exit(1);
					}finally{
					fs.close();
					}
		}
		public void listDirectoryContents(String dir) throws IOException
		{
			FileSystem fs=FileSystem.get(config);
			  Path srcPath=new Path(dir);
			  try
			  {
				  if(fs.exists(srcPath))
				  {
				  if(fs.isDirectory(srcPath))
				  {
					  FileStatus[] filestatus=fs.listStatus(srcPath);
					  Path[] paths = FileUtil.stat2Paths(filestatus);
					  System.out.println("***** Contents of the Directory *****");
					  for(Path path :paths)
					  {
						  System.out.println(path);
					  }
				  }
				  }
				  else
				  {
					  System.out.print(srcPath+" Does not exists...!!");
				  }
			  }catch(Exception e){
					System.out.println("Exception :" + e);
					System.exit(1);
					}finally{
					fs.close();
					}
		}
		public void find(String path) throws IOException
		{
			FileSystem fs=FileSystem.get(config);
			  Path srcPath=new Path(path);
			  
			  
			  try
			  {
				  
				  if(fs.exists(srcPath))
				  {
					
				  if(fs.isDirectory(srcPath)||(fs.isFile(srcPath)))
				  {
					  FileStatus[] filestatus=fs.listStatus(srcPath);
					  Path[] paths = FileUtil.stat2Paths(filestatus);
					  System.out.println("***** Directory or file contained in *****");
					  System.out.println(srcPath);
					  for(Path pathl :paths)
					  {
						  System.out.println(pathl);
					  }
				  }
				  }
				  else
				  {
					  System.out.print(srcPath+" Does not exists...!!");
				  }
			  }catch(Exception e){
					System.out.println("Exception :" + e);
					System.exit(1);
					}finally{
					fs.close();
					}
		}
		public void expunge() throws IOException { //make this clear
			  //JobConf conf=new JobConf(FileMerger.class);
			  FileSystem fs=FileSystem.get(config);
			  
			  try
			  {
				  
				  Trash trash = new Trash(config);
				  trash.expunge();
				  trash.checkpoint();
				  System.out.println("Trash cleared...!!!");
			  }catch(Exception e){
					System.out.println("Exception :" + e);
					System.exit(1);
					}finally{
					fs.close();
					}
		}
		
		
	
		public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		HDFSClient client = new HDFSClient();
//		client.addFile(source, dest);
//		client.appendToFiles(source, dest);
//		client.cat(loc);
//		client.changeMode(source);
//		client.changeUserGroup(user, group, source);
//		client.checkSum(loc);
//		client.copy(source, dest);
//		client.copyFromLocal("/home/noahdata/Narasimman/wc", "tet");
		client.copyToLocal("test/wc", "/home/noahdata/Narasimman/HiveRunner");
//		client.createSnapshot(source, snsName);
//		client.deleteFile(file);
//		client.deleteSnapshot(source);
//		client.du(source);
//		client.expunge();
//		client.find(path);
//		client.get(source, dest);
//		client.getBlockLocations(source);
//		client.getfACL(source);
//		client.getfAttri(source);
//		client.getModificationTime(source);
//		client.getHostnames();
//		client.getModificationTime(source);
//		client.ifExists(source)
//		client.listDirectoryContents(dir);
//		client.mergeFiles(inputFiles, outputFile);
//		client.mkdir(dir);
//		client.move(source, dest);
//		client.moveFromLocal(source, dest);
//		client.moveToLocal(source, dest);
//		client.put(source, dest);
//		client.readFile(file);
//		client.renameFile(fromthis, tothis);
//		client.renameSnapshot(source, snsOldName, snsNewName);
//		client.setfACL(source);
//		client.setfAttri(source);
//		client.setRep(source, repfactor);
//		client.status(source);
//		client.tailofFile(source);
//		client.testDir(dir);
//		client.text(source);
//		client.touchz(path);
		/*if (args.length < 1) {
			printUsage();
			System.exit(1);
			}
			 s
			if (args[0].equals("add")) {
			if (args.length < 3) {
			System.out.println("Usage: hdfsclient add <local_path> " + "<hdfs_path>");
			System.exit(1);
			}
			client.addFile(args[1], args[2]);
			 
			} else if (args[0].equals("read")) {
			if (args.length < 2) {
			System.out.println("Usage: hdfsclient read <hdfs_path>");
			System.exit(1);
			}
			client.readFile(args[1]);
			 
			} else if (args[0].equals("delete")) {
			if (args.length < 2) {
			System.out.println("Usage: hdfsclient delete <hdfs_path>");
			System.exit(1);
			}
			 
			client.deleteFile(args[1]);
			} else if (args[0].equals("mkdir")) {
			if (args.length < 2) {
			System.out.println("Usage: hdfsclient mkdir <hdfs_path>");
			System.exit(1);
			}
			 
			client.mkdir(args[1]);
			}else if (args[0].equals("copyfromlocal")) {
			if (args.length < 3) {
			System.out.println("Usage: hdfsclient copyfromlocal <from_local_path> <to_hdfs_path>");
			System.exit(1);
			}
			 
			client.copyFromLocal(args[1], args[2]);
			} else if (args[0].equals("rename")) {
			if (args.length < 3) {
			System.out.println("Usage: hdfsclient rename <old_hdfs_path> <new_hdfs_path>");
			System.exit(1);
			}
			 
			client.renameFile(args[1], args[2]);
			}else if (args[0].equals("copytolocal")) {
			if (args.length < 3) {
			System.out.println("Usage: hdfsclient copytolocal <from_hdfs_path> <to_local_path>");
			System.exit(1);
			}
			 
			client.copyToLocal(args[1], args[2]);
			}else if (args[0].equals("modificationtime")) {
			if (args.length < 2) {
			System.out.println("Usage: hdfsclient modificationtime <hdfs_path>");
			System.exit(1);
			}
			 
			client.getModificationTime(args[1]);
			}else if (args[0].equals("getblocklocations")) {
			if (args.length < 2) {
			System.out.println("Usage: hdfsclient getblocklocations <hdfs_path>");
			System.exit(1);
			}
			 
			client.getBlockLocations(args[1]);
			} else if (args[0].equals("gethostnames")) {
			 
			client.getHostnames();
			}else {
			 
			printUsage();
			System.exit(1);
			}*/
			 
			System.out.println("Done!");
			}
			
	}

	
	

