package com.J2HApp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.commons.io.FilenameUtils;

public class JSONProcess 
{
	String jarpath = "lib/json-hive-schema-1.0-jar-with-dependencies.jar";
	private String hqlcmd="\n";
	public String ProcessCols(File f, String tblname)
	{
		String res="";
		try
		{
		String line;
		FileReader fr;
		fr = new FileReader(f);
		BufferedReader br = new BufferedReader(fr);
		int lineNum = 0;
			while((line = br.readLine()) != null)
			{ 
			    if (lineNum > 1) 
			    {
			        if (line.trim().length() > 0) 
			        {
			            line=line.trim().replaceAll("\\s+", " ") + " ";
			        }
			    } 
			    else 
			    {
			        line = line.trim().replaceAll("\\s", "");
			    }
			    lineNum++;
			    
			}
			br.close();
			fr.close();
			Runtime rt = Runtime.getRuntime();
			String commands = "java -jar -Xms4096M -Xmx6144M " + jarpath + " " + f.getAbsolutePath() + " " + tblname;
			Process proc = rt.exec(commands);
			BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			String s = null;
			while ((s = stdInput.readLine()) != null) 
			{
				res+=s;
			}
		
		}
        catch (FileNotFoundException e) 
		{
			e.printStackTrace();
		}
		catch (IOException e) 
		{
			e.printStackTrace();
		}
        return res;
	}
	String Process(File inpPath,String dbName,String tblName,String hdfsPath) throws FileNotFoundException
	{
		String status="";
		String cols="";
		hqlcmd="CREATE DATABASE IF NOT EXISTS " + dbName + ";\n";
		hqlcmd+="USE " + dbName + ";\n";
		String path=inpPath.getParent();
	    String directoryName = path.concat(File.separator+"generated_scripts"+File.separator+"json_scripts");
	    File directory = new File(directoryName);
	    if (!directory.exists()){
	        directory.mkdirs();
	    }
		String hqlfileName=tblName.toLowerCase()+".hql";
		String scriptfileName="run_"+tblName.toLowerCase()+".sh";
	    if(!inpPath.isDirectory())
		{
	    	cols = ProcessCols(inpPath,tblName);
	    	hqlcmd+=cols+"\n";
			hqlcmd+="LOAD DATA INPATH '"+hdfsPath+"/"+inpPath.getName()+"' OVERWRITE INTO TABLE "+tblName+";\n";
			hqlcmd+="SELECT * FROM "+tblName+";";
			File filename1 = new File(directoryName+File.separator+hqlfileName);
			if(!filename1.exists())
			{
				try {
					filename1.createNewFile();
					FileWriter fw = new FileWriter(filename1);
					fw.write(hqlcmd);
					fw.close();
					status="HQL Script Generated for the table " + tblName + ".";
				} catch (IOException e) {
					status="I/O Exception";
					e.printStackTrace();
				}
			}
			else
			{
				status="HQL Script already exists for same table";
			}
			String runcmd="\n";
			runcmd="hadoop fs -put "+inpPath+" "+hdfsPath+"/"+inpPath.getName()+"\n";
			runcmd+="hive -f "+filename1.getAbsolutePath();
			File filename2 = new File(directoryName+File.separator+scriptfileName);
			if(!filename2.exists())
			{
				try {
					filename2.createNewFile();
					FileWriter fw = new FileWriter(filename2);
					fw.write(runcmd);
					fw.close();
					status+="\nUnix Script Generated for the table " + tblName + ".";
				} catch (IOException e) {
					status+="I/O Exception";
					e.printStackTrace();
				}
				return status;
			}
			else
			{
				status+="\nUnix Script already exists for same table";
				return status;
			}	
		}
	    else
	    {
	    	int count=0;
			int flag=0;
			File[] jsonFile = inpPath.listFiles(new FileFilter() {
			    @Override
			    public boolean accept(File pathname) {
			        String name = pathname.getName().toLowerCase();
			        return name.endsWith(".json") && pathname.isFile() && !pathname.isHidden();
			    }
			});
			if(jsonFile.length!=0)
			{
			for(File f:jsonFile)
			{
					if(FilenameUtils.getExtension(f.getName()).equals("json"))
						flag++;
					else
						flag--;
					count++;
			}
			if(flag == count)
			{
				int found=0;
				String cols1=ProcessCols(jsonFile[0].getAbsoluteFile(),tblName);
				for(File f:jsonFile)
				{
					if(jsonFile.length>1)
					{
						String cols2=ProcessCols(f.getAbsoluteFile(),tblName);
						if(cols1.equalsIgnoreCase(cols2))
						{
							cols2="";
							continue;
						}
						else
						{
							found=-1;
							break;
						}
					}
					else
					{
						found=0;
						break;
					}
				}
				if(found==-1)
				{
					status="Improper JSON key-value information in one or more files inside the Input folder";
				}
				else
				{
					File directoryy = new File(directoryName);
				    if (!directoryy.exists())
				    {
				        directoryy.mkdirs();
				    }
					cols=ProcessCols(jsonFile[0].getAbsoluteFile(),tblName);
					hqlcmd+=cols+"\n";
					hqlcmd+="LOAD DATA INPATH '"+hdfsPath+"/"+inpPath.getName()+"' OVERWRITE INTO TABLE "+tblName+";\n";
					hqlcmd+="SELECT * FROM "+tblName+";";
					File filename1 = new File(directoryName+File.separator+hqlfileName);
					if(!filename1.exists())
					{
						try {
							filename1.createNewFile();
							FileWriter fw = new FileWriter(filename1);
							fw.write(hqlcmd);
							fw.close();
							status="HQL Script Generated for the table " + tblName + ".";
						} catch (IOException e) 
						{
							status="I/O Exception";
							e.printStackTrace();
						}
					}
					else
					{
						status="HQL Script already exists for same table";
					}
					String runcmd="\n";
					runcmd="hadoop fs -put "+inpPath+" "+hdfsPath+"/"+inpPath.getName()+"\n";
					runcmd+="hive -f "+filename1.getAbsolutePath();
					File filename2 = new File(directoryName+File.separator+scriptfileName);
					if(!filename2.exists())
					{
						try {
							filename2.createNewFile();
							FileWriter fw = new FileWriter(filename2);
							fw.write(runcmd);
							fw.close();
							status+="\nUnix Script Generated for the table " + tblName + ".";
						} catch (IOException e) {
							status="I/O Exception";
							e.printStackTrace();
						}
						return status;
					}
					else
					{
						status+="\nUnix Script already exists for same table";
						return status;
					}	
				}
				return status;
			}
			else
			{
				return "The selected folder does not contain all JSON files";
			}
			}
			else
			{
				return "The input folder does not contain any JSON files or is empty";
			}
	    }
	}
}