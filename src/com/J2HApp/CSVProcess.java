package com.J2HApp;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.commons.io.FilenameUtils;

public class CSVProcess 
{
	private String hqlcmd="\n";
	private String[] cols;
	public String ProcessCols(File f,String dl)
	{
		BufferedReader br = null;
		String res="";
		String temp="";
		try {
			br = new BufferedReader(new FileReader(f.getAbsolutePath()));
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
        String header = null;
		try {
			header = br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if(dl.contains(","))
			cols=header.split(",");
		if(dl.contains("	"))
			cols=header.split("	");
        for(String col:cols)
        {
        	temp = col.trim().replaceAll("\"","");
        	temp = temp.trim().replaceAll("[^\\w]", "_");
        	if(temp.startsWith("_"))
        	{
        			temp = temp.replaceAll("^_+","");
        	}
        		res +=  temp +" STRING,";
        }
        if (res.endsWith(",")) 
        	{
        	  res = res.substring(0, res.length() - 1);
        	}
        return res;
	}
	public String Process(File inpPath,String delim,String dbName,String tblName,String hdfsPath) throws FileNotFoundException
	{
		String cols="";
		String status="";
		String dl="";
		if(delim.equalsIgnoreCase("comma"))
			dl="\"" + "," + "\"";
		if(delim.equalsIgnoreCase("tab"))
			dl="\"" + "	" + "\"";
		hqlcmd="CREATE DATABASE IF NOT EXISTS " + dbName + ";\n";
		hqlcmd+="USE " + dbName + ";\n";
		String path=inpPath.getParent();
	    String directoryName = path.concat(File.separator+"generated_scripts"+File.separator+"csv_scripts");
	    File directory = new File(directoryName);
	    if (!directory.exists()){
	        directory.mkdirs();
	    }
		String hqlfileName=tblName.toLowerCase()+".hql";
		String scriptfileName="run_"+tblName.toLowerCase()+".sh";
	    if(!inpPath.isDirectory())
		{
			cols=ProcessCols(inpPath,dl);
			if(dl.contains(","))
				hqlcmd+="CREATE TABLE " + tblName + " ("+cols+") "+"row format delimited fields terminated by ',' stored as textfile tblproperties ('serialization.null.format' = '', 'skip.header.line.count'='1');\n";
			if(dl.contains("	"))
				hqlcmd+="CREATE TABLE " + tblName + " ("+cols+") "+"row format delimited fields terminated by '\\t' stored as textfile tblproperties ('serialization.null.format' = '', 'skip.header.line.count'='1');\n";
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
					status+="HQL Script Generated for the table " + tblName + ".";
				} catch (IOException e) {
					status+="I/O Exception";
					e.printStackTrace();
				}
			}
			else
			{
				status+="HQL Script already exists for same table";
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
				} catch (IOException e) 
				{
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
		else
		{
			int count=0;
			int flag=0;
			File[] csvFile = inpPath.listFiles(new FileFilter() {
			    @Override
			    public boolean accept(File pathname) {
			        String name = pathname.getName().toLowerCase();
			        return name.endsWith(".csv") && pathname.isFile() && !pathname.isHidden();
			    }
			});
			if(csvFile.length!=0)
			{
			for(File f:csvFile)
			{
					if(FilenameUtils.getExtension(f.getName()).equals("csv"))
						flag++;
					else
						flag--;
					count++;
			}
			if(flag==count)
			{
				int found=0;
				String cols1=ProcessCols(csvFile[0].getAbsoluteFile(),dl);
				for(File f:csvFile)
				{
						if(csvFile.length>1)
						{
						String cols2=ProcessCols(f.getAbsoluteFile(),dl);
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
					status="Improper CSV headers information in one or more files inside the Input folder";
				}
				else
				{
					File directoryy = new File(directoryName);
				    if (!directoryy.exists())
				    {
				        directoryy.mkdirs();
				    }
					cols=ProcessCols(csvFile[0].getAbsoluteFile(),dl);
					if(dl.contains(","))
						hqlcmd+="CREATE TABLE " + tblName + " ("+cols+") "+"row format delimited fields terminated by ',' stored as textfile tblproperties ('serialization.null.format' = '', 'skip.header.line.count'='1');\n";
					if(dl.contains("	"))
						hqlcmd+="CREATE TABLE " + tblName + " ("+cols+") "+"row format delimited fields terminated by '\\t' stored as textfile tblproperties ('serialization.null.format' = '', 'skip.header.line.count'='1');\n";
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
							status+="HQL Script Generated for the table " + tblName + ".";
						} catch (IOException e) {
							status="I/O Exception";
							e.printStackTrace();
						}
					}
					else
					{
						status+="HQL Script already exists for same table";
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
				return "The input folder does not contain all CSV files";
			}
			}
			else
			{
				return "The input folder does not contain any CSV files or is empty";
			}
		}
	}
}