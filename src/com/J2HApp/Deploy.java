package com.J2HApp;
import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.io.File;
import java.io.FileNotFoundException;
import javax.swing.filechooser.FileNameExtensionFilter;

public class Deploy extends JFrame
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private JPanel contentPane;
	private File f=null;
	private JTextField inputFile = new JTextField(45);
	private JTextField hadoopLoc = new JTextField(53);
	private JTextField dbName = new JTextField(62);
	private JTextField tblName = new JTextField(50);
	private JTextArea progressStatus = new JTextArea(28,60);
	private JRadioButton csvType = new JRadioButton("CSV", false);
	private JRadioButton jsonType = new JRadioButton("JSON",false);
	private JRadioButton commaDelim = new JRadioButton("Comma",false);
	private JRadioButton tabDelim = new JRadioButton("Tab",false);
	private JRadioButton multipleY = new JRadioButton("Yes, for folder", false);
	private JRadioButton multipleN = new JRadioButton("No, for file",false);
	private ButtonGroup fileType = new ButtonGroup();
	private ButtonGroup delimSet = new ButtonGroup();
	private ButtonGroup multipleF = new ButtonGroup();
	private String userDir = System.getProperty("user.home");
	private JFileChooser chooser = new JFileChooser(userDir +"/Downloads");
	
	public static void main(String[] args) 
	{
		EventQueue.invokeLater(new Runnable() 
		{
	         @Override
	         public void run() {
	            try {
	               Deploy frame = new Deploy();
	               frame.setTitle("Automated Data Ingestion Tool");
	               frame.setResizable(false);
	               frame.setVisible(true);
	            } catch (Exception e) {
	               e.printStackTrace();
	            }
	         }
	      });	
	}
	
	public Deploy() 
	{
	      setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
	      setBounds(100, 100, 850, 600);
	      contentPane = new JPanel();
	      setContentPane(contentPane);
	      JLabel inpType = new JLabel("Input Type: ");
	      contentPane.add(inpType);
	      fileType.add(csvType);
	      fileType.add(jsonType);
	      csvType.setToolTipText("Choose for CSV Type");
	      jsonType.setToolTipText("Choose for JSON Type");
	      contentPane.add(csvType);
	      contentPane.add(jsonType);
	      JLabel delimType = new JLabel("Delimiter Type: ");
	      contentPane.add(delimType);
	      delimSet.add(commaDelim);
	      delimSet.add(tabDelim);
	      commaDelim.setToolTipText("Choose for Comma CSV Delimiter");
	      tabDelim.setToolTipText("Choose for Tab CSV Delimiter");
	      contentPane.add(commaDelim);
	      contentPane.add(tabDelim);
	      JLabel multipleFilter = new JLabel("Multiple Files: ");
	      contentPane.add(multipleFilter);
	      multipleF.add(multipleY);
	      multipleF.add(multipleN);
	      multipleY.setToolTipText("Choose for multiple files");
	      multipleN.setToolTipText("Choose for single file");
	      contentPane.add(multipleY);
	      contentPane.add(multipleN);
	      JLabel inpLoc = new JLabel("Input File/Folder: ");
	      contentPane.add(inpLoc);
	      inputFile.setEditable(false);
	      inputFile.setToolTipText("Choose path for input file/folder");
	      contentPane.add(inputFile);
	      JButton browseBtn = new JButton("Browse");
	      browseBtn.setToolTipText("Open File Browser for selecting input source");
	      contentPane.add(browseBtn);
	      JLabel hadoopPath = new JLabel("HDFS File/Directory Path: ");
	      contentPane.add(hadoopPath);
	      hadoopLoc.setText("/");
	      hadoopLoc.setToolTipText("Give full HDFS path starting with /. Eg. /user/dev");
	      contentPane.add(hadoopLoc);
	      JLabel databaseName = new JLabel("Database name: ");
	      contentPane.add(databaseName);
	      dbName.setToolTipText("Enter Hive Database name");
	      contentPane.add(dbName);
	      JLabel tableName = new JLabel("Table name: ");
	      contentPane.add(tableName);
	      tblName.setToolTipText("Enter Hive Table name");
	      contentPane.add(tblName);
	      JButton resetBtn = new JButton("Reset");
	      resetBtn.setToolTipText("Reset");
	      JButton submitBtn = new JButton("Submit");
	      submitBtn.setToolTipText("Submit");
	      contentPane.add(resetBtn);
	      resetBtn.addActionListener(new ActionListener() {
			
			@Override
			public void actionPerformed(ActionEvent arg0) {
				inputFile.setText("");
				csvType.setSelected(false);
	            jsonType.setSelected(false);
	            fileType.clearSelection();
	            delimSet.clearSelection();
	            multipleF.clearSelection();
	            multipleY.setSelected(false);
	            multipleN.setSelected(false);
	            dbName.setText("");
	            tblName.setText("");
	            hadoopLoc.setText("/");
	            progressStatus.setText("");
	            if(commaDelim.isEnabled()==false)
					commaDelim.setEnabled(true);
				if(tabDelim.isEnabled()==false)
					tabDelim.setEnabled(true);
	            chooser.resetChoosableFileFilters();
	            inpLoc.setText("Input File/Folder: ");
	            chooser.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
			}
		});
	      contentPane.add(submitBtn);
	      browseBtn.addActionListener(new ActionListener() {
			
			@Override
			public void actionPerformed(ActionEvent arg0) {
				
        chooser.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
        FileNameExtensionFilter csvFilter = new FileNameExtensionFilter("Comma Separated Values (.csv)", "csv");
        FileNameExtensionFilter jsonFilter = new FileNameExtensionFilter("JSON Files (.json)", "json");
        if(csvType.isSelected())
        {
        	if(multipleY.isSelected())
        		chooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
        	else if(multipleN.isSelected())
        		chooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
        	else
        		chooser.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
        	chooser.resetChoosableFileFilters();
        	chooser.setAcceptAllFileFilterUsed(false);
        	chooser.addChoosableFileFilter(csvFilter);
        }
        else if(jsonType.isSelected())
        {
        	if(multipleY.isSelected())
        		chooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
        	else if(multipleN.isSelected())
        		chooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
        	else
        		chooser.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
        	chooser.resetChoosableFileFilters();
        	chooser.setAcceptAllFileFilterUsed(false);
        	chooser.addChoosableFileFilter(jsonFilter);	
        }
        else
        {
        chooser.setAcceptAllFileFilterUsed(true);
        }
        int result = chooser.showOpenDialog(browseBtn);
        if (result == JFileChooser.APPROVE_OPTION) {
        	f=chooser.getSelectedFile();
            inputFile.setText(chooser.getSelectedFile().getAbsoluteFile().toString());
        }
        
			}
		});
	      csvType.addActionListener(new ActionListener() {
			
			@Override
			public void actionPerformed(ActionEvent arg0) {
				if(inputFile.getText()!="")
					inputFile.setText("");
				if(commaDelim.isEnabled()==false)
					commaDelim.setEnabled(true);
				if(tabDelim.isEnabled()==false)
					tabDelim.setEnabled(true);
			}
		});
	      jsonType.addActionListener(new ActionListener() {
				
				@Override
				public void actionPerformed(ActionEvent arg0) {
					if(inputFile.getText()!="")
						inputFile.setText("");
					delimSet.clearSelection();
					commaDelim.setEnabled(false);
				    tabDelim.setEnabled(false);
				}
			});
	      multipleY.addActionListener(new ActionListener() {
				
				@Override
				public void actionPerformed(ActionEvent arg0) {
					inpLoc.setText("Input Folder:");
					if(inputFile.getText()!="")
						inputFile.setText("");
				}
			});
	      multipleN.addActionListener(new ActionListener() {
				
				@Override
				public void actionPerformed(ActionEvent arg0) {
					inpLoc.setText("Input Files:");
					if(inputFile.getText()!="")
						inputFile.setText("");
				}
			});
	      submitBtn.addActionListener(new ActionListener() {
	         @Override
	         public void actionPerformed(ActionEvent e) {
	            String errormsg="";
	            if(errormsg!="")
	            	errormsg="";
	            if(csvType.isSelected()==false && jsonType.isSelected()==false)
	            {
	            	errormsg+="*Please choose File Type Filter\n";
	            }
	            if(jsonType.isSelected()==false)
	            {
	            if(csvType.isSelected()==true && (commaDelim.isSelected()==false)&&(tabDelim.isSelected()==false))
	            {
	            	errormsg+="*Please choose Delimiter Type\n";
	            }
	            }
	            if(multipleY.isSelected()==false && multipleN.isSelected()==false)
	            {
	            	errormsg+="*Please choose Multiple Files Filter\n";
	            }
	            
	            if(inputFile.getText().isEmpty())
	            {
	            	errormsg+="*Please choose Input File/Folder Path\n";
	            }
	            
	            if(hadoopLoc.getText().equals("/") || hadoopLoc.getText().isEmpty())
	            {
	            	errormsg+="*Please input Hadoop File Path\n";
	            }
	            
	            if(dbName.getText().isEmpty())
	            {
	            	errormsg+="*Please input Database Name\n";
	            }
	            
	            if(tblName.getText().isEmpty())
	            {
	            	errormsg+="*Please input Table Name\n";
	            }
	            
	            if(errormsg!="")
	            	progressStatus.setText("\nPlease provide the required information:"+"\n\n"+errormsg);
	            else
	            {
	                String dl="";
	                if(commaDelim.isSelected())
	                	dl="Comma";
	                if(tabDelim.isSelected())
	                	dl="tab";
	            	progressStatus.setText("Progressing.....Please wait");
	            	if(csvType.isSelected())
	            	{
	            CSVProcess csvp=new CSVProcess();
	            try {
					String result="Progressing.....Please wait";
					result = csvp.Process(f,dl,dbName.getText(),tblName.getText(),hadoopLoc.getText());
					progressStatus.setText(result);
	            } catch (FileNotFoundException e1) {
					e1.printStackTrace();
				}
	            	}
	            	else
	            	{
	            		JSONProcess jsvp=new JSONProcess();
	            		String result="Progressing.....Please wait";
						try {
							result = jsvp.Process(f,dbName.getText(),tblName.getText(),hadoopLoc.getText());
							progressStatus.setText(result);
						} catch (FileNotFoundException e1) {
							e1.printStackTrace();
						}
	            	}
	            }
	         }
	      });
	      progressStatus.setBackground(Color.LIGHT_GRAY);
	      progressStatus.setForeground(Color.RED);
	      progressStatus.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));
	      progressStatus.setFont(new Font("Dialog", Font.BOLD, 12));
	      progressStatus.setEditable(false); 
	      contentPane.add(progressStatus);
	}
}