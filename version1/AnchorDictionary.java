package version1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Scanner;

/*Class created single merged index, from the wikipedia anchor - inlinks crawl collection */
public class AnchorDictionary { 	
	
	/*input - Directory with Wikipedia inlink parsing ouput */	
	public static void main(String[] args) {
		AnchorDictionary ancDic = new AnchorDictionary();
		String inlinkDir = args[0];
		String indexDir = args[1];
		ancDic.getMergedIndex(inlinkDir, indexDir);
		
	}//End main()
	
	/*Merge the index files in inlinkDir to a single sorted index in indexDir*/
	public void getMergedIndex(String inlinkDir, String indexDir){	
		
		File tempDir  = new File("temp");
		tempDir.mkdir();
		File temp2Dir = new File("temp2");
		temp2Dir.mkdir();
		
		File inlinkD = new File(inlinkDir);
		if (inlinkD.isDirectory()){
			mergeDir(inlinkD,"temp");
			int sortFilesCount = tempDir.listFiles().length; 
			while(sortFilesCount != 1){
				mergeDir(tempDir, "temp2"); //create sorted index of temp in temp2
				//System.out.println("Sorted files in Temp2 : "+temp2Dir.listFiles().length);
				for(File file: tempDir.listFiles()) file.delete(); //clean directory temp
				//System.out.println("Temp cleaned : "+tempDir.listFiles().length);
				//copy temp2 to temp
				for(File file: temp2Dir.listFiles()){
					file.renameTo(new File(tempDir.getAbsolutePath()+"/"+file.getName()));
				}				
				//System.out.println("Temp reloaded : "+tempDir.listFiles().length);
				sortFilesCount = tempDir.listFiles().length;
				for(File file: temp2Dir.listFiles()) file.delete(); //clean directory temp2
				//System.out.println("Temp2 cleaned : "+temp2Dir.listFiles().length);
			}//End while		    
		
			//copy index into destination indexDir
			boolean copyIndex = tempDir.listFiles()[0].renameTo(new File(indexDir+tempDir.listFiles()[0].getName()));
			if(!copyIndex){
				System.out.println("FAILED : copy index into "+indexDir+tempDir.listFiles()[0]);
				System.out.println("Index file in "+tempDir.getAbsolutePath());
			}
		}
	}//End getMergedIndex
	
   /*merge sort - merge the N files in inlinkDir files to N/2 sorted index files*/
   void mergeDir(File inlinkDir,String indexDir){
		File[] listOfFiles = inlinkDir.listFiles();
	      if(listOfFiles!=null) {
	    	 if(listOfFiles.length % 2 == 0) { // no remaining files	    	 
		         for (int i = 0; i < listOfFiles.length; i=i+2){
		        	mergeTwo(listOfFiles[i],listOfFiles[i+1],indexDir);
		         }
	    	 } else {
	    		 for (int i = 0; i < listOfFiles.length-1; i=i+2){ //System.out.println("Merging "+i+" and "+(i+1));
			       	mergeTwo(listOfFiles[i],listOfFiles[i+1],indexDir);
			     }
	    		 //copy the last remaining file	    		 
	    		 listOfFiles[listOfFiles.length-1].renameTo(new File(indexDir+"/"+listOfFiles[listOfFiles.length-1].getName()));	    	    	
	    	 }//End if remaining files		     
	      } else {
	        System.out.println(inlinkDir.getAbsolutePath() + " FOUND EMPTY");
	      }		
    }//End method mergeDir
	
   /*merge file1 and file2 in inlineDir into file1 in indexDir */
   public void mergeTwo(File inputfile1, File inputfile2,String indexDir) {
    try {
      File outputfile = new File(indexDir + "/" +inputfile1.getName());
      //outputfile.getParentFile().mkdirs();

      Scanner readerL = new Scanner(inputfile1);
      Scanner readerR = new Scanner(inputfile2);
      PrintWriter printWriter = new PrintWriter(outputfile);
      	  
	  String line1 = readerL.nextLine();
      String line2 = readerR.nextLine();
      while (line1 != null || line2 != null) {
        if (line1 == null) {
           printWriter.println(line2);
           line2 = readLine(readerR);
        } else if (line2 == null) {
           printWriter.println(line1);
           line1 = readLine(readerL);        
        } else {
        	String anchor1 = getAnchor(line1);
	    	String anchor2 = getAnchor(line2);
	        if (anchor1.compareToIgnoreCase(anchor2) <= 0) {//anchors are same
	           printWriter.println(line1);
	           line1 = readLine(readerL);
	        } else {
	           printWriter.println(line2);
	           line2 = readLine(readerR);
	        }
         }
      }//End while
      
      readerL.close();
      readerR.close();
      printWriter.close ();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }

  }//End method mergeTwo

   public String readLine(Scanner reader){
	  if (reader.hasNextLine()){	
		  return reader.nextLine();
	  } else {
		  return null;
	  }
  }
   public String getAnchor(String record) {
	  String anchor=null;
      if(record.contains("|")){
	      	anchor = record.split("|")[0];
	    }		    
	  return anchor;
  }//End method getAnchor

	
}//End Class AnchorDictionary
