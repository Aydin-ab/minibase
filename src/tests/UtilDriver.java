package tests;
//originally from : joins.C

import iterator.*;
import heap.*;
import global.*;
import index.*;
import java.io.*;
import java.util.*;
import java.lang.*;
import diskmgr.*;
import bufmgr.*;
import btree.*; 
import catalog.*;

/**
   A public class where we store utility functions and methods used for the IEjoin assignment.
   Contains :
		- File2Heap and File2List to deal with database/query .txt files
    	- read_Query_info to create a CondExpr from a query .txt file
   
*/


public class UtilDriver implements GlobalConst {
	
	  // The path from the workspace to the directory where all the .txt files are (data and queries)
	  private static String DATA_DIR = "QueriesData/";
	
	  
	  /** Constructor just for fun
	   */
	  public UtilDriver() {
		  String a = "Hello handsome";
	  }
    
	  
	/**
	* 
	* BUILD table from "fileInput"
	* 
	*  @parameter fileNameInput Name of file containing data
	*  @parameter fileNameOutput Name of table saved in the DB
	*  @parameter max_num_tuples Max number of tuple to load from the file in case of big files. 
	*  
	*  @return boolean to know if it succeeded or not
	*  
	* **/  
	  static boolean File2Heap(String fileNameInput, String fileNameOutput, int max_num_tuples){
	  		String pathInput = DATA_DIR + fileNameInput;
		    if(pathInput==null || fileNameOutput==null) {
		  
		    	return false;
		  
		    }
		    
		    if(max_num_tuples<=0) {
		    
		    	max_num_tuples=Integer.MAX_VALUE; // Load tuples until the HeapFile can contain them
		    
		    }
		    
		    
		    /* Create relation */
		     
		    AttrType [] types = new AttrType[4];
		    types[0] = new AttrType (AttrType.attrInteger);
		    types[1] = new AttrType (AttrType.attrInteger);
		    types[2] = new AttrType (AttrType.attrInteger);
		    types[3] = new AttrType (AttrType.attrInteger);
		     
		    short numField=4;
		       
		    Tuple t = new Tuple();
		       
		    try {
		  
		    	t.setHdr(numField,types, null);
		     
		    }
		    catch (Exception e) {
		       
				 System.err.println("*** error in Tuple.setHdr() ***");
				 e.printStackTrace();
				 return false;
		     
		     }
		       
		     int t_size = t.size();
		       
		     RID rid;
		       
		     Heapfile f = null;
		       
		     try {
		    	  
		    	 f = new Heapfile(fileNameOutput);
		      
		     }
		       
		     catch (Exception e) {
		    	  
		    	 System.err.println("*** error in Heapfile constructor ***");
		    	 e.printStackTrace();
		    	 return false;
	
		     }
		       
		       
		     t = new Tuple(t_size);
		      
		     try {
		    
		    	 t.setHdr((short) 4, types, null);
		      
		     }
		      
		     catch (Exception e) {
	
		    	 System.err.println("*** error in Tuple.setHdr() ***");
				 e.printStackTrace();
				 return false;
		     }
		      
		     int cont=0; // To limit the size of table
		      
		     try {
		    
		    	 File file = new File(pathInput);
		    	 BufferedReader reader=null;
				 reader = new BufferedReader(new FileReader(file));
				
				 String text = null;
				 text = reader.readLine(); //To skip header
				 text="";
				   
				 while ((text = reader.readLine()) != null && cont!=max_num_tuples) {
				     
					 String[] attributes=text.split(",");
					 t.setIntFld(1, Integer.parseInt(attributes[0]));
					 t.setIntFld(2, Integer.parseInt(attributes[1]));    
					 t.setIntFld(3, Integer.parseInt(attributes[2]));
					 t.setIntFld(4, Integer.parseInt(attributes[3]));
					 f.insertRecord(t.getTupleByteArray());
					 cont++;
				  
				   }
				   
				 reader.close();
				   
		      }
		      
		      catch(FileNotFoundException e1) {
		    	 
		    	  System.err.println("*** File "+pathInput+" ***");
		    	  e1.printStackTrace();
				  return false;
		      
		      }
		      catch (Exception e) {
		       
			      System.err.println("*** Heapfile error in Tuple.setIntFld() ***");
			      e.printStackTrace();
			      return false;
		        
		      }   
		      
		      System.out.println("Number of tuple inserted: "+cont);  
		      return true;
		      
	  }
	  
		/**
		* 
		* Give String array from "queryFile" of a query so each line is a line and each column is a word
		* 
		*  @parameter queryFile Name of file containing data
		*  
		*  @return east the String array (why east ? Mystery.) or null if it failed by IOException.
		*  
		* **/ 
	  
	  static  String[][] File2List(String queryFile)  {
			
		  	String queryPath = DATA_DIR + queryFile;
			BufferedReader abc;
			
			
			try {
				abc = new BufferedReader(new FileReader(queryPath));
				
				int M=(int) abc.lines().count();
				abc.close();
				
				int i=0;
				
				String [][] east ;
				String line;
				
				abc = new BufferedReader(new FileReader(queryPath));
				
				line = abc.readLine() + ","+ Integer.toString(0);
				
				String[] dimes = line.split(",");
					
				int N=dimes.length;
			
				east= new String [M][N];
			 
				east[0]=dimes;
	 
				for( i=1;i<M;i++) {
					line = abc.readLine();
				   	dimes = line.split("\\s+");
				   	east[i]=dimes;	
	            }
				
				abc.close();
				
				return east;
			}
			
			catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return null;
			}
		 
	  }
	  
		/**
		* 
		* Convert an int representing an operator to the String representation of it.
		* It is only used when displaying the queries in the console so it's just esthetic
		* 
		*  @parameter op_int int that represents the operation.
		*  
		*  @return op_String String that represents the operation.
		*  
		* **/ 
	  
	  static String opToSring(int op_int) {
		  

		  String op_String;
		  
		  switch (op_int) {
		  
		  	case 0 :
		  		op_String = "=";
		  		break;
		  		
		  	case 1 :
		  		op_String = "<";
		  		break;
		  		
		  	case 2 :
		  		op_String = ">";
		  		break;
		  		
		  	case 3 :
		  		op_String = "!=";
		  		break;
		  		
		  	case 4 :
		  		op_String = "<=";
		  		break;
		  		
		  	case 5 :
		  		op_String = ">=";
		  		break;
		  		
		  	default :
		  		op_String = "?? Unable to find the operation";
		  		
		  }
		  
		  return op_String;
		  
	  }
	  
	  

	  
	  
	  
}



/**
* 
* Class for prompting the queries in the console
* Its parameters contains the strings necessary to write the query
*   
* **/ 
class QueryString {
	  
	  // Query name 
	  String queryName;
	  
	  // Full query text
	  String[][] full_query;
	  
	  // Number of predicate;
	  int predicate;
	  // AND/OR
	  String and_or;
	  
	  // Lines of the query
	  String[] proj_line;
	  String[] rel_line;
	  String[] pred1_line;
	  String[] pred2_line; // If there is a second predicate.
	
	  // Words of the query
			// The projection attributes
	  String proj1; // Relation 1
	  String proj2 ; // Relation 2
			// The relations name
	  String rel1; // Relation 1
	  String rel2; // Relation 2
			// The first condition operator and operands
	  String pred1_operand1; // Relation 1
	  int pred1_op; // Notation of the class AttrOperator
	  String pred1_operand2; // Relation 2
			// The second condition operator and operands
	  String pred2_operand1; // Relation 1
	  int pred2_op; // Notation of the class AttrOperator
	  String pred2_operand2; // Relation 2
	  
	  
	  public QueryString(String queryTask) {
		   	
		  	// Query Name
		  	queryName = queryTask.substring(6, 8);
		  
			// Use File2List to create the String array of the query
			full_query = UtilDriver.File2List(queryTask);
			
			// Load the lines of the query
			proj_line = full_query[0];
			rel_line = full_query[1];
			pred1_line = full_query[2];
			
			// Load the words of the query
				// The projection attributes
			proj1 = proj_line[0].substring(0,3); // Relation 1
			proj2 = proj_line[0].substring(4); // Relation 2
				// The relations name
			rel1 = rel_line[0]; // Relation 1
			if (rel_line.length == 2) { // If it is a join between 2 relations
				
				rel2 = rel_line[1]; // Relation 2

			}
			else {
				rel2 = "";
				
			}
				// The first condition operator and operands
			pred1_operand1 = pred1_line[0]; // Relation 1
			pred1_op = Integer.parseInt(pred1_line[1]); // Notation of the AttrOperator class
			pred1_operand2 = pred1_line[2]; // Relation 2
			
			// Check if there are 3 lines (single predicate) or more (5 lines so it's double predicate)
			if (full_query.length == 5) {
				
				// This means there are 5 lines so 2 predicated
				predicate = 2;
				// AND/OR
				and_or = full_query[3][0]; 
				
				// The line of the second predicate
				pred2_line = full_query[4];
				
				// The second condition operator and operands
				pred2_operand1 = pred2_line[0]; // Relation 1
				pred2_op = Integer.parseInt(pred2_line[1]); // Notation of the AttrOperator class
				pred2_operand2 = pred2_line[2]; // Relation 2
				
			}
			else {
				
				// Only 1 predicate
				predicate = 1;
				
			}
			
		  
	  }
	  
	  public void promptQuery() {
		  
		  // If one predicate only
		  String text = "Query " + queryName.charAt(0) + "." + queryName.charAt(1) + " text : \n\n"
			      + "  SELECT " + proj1 + " , " + proj2 + "\n"
			      + "  FROM   " + rel1 + " , " + rel2 + "\n"
			      + "  WHERE  " + pred1_operand1 + " " + UtilDriver.opToSring(pred1_op) + " " + pred1_operand2 ;
		  
		  // If two predicates
		  if (predicate == 2) {
			  
			  // Second predicate
			  String predicate2_text =  " " + and_or + " " + pred2_operand1 + " " + UtilDriver.opToSring(pred2_op) + " " + pred2_operand2;
			  text = text + predicate2_text;
		  }
		  
		  // Just add the jumping lines
		  text = text + " \n\n";
		  
		  // Now we print the query
		  System.out.print("********************** Query" + queryName.charAt(0) + "." + queryName.charAt(1) + " starting *********************\n");

		  System.out.print (text);
		    
		  
	  }
				
	  
}


