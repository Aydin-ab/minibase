package tests;
//originally from : joins.C

import heap.*;
import global.*;
import java.io.*;
import java.util.Arrays;
import java.util.List;


/**
   A public class where we store utility functions and methods used for the IEjoin assignment.
   Contains :
		- File2Heap and File2List to deal with database/query .txt files
    	- read_Query_info to create a CondExpr from a query .txt file
    	- opToSring to convert an int of AttrOperator class to the string equivalent of the equation
    		It's really just for prompting the query in the console
    	- QueryString class that deals with reading the .txt file of the queries
    		and load the important information  so that we code quicker. Also help to prompt the queries
    			Contains :
    				- Constructor that initialize the attributes
    				- promptQuery that print the query in the console
   
*/
public class UtilDriver implements GlobalConst {

	  /** Constructor just for fun
	   */
	  public UtilDriver() {
	  }
    
	  
	/**
	* 
	* BUILD table from "fileInput"
	* 
	*  @parameter fileNameInput : String Name of file containing data
	*  @parameter fileNameOutput : String Name of table saved in the DB
	*  @parameter max_num_tuples : int Max number of tuple to load from the file in case of big files. 
	*  @parameter DATA_DIR : String path of directory containing data
	*  
	*  @return boolean to know if it succeeded or not
	*  
	* **/  
	  static boolean File2Heap(String fileNameInput, String fileNameOutput, int max_num_tuples, String DATA_DIR){
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
		*  @parameter queryFile : String path of file containing data
		*  @parameter DATA_DIR : String path of directory containing data
		*  
		*  @return east the String array (why east ? Mystery.) or null if it failed by IOException.
		*  
		* **/ 
	  
	  static  String[][] File2List(String queryFile, String DATA_DIR)  {
			
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
		*  @parameter op_int : int that represents the operation.
		*  
		*  @return op_String : String that represents the operation.
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
	  
	  
		/**
		* 
		* Fill csv file with inputs
		* 
		*  @parameter queries : List<String>  List of queries you want to test : "1a","1b","2a","2b" or "2c"
		*  @parameter begin : int first value of the num_tuple_Q number of tuples in dataset Q you want
		*  @parameter end : int last value of the num_tuple_Q number of tuples in dataset Q you want
		*  @parameter step : int space between different values
		*  @parameter IE : boolean <code> true </code> if you want data for IE. <code> false </code> if you want data from NLJ
		*  
		* **/  
	  static void fill_csv(List<String> queries, int begin, int end, int step, boolean IE) {
		  
		// path to the Directory containing both the queries and relations .txt files 
			final String DATA_DIR = "QueriesData/"; 
		// Number of pages in the Database. For sysdef instantiation. 1000000 is nice
			int num_pgs = 1000000;
		// Number of pages in the Buffer. For sysdef instantiation. 5000 is nice
			int num_buf = 5000;
		// Number of pages in memory when calling the Sort class. For IESelfJoin, IEJoin and NLJ instantiation. 100 is nice
			int amt_mem = 100; 
			boolean with_NLJ, with_IE;
			// If interested on IE 
			if (IE) {
			
				with_NLJ = false; // If you want to compare NLJ with the IE Self Join of task 2a and 2b and IE Join from task 2c
				with_IE = true;
			
			}
			else {
				
				with_NLJ = true; // If you want to compare NLJ with the IE Self Join of task 2a and 2b and IE Join from task 2c
				with_IE = false;
				
			}

		    boolean verbose = false; // If you want to print the resulting tuples while the queries are running.
		    
		    
		    
		    for (int num_tuples_Q = begin; num_tuples_Q <= end; num_tuples_Q += step) {
			
		    	MyJoinsDriver jjoin = new MyJoinsDriver(num_pgs, num_buf, amt_mem, 
														num_tuples_Q, 
														DATA_DIR);
				boolean sortstatus = jjoin.runTests(queries, with_NLJ, verbose, with_IE); // true if the tests succeed

		    }
			
		  
	  }
	  
	  static void writeToCSV(QueryString query, int num_tuples_Q, float durationTime) {
		  
	       FileWriter pw = null;
	        try {
	            pw = new FileWriter("duration_" + query.queryName + ".csv", true);

		        StringBuilder builder = new StringBuilder();
		        builder.append(Integer.toString(num_tuples_Q)+ "," + Float.toString(durationTime));
		        builder.append('\n');
		        pw.write(builder.toString());
		        pw.close();
	        }
	       catch (Exception e) {
	          e.printStackTrace();
	       } 
		  
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
	  
	  
	  public QueryString(String DATA_DIR, String queryTask) {
		   	
		  	// Query Name
		  	queryName = queryTask.substring(6, queryTask.length()-4);
		  
			// Use File2List to create the String array of the query
			full_query = UtilDriver.File2List(queryTask, DATA_DIR);
			
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


