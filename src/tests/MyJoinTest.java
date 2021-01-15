package tests;

import iterator.*;
import heap.*;
import global.*;
import java.io.*;
import java.util.Arrays;
import java.util.List;




/**
* JoinsDriver for the IEJoins assignment of lab3 of DBSys course. 
*    Contains :
		- Constructor that sets the parameters and creates the database and the heapfiles 
    	- read_Query_info to create a CondExpr from a query .txt file
    	- runTests that run the tests chosen in the main
    	- SinglePredicate that generate the CondExpr for single predicate queries
    	- DoublePredicate that generate the CondExpr for double predicate queries
    	- All the queries we could do :
    		Query_1_a
    		Query_1_b
    		Query_2_a
    		Query_2_b
    		Query_2_c
    	- Disclaimer because why not
    	- The main to run, where we set all the parameters of the tests.
    		It should be complete enough to do whatever you want to manipulate the test
    		Just need to read the comments
*/
class MyJoinsDriver implements GlobalConst {
	
	private boolean OK = true;
	private boolean FAIL = false; 
	
	// ****   We read and save the queries informations into many QueryString variable ****
	// ****   so that it is more readable and intuitive when we read the code later on ****
	// ****   We use them to write the queries in the console for example. ****
	// ****   Or for the Query_*_CondExpr functions ****
	
	// QueryString (class we created) to read and prompt the queries in the console
	// Calls File2List()
	private String DATA_DIR; // String path to the Directory of the query
	QueryString query1a;
	QueryString query1b;
	QueryString query2a;
	QueryString query2b;
	QueryString query2c;
	QueryString query2c_1;
	QueryString query2c_2;
			
	// Parameters for the test. To be defined in the main
	int num_pgs; // For sysdef instantiation
	int num_buf; // For sysdef instantiation
	int amt_mem; // For IESelfJoin, IEJoin and NLJ instantiation
	int max_num_tuples_Q;
	
	/**
	* Constructor of the JoinsDriver where we put the data into heapfiles. 
	* 
	* @param  _num_pgs : int Number of pages of the Database
	* @param  _num_buf : int Number of pages in the Buffer
	* @param  _amt_mem : int Number of page in memory of the Sort class when calling for joins (IE Self Join, IE Join and NLJ)
	* @param  max_num_tuples_Q : int Number of tuple loaded from q. Heap can support < 30 000 but after 2500, joins become very long
	* 
	*/
	public MyJoinsDriver(int _num_pgs, int _num_buf, int _amt_mem, int _max_num_tuples_Q, String _DATA_DIR) {
		
		// Create the queries String informations and store them inside our QueryString class we made
		DATA_DIR = _DATA_DIR;
		query1a = new QueryString(DATA_DIR, "query_1a.txt");
		query1b = new QueryString(DATA_DIR, "query_1b.txt");
		query2a = new QueryString(DATA_DIR, "query_2a.txt");
		query2b = new QueryString(DATA_DIR, "query_2b.txt");
		query2c = new QueryString(DATA_DIR, "query_2c.txt");
		query2c_1 = new QueryString(DATA_DIR, "query_2c_1.txt");
		query2c_2 = new QueryString(DATA_DIR, "query_2c_2.txt");

		
		num_pgs = _num_pgs;
		num_buf = _num_buf;
		amt_mem = _amt_mem;
		
		String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb"; 
	    

	    @SuppressWarnings("unused")
		SystemDefs sysdef = new SystemDefs( dbpath, num_pgs, num_buf, "Clock" );
	    
	    
		max_num_tuples_Q = _max_num_tuples_Q; // Don't get it too much... Heap can support < 30 000
		final int MAX_NUM_TUPLES_R = 200;	// Max capacity of R
		final int MAX_NUM_TUPLES_S = 300; // Max capacity of S

		
		// Load relations into heap files with File2Heap and check if it worked
		boolean q_heap_exist = UtilDriver.File2Heap("q.txt", "Q.in", max_num_tuples_Q, DATA_DIR);
		if (!q_heap_exist) {
			System.err.println("File2Heap did not succeed \n" +
					"q.in heap file failed");
		}
		
		boolean R_heap_exist = UtilDriver.File2Heap("R.txt", "R.in", MAX_NUM_TUPLES_R, DATA_DIR);
		if (!R_heap_exist) {
			System.err.println("File2Heap did not succeed \n" +
					"R.in heap file failed");
		}
		
		boolean S_heap_exist = UtilDriver.File2Heap("S.txt", "S.in", MAX_NUM_TUPLES_S, DATA_DIR);
		if (!S_heap_exist) {
			System.err.println("File2Heap did not succeed \n" +
					"S.in heap file failed");
		}
		

	}
	
	/**
	* Run the tests. 
	* 
	* @param  queries  : List<String> list of queries to be run
	* @param  correctness_NLJ : boolean set to <code> true </code> if you want to check 2a, 2b and 2c with NLJ
	* @param  verbose : boolean set to <code> true </code> if you want to print the resulting tuple (even for NLJ if you choose it to correct IE)
	* @param  verbose : boolean set to <code> true </code> if you want to use IE Join (for example to measure duration of NLJ)
	* 
	* @return <code> true </code> if the tests ran without errors
	* 
	*/
	public boolean runTests(List<String> queries, boolean correctness_NLJ, boolean verbose, boolean with_IE) {
		
		Disclaimer();
		if (queries.contains("1a")) {
			
			Query_1_a(verbose);

		}
		if (queries.contains("1b")) {
			
			Query_1_b(verbose);

		}
		if (queries.contains("2a")) {
			
			Query_2_a(correctness_NLJ, verbose, with_IE);

		}
		if (queries.contains("2b")) {
	
			Query_2_b(correctness_NLJ, verbose, with_IE);

		}
		if (queries.contains("2c")) {
				
			Query_2_c(query2c, correctness_NLJ, verbose, with_IE);

		}
		if (queries.contains("2c_1")) {
			
			Query_2_c(query2c_1, correctness_NLJ, verbose, with_IE);

		}
		if (queries.contains("2c_2")) {
			
			Query_2_c(query2c_2, correctness_NLJ, verbose, with_IE);

		}
	    
	    System.out.print ("Finished joins testing"+"\n");
	   
	    
	    return true;
		    
	}
	  
	/**
	* Define the predicate for task 1a, 2a for which queries have 1 predicate. 
	*
	* @param  expr  : CondExpr[] list of objects of type CondExpr that describe the query's predicate
	* @param  query : QueryString object of type QueryString that describe the different part of the text of the query
	* 
	*/
	private void SinglePredicate(CondExpr[] expr, QueryString query) {
	
	    expr[0].next  = null;
	    expr[0].op    = new AttrOperator(query.pred1_op);
	    expr[0].type1 = new AttrType(AttrType.attrSymbol);
	    expr[0].type2 = new AttrType(AttrType.attrSymbol);
	    // First Relation = Outer
	    char operand1_attribute_char = query.pred1_operand1.charAt(2); // For example operand1 = R_3 so we extract the "3" here
	    int operand1_attribute_int = Integer.parseInt(String.valueOf(operand1_attribute_char)); // And change it from char "3" to int 3
	    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer), operand1_attribute_int);
	    // Second Relation = Inner
	    char operand2_attribute_char = query.pred1_operand2.charAt(2); // For example operand2 = S_3 so we extract the "3" here
	    int operand2_attribute_int = Integer.parseInt(String.valueOf(operand2_attribute_char)); // And change it from char "3" to int 3
	    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel), operand2_attribute_int);
	 
	    expr[1] = null;
	    
	}
	
	
	/**
	* Define the predicates for task 1b, 2b, 2c for which queries have 2 predicates. 
	* 
	* @param  expr  : list of objects of type CondExpr that describe the query's predicates
	* @param  query : object of type QueryString that describe the different part of the text of the query
	* 
	*/
	private void DoublePredicate(CondExpr[] expr, QueryString query) {
		// We have 2 predicates so we construct expr = [pred1, pred2, null]
		// I don't get why there isn't this format in JoinTest for multiple queries.. I find it easier and more intuitive
		
		// First predicate
	    expr[0].next  = null;
	    expr[0].op    = new AttrOperator(query.pred1_op);
	    expr[0].type1 = new AttrType(AttrType.attrSymbol);
	    expr[0].type2 = new AttrType(AttrType.attrSymbol);
	    // First Relation is Outer
	    char pred1_operand1_attribute_char = query.pred1_operand1.charAt(2); // For example operand1 = R_3 so we extract the "3" here
	    int pred1_operand1_attribute_int = Integer.parseInt(String.valueOf(pred1_operand1_attribute_char)); // And change it from char "3" to int 3
	    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer), pred1_operand1_attribute_int);
	    // Second Relation is Inner
	    char pred1_operand2_attribute_char = query.pred1_operand2.charAt(2); // For example operand2 = S_3 so we extract the "3" here
	    int pred1_operand2_attribute_int = Integer.parseInt(String.valueOf(pred1_operand2_attribute_char)); // And change it from char "3" to int 3
	    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel), pred1_operand2_attribute_int);
	 

		// Second predicate
	    expr[1].next  = null;
	    expr[1].op    = new AttrOperator(query.pred2_op);
	    expr[1].type1 = new AttrType(AttrType.attrSymbol);
	    expr[1].type2 = new AttrType(AttrType.attrSymbol);
	    // First Relation is the Outer
	    char pred2_operand1_attribute_char = query.pred2_operand1.charAt(2); // For example operand1 = R_3 so we extract the "3" here
	    int pred2_operand1_attribute_int = Integer.parseInt(String.valueOf(pred2_operand1_attribute_char)); // And change it from char "3" to int 3
	    expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer), pred2_operand1_attribute_int);
	    // Second Relation is the Inner
	    char pred2_operand2_attribute_char = query.pred2_operand2.charAt(2); // For example operand2 = S_3 so we extract the "3" here
	    int pred2_operand2_attribute_int = Integer.parseInt(String.valueOf(pred2_operand2_attribute_char)); // And change it from char "3" to int 3
	    expr[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel), pred2_operand2_attribute_int);
	 
	    expr[2] = null;

	    
	}
	
	
	/**
	* Run the query of task 1a. Nested Loop Join with 1 predicates
	* 
	* @param  verbose : boolean set to <code> true </code> if you want to print the resulting tuples
	* 
	*/
	public void Query_1_a(boolean verbose) {
		
		long startTime = System.currentTimeMillis(); // To measure durations of the query

	    query1a.promptQuery(); // Print the SQL query in the console
		System.out.print ("\n(Tests FileScan, Projection, and Nested Loop Join)\n");

	    boolean status = OK;

	    // Defining the single predicate
		CondExpr[] outFilter = new CondExpr[2];
	 	outFilter[0] = new CondExpr();
	 	outFilter[1] = new CondExpr();
 
	 	SinglePredicate(outFilter, query1a);
	 	
	 		 	
	 	// ************* 		Rel_1 = R and Rel_2 = S in template			*************
	 	
	 	// The attributes types of the outer relation.
	 	// Every relations q, R and S has int types anyway.
	    AttrType [] Rel_1_types = new AttrType[4];
	    Rel_1_types[0] = new AttrType (AttrType.attrInteger);
	    Rel_1_types[1] = new AttrType (AttrType.attrInteger);
	    Rel_1_types[2] = new AttrType (AttrType.attrInteger);
	    Rel_1_types[3] = new AttrType (AttrType.attrInteger);

		
	    // There are no strings anyway
	    short [] Rel_1_sizes = new short[0];
	   
	    // Projections on the columns for the FileScan
	    // Every relations q, R and S has 4 columns anyway.
	    FldSpec [] Rel_1_projection = new FldSpec[4];
	    Rel_1_projection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
	    Rel_1_projection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
	    Rel_1_projection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
	    Rel_1_projection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

	    
	    // File scan the outer relation rel1 (R in the example)
	    FileScan am = null;
	    try {
	    
	    	am  = new FileScan( query1a.rel1 + ".in", Rel_1_types, Rel_1_sizes, 
					  (short)4, (short)4,
					  Rel_1_projection, null);
	    
	    }
	    
	    catch (Exception e) {
	    
	    	status = FAIL;
	    	System.err.println (""+e);
	    
	    }

	    if (status != OK) {
	      
	    	//bail out
	    	System.err.println ("*** Error setting up scan for Relation 1 (outer)");
	    	Runtime.getRuntime().exit(1);
	      
	    }
	    
	 	// The attributes types of the inner relation. 
	 	// Every relations q, R and S has int types anyway.
	    AttrType [] Rel_2_types = new AttrType[4];
	    Rel_2_types[0] = new AttrType (AttrType.attrInteger);
	    Rel_2_types[1] = new AttrType (AttrType.attrInteger);
	    Rel_2_types[2] = new AttrType (AttrType.attrInteger);
	    Rel_2_types[3] = new AttrType (AttrType.attrInteger);

	    // There are no strings anyway in q, R and S
	    short [] Rel_2_sizes = new short[0]; 
	    
	    
	    // **** Projection settings
	    // 2 columns on which we project
	    FldSpec [] proj_list = new FldSpec[2];
	    // Outer is the first relation
	    char proj1_attribute_char = query1a.proj1.charAt(2); // For example proj1 = R_1 so we extract the "1" here
	    int proj1_attribute_int = Integer.parseInt(String.valueOf(proj1_attribute_char)); // And change it from char "1" to int 1
	    proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), proj1_attribute_int); 
	    // Inner is the second relation
	    char proj2_attribute_char = query1a.proj2.charAt(2); // For example proj2 = S_1 so we extract the "1" here
	    int proj2_attribute_int = Integer.parseInt(String.valueOf(proj2_attribute_char)); // And change it from char "1" to int 1
	    proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), proj2_attribute_int);

	    
	    // The attributes types.
	 	// Every relations q, R and S has int types anyway.
 	    AttrType [] jtype = new AttrType[4];
	    jtype[0] = new AttrType (AttrType.attrInteger);
	    jtype[1] = new AttrType (AttrType.attrInteger);
	    jtype[2] = new AttrType (AttrType.attrInteger);
	    jtype[3] = new AttrType (AttrType.attrInteger);

	 
	    // Nested Loop Join
		NestedLoopsJoins nlj = null;
		try {
		
			nlj = new NestedLoopsJoins (Rel_1_types, 4, Rel_1_sizes,
					Rel_2_types, 4, Rel_2_sizes,
					10,
					am, query1a.rel1 + ".in",
					outFilter, null, proj_list, 2);
		
		}
		
		catch (Exception e) {
		
			System.err.println ("*** Error preparing for Nested Loop Join");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		
		}
		
		
		// OK, now we start reading the results, print them on the console if verbose = True
		// + count them and load everything in a .txt file for analysis purpose
	 	Tuple t = new Tuple();
		t = null; // Initialize the current tuple
		int i = 0; // To count the tuples
		
		try {
		
			// (Thank you Stackoverflow)
			// To save the results in a txt file
			PrintWriter printer = new PrintWriter("output_query_1_a.txt");
		
			// Iterate through the results
			while ((t = nlj.get_next()) != null) {
				// If user want to print the tuples
				if (verbose) {
					
					// Print the rank of the tuple and the tuple itself
					System.out.print("\n" + i + "       ");
					t.print(jtype);

				}
				// Extract the projected attributes
				int proj1_result = t.getIntFld(1); // 1 in example
				int proj2_result = t.getIntFld(2); // 1 in example
				// Save the tuple in the .txt file
				printer.print("[" + proj1_result + ","  +  proj2_result +  "]\n"); 
				i++; // Incrementing the counter
				
			}
			printer.close(); // Open resources
			
			// Print in console the number of tuple results
			System.out.println("\nOutput Tuples for query_1_a : " + i);
		
		} 
		
		// From the printer
		catch (FileNotFoundException e1) {
			
			status = FAIL;
			e1.printStackTrace();
			System.err.println("It's because of the PrintWriter...");
		
		}
		
		catch (Exception e) {
		
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		
		}
		
		long endTime = System.currentTimeMillis() - startTime;
		float durationTime = endTime / 1000; // In seconds
		UtilDriver.writeToCSV(query1a, max_num_tuples_Q, durationTime);
		System.out.println("Duration of the query 1a : " + durationTime + " s");
	    
	}
	
	
	/**
	* Run the query of task 1b. Nested Loop Join with 2 predicates
	* 
	* @param  verbose : boolean set to <code> true </code> if you want to print the resulting tuple s
	*/
	public void Query_1_b(boolean verbose) {
		
		long startTime = System.currentTimeMillis(); // To measure durations of the query

		query1b.promptQuery(); // Print the SQL query in the console
		System.out.print ("\n(Tests FileScan, Projection, and Nested Loop Join)\n");

	    boolean status = OK;

	    // Defining the two predicates
		CondExpr[] outFilter = new CondExpr[3];
	 	outFilter[0] = new CondExpr();
	 	outFilter[1] = new CondExpr();
	 	outFilter[2] = new CondExpr();
 
	 	DoublePredicate(outFilter, query1b);
	 	
	 		 	
	 	// ************* 		Rel_1 = R and Rel_2 = S in example			*************
	 	
	 	// The attributes types of the outer relation.
	 	// Every relations q, R and S has int types anyway.
	 	AttrType [] Rel_1_types = new AttrType[4];
	    Rel_1_types[0] = new AttrType (AttrType.attrInteger);
	    Rel_1_types[1] = new AttrType (AttrType.attrInteger);
	    Rel_1_types[2] = new AttrType (AttrType.attrInteger);
	    Rel_1_types[3] = new AttrType (AttrType.attrInteger);

		
	    // There are no strings anyway in q, R and S
	    short [] Rel_1_sizes = new short[0]; // We don't need this because we only deal with Integers.

	    
	    // Projections on the columns for the FileScan
	 	// Every relations q, R and S has 4 column anyway.
	    FldSpec [] Rel_1_projection = new FldSpec[4];
	    Rel_1_projection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
	    Rel_1_projection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
	    Rel_1_projection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
	    Rel_1_projection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

	    
	    // File scan the outer relation rel1 (R in the example)
	    FileScan am = null;
	    try {
	    
	    	am  = new FileScan( query1b.rel1 + ".in", Rel_1_types, Rel_1_sizes, 
					  (short)4, (short)4,
					  Rel_1_projection, null);
	    
	    }
	    
	    catch (Exception e) {
	    
	    	status = FAIL;
	    	System.err.println (""+e);
	    
	    }

	    if (status != OK) {
	      
	    	//bail out
	    	System.err.println ("*** Error setting up scan for the Relation 1 (outer)");
	    	Runtime.getRuntime().exit(1);
	      
	    }
	    
	 	// The attributes types of the inner relation.
	 	// Every relations q, R and S has int types anyway.
	    AttrType [] Rel_2_types = new AttrType[4];
	    Rel_2_types[0] = new AttrType (AttrType.attrInteger);
	    Rel_2_types[1] = new AttrType (AttrType.attrInteger);
	    Rel_2_types[2] = new AttrType (AttrType.attrInteger);
	    Rel_2_types[3] = new AttrType (AttrType.attrInteger);

	    // There are no strings anyway in q, R and S
	    short [] Rel_2_sizes = new short[1];
	    Rel_2_sizes[0] = 0;
	    
	    
	    // **** Projection settings
	    // 2 columns on which we project
	    FldSpec [] proj_list = new FldSpec[2];
	    // Outer is the first Relation
	    char proj1_attribute_char = query1b.proj1.charAt(2); // For example proj1 = R_1 so we extract the "1" here
	    int proj1_attribute_int = Integer.parseInt(String.valueOf(proj1_attribute_char)); // And change it from char "1" to int 1
	    proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), proj1_attribute_int); 
	    // Inner is the second Relation
	    char proj2_attribute_char = query1b.proj2.charAt(2); // For example proj2 = S_1 so we extract the "1" here
	    int proj2_attribute_int = Integer.parseInt(String.valueOf(proj2_attribute_char)); // And change it from char "1" to int 1
	    proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), proj2_attribute_int);

	    
	    // The attributes types.
	 	// Every relations q, R and S has int types anyway.
	    AttrType [] jtype = new AttrType[4];
	    jtype[0] = new AttrType (AttrType.attrInteger);
	    jtype[1] = new AttrType (AttrType.attrInteger);
	    jtype[2] = new AttrType (AttrType.attrInteger);
	    jtype[3] = new AttrType (AttrType.attrInteger);

	    
	    // Nested Loop Join
		NestedLoopsJoins nlj = null;
		try {
		
			nlj = new NestedLoopsJoins (Rel_1_types, 4, Rel_1_sizes,
					Rel_2_types, 4, Rel_2_sizes,
					10,
					am, query1b.rel2 + ".in",
					outFilter, null, proj_list, 2);
		
		}
		
		catch (Exception e) {
		
			System.err.println ("*** Error preparing for Nested Loop Join");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		
		}
		
		
		// OK, now we start reading the results, print them on the console, count them 
		// and load everything in a .txt file for analysis purpose
	 	Tuple t = new Tuple();
		t = null; // Initialize the current tuple
		int i = 0; // To count the tuples
		
		try {
		
			// Thank you Stackoverflow
			// To save the results in a .txt file
			PrintWriter printer = new PrintWriter("output_query_1_b.txt"); 
		
			// Iterate through the results
			while ((t = nlj.get_next()) != null) {
				if (verbose) {
					
					// Print the rank of the tuple and the tuple itself
					System.out.print("\n" + i + "       ");
					t.print(jtype); 

				}
				
				// Extract the projected attributes
				int proj1_result = t.getIntFld(1); // 1 in example
				int proj2_result = t.getIntFld(2); // 1 in example
				// Save the tuple in the .txt file
				printer.print("[" + proj1_result + ","  +  proj2_result +  "]\n"); 
				i++; // Incrementing the counter
			}
			printer.close(); // Open resources
			
			// Print in console the number of tuple results
			System.out.println("\nOutput Tuples for query_1_b : " + i);
		
		} 
		
		// From the printer
		catch (FileNotFoundException e1) {
			
			status = FAIL;
			e1.printStackTrace();
			System.err.println("It's because of the PrintWriter...");
		
		}
		
		catch (Exception e) {
		
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		
		}
		
		float endTime = System.currentTimeMillis() - startTime;
		float durationTime = endTime / 1000; // In seconds
		UtilDriver.writeToCSV(query1b, max_num_tuples_Q, durationTime);
		System.out.println("Duration of query 1b : " + durationTime + "s");
	    
	}

	
	/**
	* Run the query of task 2a. IE Self Join with 1 predicate + Correctness with Nested Loop Join
	* 
	* @param  correctness_NLJ : boolean set to <code> true </code> if you want to compare the resulting tuples with NLJ
	* @param  verbose : boolean set to <code> true </code> if you want to print the resulting tuples for both IE and NLJ (if chosen to correct the results)
	* @param  verbose : boolean set to <code> true </code> if you want to use IE Join (for example to measure duration of NLJ)
	*
	*/
	public void Query_2_a(boolean correctness_NLJ, boolean verbose, boolean with_IE) {

		long startTime = System.currentTimeMillis(); // To measure durations of the query

		query2a.promptQuery();
		System.out.print ("\n(Tests FileScan, Projection, and IE Self Join verified by NLJ)\n");

	    boolean status = OK;

	    // Defining the single predicate
		CondExpr[] outFilter = new CondExpr[2];
	 	outFilter[0] = new CondExpr();
	 	outFilter[1] = new CondExpr();
 
	 	SinglePredicate(outFilter, query2a);
	 	
	 		 	
	 	// ************* 		Rel = Q in example			*************
	 	
	    // The attributes types.
	 	// Every relations q, R and S has int types anyway.
	 	AttrType [] Rel_types = new AttrType[4];
	    Rel_types[0] = new AttrType (AttrType.attrInteger);
	    Rel_types[1] = new AttrType (AttrType.attrInteger);
	    Rel_types[2] = new AttrType (AttrType.attrInteger);
	    Rel_types[3] = new AttrType (AttrType.attrInteger);

		
	    // There are no strings anyway in q, R and S
	    short [] Rel_sizes = new short[0];

	    // Projections on the columns for the FileScan
	    // Every relations q, R and S has 4 columns anyway.
	    FldSpec [] Rel_projection = new FldSpec[4];
	    Rel_projection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
	    Rel_projection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
	    Rel_projection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
	    Rel_projection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

	    
	    // File scan the relation (Q in the example)
	    // We have to clone because for some reasons, using the same FileScan fails the program. 
	    // And I'm too much of a coward to debug into deep important class of Minibase
	    // It may be about something in Sort ?..
	    // I also tried to deep copy am (by implementing the clone() method from Cloneable in FileScan class)
	    // but it didn't work. Why ?
	    FileScan am = null;
	    FileScan amCloneIE = null;
	    FileScan amCloneNLJ = null;
	    
	    try {
	    
	    	am  = new FileScan( query2a.rel1 + ".in", Rel_types, Rel_sizes, 
					  (short)4, (short)4,
					  Rel_projection, null);
	    	// Cloning for the 2nd argument of IE
	    	amCloneIE  = new FileScan( query2a.rel1 + ".in", Rel_types, Rel_sizes, 
					  (short)4, (short)4,
					  Rel_projection, null);
	    	// Cloning for the NLJ (correctness)
	    	amCloneNLJ  = new FileScan( query2a.rel1 + ".in", Rel_types, Rel_sizes, 
					  (short)4, (short)4,
					  Rel_projection, null);

	    }
	    
	    catch (Exception e) {
	    
	    	status = FAIL;
	    	System.err.println (""+e);
	    
	    }
	    
	    if (status != OK) {
	      
	    	//bail out
	    	System.err.println ("*** Error setting up scan for Q and its clones");
	    	Runtime.getRuntime().exit(1);
	      
	    }
	    
	    
	    // **** Projection settings
	    // 2 columns on which we project
	    FldSpec [] proj_list = new FldSpec[2];
	    // First relation (Q in example)
	    char proj1_attribute_char = query2a.proj1.charAt(2); // For example proj1 = Q_1 so we extract the "1" here
	    int proj1_attribute_int = Integer.parseInt(String.valueOf(proj1_attribute_char)); // And change it from char "1" to int 1
	    proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), proj1_attribute_int); 
	    // Second relation (which is the same one in self join) (Q in example)
	    char proj2_attribute_char = query2a.proj2.charAt(2); // For example proj2 = Q_1 so we extract the "1" here
	    int proj2_attribute_int = Integer.parseInt(String.valueOf(proj2_attribute_char)); // And change it from char "1" to int 1
	    proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), proj2_attribute_int);

	    
	    // The attributes types.
	 	// Every relations q, R and S has int types anyway.
	    AttrType [] jtype = new AttrType[4];
	    jtype[0] = new AttrType (AttrType.attrInteger);
	    jtype[1] = new AttrType (AttrType.attrInteger);
	    jtype[2] = new AttrType (AttrType.attrInteger);
	    jtype[3] = new AttrType (AttrType.attrInteger);
	 
	    // IE Self Join + Nested Loop Join if correctness_NLJ = true
	    IESelfJoin ieSF = null;
		NestedLoopsJoins nlj = null;
		try {
			
			if (with_IE) {
				ieSF = new IESelfJoin(Rel_types, 4, new short[]{0}, amt_mem, am, amCloneIE, 
						outFilter, proj_list, 2, 1); // Last argument is 1 because there is 1 predicate
			}
			if (correctness_NLJ ) {
				nlj = new NestedLoopsJoins (Rel_types, 4, Rel_sizes,
						Rel_types, 4, Rel_sizes,
						10,
						amCloneNLJ, query2a.rel1 + ".in",
						outFilter, null, proj_list, 2);
			}
			
		}
		catch (Exception e) {
			System.err.println ("*** Error preparing for IE Self Join + Nested Loop Join if correctness_NLJ = true");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		
		
		// OK, now we start reading the results, print them on the console, count them 
		// and load everything in a .txt file for analysis purpose
	 	Tuple t = new Tuple();
		t = null; // Initialize the current tuple
		int i = 0; // To count the tuples IESelfJOIN
		int j = 0; // To count the tuples NLJ
		try {
			
			// If we want to run IE for this task
			if (with_IE) {
				
				System.out.println("\n\n Start IE Self Join \n\n");
				// Thank you Stackoverflow
				// To save the results in a .txt file
				PrintWriter printer = new PrintWriter("output_query_2_a.txt"); 
				
				// Iterate through the results of IE Self Join
				while ((t = ieSF.get_next()) != null) {
	
					if (verbose) {
						
						// Print the rank of the tuple and the tuple itself
						System.out.print("\n" + "IE Self Join : " + i + "       "); 
						t.print(jtype);
	
					}
					// Extract the projected attributes
					int proj1_result = t.getIntFld(1); // 1 in example
					int proj2_result = t.getIntFld(2); // 1 in example
					// Save the tuple in the .txt file
					printer.print("[" + proj1_result + ","  +  proj2_result +  "]\n"); 
					i++; // Incrementing the counter
				
				}
				
				printer.close(); // Open resources

			}	
			
			if (correctness_NLJ) {
				
				System.out.println("\n\n Start NLJ \n\n");
				
				t = null;
				while ((t = nlj.get_next()) != null) {
					
					if (verbose) {
						// Print the rank of the tuple and the tuple itself
						System.out.print("\n" + "NLJ : " + j + "       "); 
						t.print(jtype); 

					}
					j++; // Incrementing the counter
					
				}
				
			}
			
			
			
			// Print the total number of tuples results in the console and compare
			System.out.println("\nOutput Tuples for query_2_a IE Self Join : " + i);
			
			if (correctness_NLJ) {
				
				System.out.println("Output Tuples for query_2_a NLJ : " + j);
				
				if (i==j) {
					
					System.out.print("\n\n CONGRATULATIONS ! Same results than NLJ ! \n\n");
					
				}
				else {
					
					System.err.print("\n\n FAIL ! NOT the same results than NLJ ! \n\n");
					
				}
				
			}

		
		} 
		
		// From the printer
		catch (FileNotFoundException e1) {
			
			status = FAIL;
			e1.printStackTrace();
			System.err.println("It's because of the PrintWriter");
		
		}
		
		catch (Exception e) {
		
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		
		}
		
		float endTime = System.currentTimeMillis() - startTime;
		float durationTime = endTime / 1000; // In seconds
		UtilDriver.writeToCSV(query2a, max_num_tuples_Q, durationTime);
		System.out.println("Duration of the query : " + endTime + " ms");
		
	}
	
	
	/**
	* Run the query of task 2b. IE Self Join with 2 predicates + Correctness with Nested Loop Join
	*
	* @param  correctness_NLJ : boolean set to <code> true </code> if you want to compare the resulting tuples with NLJ
	* @param  verbose : boolean set to <code> true </code> if you want to print the resulting tuples for both IE and NLJ (if chosen to correct the results)
	* @param  verbose : boolean set to <code> true </code> if you want to use IE Join (for example to measure duration of NLJ)
	*
	*/
	public void Query_2_b(boolean correctness_NLJ, boolean verbose, boolean with_IE) {
		
		long startTime = System.currentTimeMillis(); // To measure durations of the query

		query2b.promptQuery();
		System.out.print ("\n(Tests FileScan, Projection, and IE Self Join verified by NLJ)\n");

	    boolean status = OK;

		CondExpr[] outFilter = new CondExpr[3];
	 	outFilter[0] = new CondExpr();
	 	outFilter[1] = new CondExpr();
	 	outFilter[2] = new CondExpr();

 
	 	DoublePredicate(outFilter, query2b);
	 	
	 	
	 	// ************* 		Rel = Q in example			*************
	 	
	    // The attributes types.
	 	// Every relations q, R and S has int types anyway.
	 	AttrType [] Rel_types = new AttrType[4];
	    Rel_types[0] = new AttrType (AttrType.attrInteger);
	    Rel_types[1] = new AttrType (AttrType.attrInteger);
	    Rel_types[2] = new AttrType (AttrType.attrInteger);
	    Rel_types[3] = new AttrType (AttrType.attrInteger);

		
	    // There are no strings anyway in q, R and S
	    short [] Rel_sizes = new short[0]; 

	    
	    // Projections on the columns for the FileScan
	    // Every relations q, R and S has 4 columns anyway.
	    FldSpec [] Rel_projection = new FldSpec[4];
	    Rel_projection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
	    Rel_projection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
	    Rel_projection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
	    Rel_projection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

	    
	    // File scan the relation (Q in the example)
	    // We have to clone because for some reasons, using the same FileScan fails the program. 
	    // And I'm too much of a coward to debug into deep important class of Minibase
	    // It may be about something in Sort ?..
	    // I also tried to deep copy am (by implementing the clone() method from Cloneable in FileScan class)
	    // but it didn't work. Why ?
	    FileScan am = null;
	    FileScan amCloneIE = null;
	    FileScan amCloneNLJ = null;
	    
	    try {
	    
	    	am  = new FileScan( query2b.rel1 + ".in", Rel_types, Rel_sizes, 
					  (short)4, (short)4,
					  Rel_projection, null);
		    amCloneIE = new FileScan( query2b.rel1 + ".in", Rel_types, Rel_sizes, 
					  (short)4, (short)4,
					  Rel_projection, null); // This clone is useful for the ieJoin because else, the results do not work
		    amCloneNLJ = new FileScan( query2b.rel1 + ".in", Rel_types, Rel_sizes, 
					  (short)4, (short)4,
					  Rel_projection, null);
	    
	    }
	    
	    catch (Exception e) {
	    
	    	status = FAIL;
	    	System.err.println (""+e);
	    
	    }
	    
	    	    
	    
	    if (status != OK) {
	      
	    	//bail out
	    	System.err.println ("*** Error setting up scan for Q and its clones");
	    	Runtime.getRuntime().exit(1);
	      
	    }
	    
	    
	    // **** Projection settings
	    // 2 columns on which we project
	    FldSpec [] proj_list = new FldSpec[2];
	    // First relation (Q in example)
	    char proj1_attribute_char = query2b.proj1.charAt(2); // For example proj1 = Q_1 so we extract the "1" here
	    int proj1_attribute_int = Integer.parseInt(String.valueOf(proj1_attribute_char)); // And change it from char "1" to int 1
	    proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), proj1_attribute_int); 
	    // Second relation (which is the same one in self join) (Q in example)
	    char proj2_attribute_char = query2b.proj2.charAt(2); // For example proj2 = Q_1 so we extract the "1" here
	    int proj2_attribute_int = Integer.parseInt(String.valueOf(proj2_attribute_char)); // And change it from char "1" to int 1
	    proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), proj2_attribute_int);

	    
	    // The attributes types.
	 	// Every relations q, R and S has int types anyway.
	    AttrType [] jtype = new AttrType[4];
	    jtype[0] = new AttrType (AttrType.attrInteger);
	    jtype[1] = new AttrType (AttrType.attrInteger);
	    jtype[2] = new AttrType (AttrType.attrInteger);
	    jtype[3] = new AttrType (AttrType.attrInteger);
	 
	    // IE Self Join + Nested Loop Join if correctness_NLJ = true
	    IESelfJoin ieSF = null;
		NestedLoopsJoins nlj = null;
		try {
			
			if (with_IE) {
				ieSF = new IESelfJoin(Rel_types, 4, new short[]{0}, 10, am, amCloneIE, 
						outFilter, proj_list, 2, 2); // Last argument is 1 because 1 predicate
			}
			if (correctness_NLJ ) {

				nlj = new NestedLoopsJoins (Rel_types, 4, Rel_sizes,
					Rel_types, 4, Rel_sizes,
					10,
					amCloneNLJ, query2b.rel1 + ".in",
					outFilter, null, proj_list, 2);
			
			}
			
		}
		catch (Exception e) {
			System.err.println ("*** Error preparing for IE Self Join + Nested Loop Join if correctness_NLJ = true");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		
		
		// OK, now we start reading the results, print them on the console, count them 
		// and load everything in a .txt file for analysis purpose
	 	Tuple t = new Tuple();
		t = null; // Initialize the current tuple
		int i = 0; // To count the tuples of IE
		int j = 0; // To count the tuples of NLJ
		
		try {
		
			// If we want to run IE for this task
			if (with_IE) {
				
				System.out.println("\n\n Start IE Self Join \n\n");
				// Thank you Stackoverflow
				// To save the results in a .txt file
				PrintWriter printer = new PrintWriter("output_query_2_b.txt");
			
				// Iterate through the results
				while ((t = ieSF.get_next()) != null) {
					
					if (verbose) {
						
						// Print the rank of the tuple and the tuple itself
						System.out.print("\n" + "IE Self Join : " + i + "       "); 
						t.print(jtype); 
	
					}
					// Extract the projected attributes
					int proj1_result = t.getIntFld(1); // 1 in example
					int proj2_result = t.getIntFld(2); // 1 in example
					// Save the tuple in the .txt file
					printer.print("[" + proj1_result + ","  +  proj2_result +  "]\n"); 
					i++; // Incrementing the counter
				}
							
				printer.close(); // Open resources
				
			}
			
			// If we want to run NLJ to correct for example
			if (correctness_NLJ) {
				
				System.out.println("\n\n Start NLJ \n\n");
				
				t = null;
				while ((t = nlj.get_next()) != null) {
					
					if (verbose) {
						
						// Print the rank of the tuple and the tuple itself
						System.out.print("\n" + "NLJ : " + j + "       "); 
						t.print(jtype); 
						
					}
					j++; // Incrementing the counter
					
				}
				
			}
			
			// Print the total number of tuples results in the console and compare
			System.out.println("\nOutput Tuples for query_2_b IE Self Join : " + i);
			
			if (correctness_NLJ) {
				
				System.out.println("Output Tuples for query_2_b NLJ : " + j);
				
				if (i==j) {
					
					System.out.print("\n\n CONGRATULATIONS ! Same results than NLJ ! \n\n");
					
				}
				else {
					
					System.err.print("\n\n FAIL ! NOT the same results than NLJ ! \n\n");
					
				}
				
			}

		
		} 
		
		// From the printer
		catch (FileNotFoundException e1) {
			
			status = FAIL;
			e1.printStackTrace();
			System.err.println("It's because of the PrintWriter...");
		
		}
		
		catch (Exception e) {
		
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		
		}
		
		float endTime = System.currentTimeMillis() - startTime;
		float durationTime = endTime / 1000; // In seconds
		UtilDriver.writeToCSV(query2b, max_num_tuples_Q, durationTime);
		System.out.println("Duration of the query 2b: " + durationTime + " ms");
		
	}
		
	
	/**
	* Run the query of task 2c. IE Join with 2 predicates + Correctness with Nested Loop Join 
	* 
	* @param  query : QueryString referring to either query 2c, 2c_1 or 2c_2
	* @param  correctness_NLJ : boolean set to <code> true </code> if you want to compare the resulting tuples with NLJ
	* @param  verbose : boolean set to <code> true </code> if you want to print the resulting tuples for both IE and NLJ (if chosen to correct the results)
	* @param  verbose : boolean set to <code> true </code> if you want to use IE Join (for example to measure duration of NLJ)
	* 
	*/
	public void Query_2_c(QueryString query, boolean correctness_NLJ, boolean verbose, boolean with_IE) {
		
		long startTime = System.currentTimeMillis(); // To measure durations of the query

		query.promptQuery();
		System.out.print ("\n(Tests FileScan, Projection, and IE Join verified by NLJ)\n");

	    boolean status = OK;

		CondExpr[] outFilter = new CondExpr[3];
	 	outFilter[0] = new CondExpr();
	 	outFilter[1] = new CondExpr();
	 	outFilter[2] = new CondExpr();

 
	 	DoublePredicate(outFilter, query);
	 	
	 		 	
	 	// ************* 		Rel = Q in example			*************
	 	
	    // The attributes types.
	 	// Every relations q, R and S has int types anyway.
	 	AttrType [] Rel_types = new AttrType[4];
	    Rel_types[0] = new AttrType (AttrType.attrInteger);
	    Rel_types[1] = new AttrType (AttrType.attrInteger);
	    Rel_types[2] = new AttrType (AttrType.attrInteger);
	    Rel_types[3] = new AttrType (AttrType.attrInteger);

		
	    // There are no strings anyway in q, R and S
	    short [] Rel_sizes = new short[1]; // We don't need this because we only deal with Integers.
	    Rel_sizes[0] = 0; // Need to do that instead of an empty table
	    				  // or we get an error when sorting and i don't want to modify the Sort class...
	    
	    // Projections on the columns for the FileScan
	    // Every relations q, R and S has 4 columns anyway.
	    FldSpec [] Rel_projection = new FldSpec[4];
	    Rel_projection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
	    Rel_projection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
	    Rel_projection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
	    Rel_projection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

	    
	    // File scan the relation (Q in the example)
	    // We have to clone because for some reasons, using the same FileScan fails the program. 
	    // And I'm too much of a coward to debug into deep important class of Minibase
	    // It may be about something in Sort ?..
	    // I also tried to deep copy am (by implementing the clone() method from Cloneable in FileScan class)
	    // but it didn't work. Why ?
	    FileScan outer = null; // For the 1st argument of outer for IEjoin
	    FileScan outerCloneIE = null; // For the 2nd argument of outer for IEjoin
	    FileScan inner = null; // For the 1st argument of inner for IEjoin
	    FileScan innerCloneIE = null; // For the 2nd argument of inner for IEjoin
	    FileScan am = null; // For the NLJ verification

	    // Here, everything is Q anyway
	    try {
	    
	    	outer  = new FileScan( query.rel1 + ".in", Rel_types, Rel_sizes, 
					  (short)4, (short)4,
					  Rel_projection, null);
		    outerCloneIE = new FileScan( query.rel1 + ".in", Rel_types, Rel_sizes, 
					  (short)4, (short)4,
					  Rel_projection, null); // This clone is useful for the ieJoin because else, the results do not work
		    
		    inner = new FileScan( query.rel1 + ".in", Rel_types, Rel_sizes, 
					  (short)4, (short)4,
					  Rel_projection, null);
		    innerCloneIE = new FileScan( query.rel1 + ".in", Rel_types, Rel_sizes, 
					  (short)4, (short)4,
					  Rel_projection, null);// This clone is useful for the ieJoin because else, the results do not work
	    	
		    am  = new FileScan( query.rel1 + ".in", Rel_types, Rel_sizes, 
					  (short)4, (short)4,
					  Rel_projection, null);
		    
	    }
	    
	    catch (Exception e) {
	    
	    	status = FAIL;
	    	System.err.println (""+e);
	    
	    }
	    
	    	    
	    
	    if (status != OK) {
	      
	    	//bail out
	    	System.err.println ("*** Error setting up scan for relation and its clones");
	    	Runtime.getRuntime().exit(1);
	      
	    }
	    
	    
	    // **** Projection settings
	    // 2 columns on which we project
	    FldSpec [] proj_list = new FldSpec[2];
	    // First relation (Q in example)
	    char proj1_attribute_char = query.proj1.charAt(2); // For example proj1 = Q_1 so we extract the "1" here
	    int proj1_attribute_int = Integer.parseInt(String.valueOf(proj1_attribute_char)); // And change it from char "1" to int 1
	    proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), proj1_attribute_int); 
	    // Second relation (which is the same one in self join) (Q in example)
	    char proj2_attribute_char = query.proj2.charAt(2); // For example proj2 = Q_1 so we extract the "1" here
	    int proj2_attribute_int = Integer.parseInt(String.valueOf(proj2_attribute_char)); // And change it from char "1" to int 1
	    proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), proj2_attribute_int);

	    
	    // The attributes types.
	 	// Every relations q, R and S has int types anyway.
	    AttrType [] jtype = new AttrType[4];
	    jtype[0] = new AttrType (AttrType.attrInteger);
	    jtype[1] = new AttrType (AttrType.attrInteger);
	    jtype[2] = new AttrType (AttrType.attrInteger);
	    jtype[3] = new AttrType (AttrType.attrInteger);
	 
	    // IE Self Join + Nested Loop Join if correctness_NLJ = true
	    IEJoin ieJoin = null;
		NestedLoopsJoins nlj = null;
		try {
			
			if (with_IE) {
				ieJoin = new IEJoin(Rel_types, 4, Rel_sizes, Rel_types, 4, Rel_sizes, 10, 
						outer, outerCloneIE, inner, innerCloneIE, outFilter, proj_list, 2);
			}
			if (correctness_NLJ ) {
				
				nlj = new NestedLoopsJoins (Rel_types, 4, Rel_sizes,
				Rel_types, 4, Rel_sizes,
				10,
				am, query2c.rel1 + ".in",
				outFilter, null, proj_list, 2);
				
			}
			
		}
		catch (Exception e) {
			
			System.err.println ("*** Error preparing for IE Join + Nested Loop Join if correctness_NLJ = true");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
			
		}
		
		
		// Ok, now we start reading the results, print them on the console, count them 
		// and load everything in a txt file for analysis purpose
	 	Tuple t = new Tuple();
		t = null; // Initialise the current tuple
		int i = 0; // To count the tuples of IE
		int j = 0; // To count the tuples of NLJ
		try {
		
			// If we want to run IE for this task
			if (with_IE) {
				
				System.out.println("\n\n Start IE Self Join \n\n");
				
				// Thank you Stackoverflow
				// To save the results in a .txt file
				PrintWriter printer = new PrintWriter("output_" + query.queryName + ".txt");
			
				// Iterate through the results
				System.out.println("\n\n");
				while ((t = ieJoin.get_next()) != null) {
					
					if (verbose) {
						
						// Print the rank of the tuple and the tuple itself
						System.out.print("\n" + "IE Join : " + i + "       ");
						t.print(jtype);
	
					}
					// Extract the projected attributes
					int proj1_result = t.getIntFld(1);
					int proj2_result = t.getIntFld(2);
					// Save the tuple in the .txt file
					printer.print("[" + proj1_result + ","  +  proj2_result +  "]\n");
					i++; // Incrementing the counter
					
				}
				
				printer.close(); // Open resources

			}
			
			// If we want to run NLJ for the query (for correction for example)
			if (correctness_NLJ) {
				
				System.out.println("\n\n Start NLJ \n\n");
				
				t = null;
				while ((t = nlj.get_next()) != null) {
					
					if (verbose) {
						
						// Print the rank of the tuple and the tuple itself
						System.out.print("\n" + "NLJ : " + j + "       ");
						t.print(jtype);

					}
					j++; // Incrementing the counter
					
				}
				
			}
			
			
			
			// Print the total number of tuples results in the console
			System.out.println("\nOutput Tuples for query " + query.queryName + " IE Join : " + i);
			
			if (correctness_NLJ) {
				
				System.out.println("Output Tuples for query " + query.queryName + " NLJ : " + j);
				if (i==j) {
					
					System.out.print("\n\n CONGRATULATIONS ! Same results than NLJ ! \n\n");
					
				}
				else {
					
					System.err.print("\n\n FAIL ! NOT the same results than NLJ ! \n\n");
					
				}
			}

		
		} 
		
		// From the printer
		catch (FileNotFoundException e1) {
			
			status = FAIL;
			e1.printStackTrace();
			System.err.println("It's because of the PrintWriter...");
		
		}
		
		catch (Exception e) {
		
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		
		}
		
		long endTime = System.currentTimeMillis() - startTime;
		float durationTime = endTime / 1000; // In seconds
		UtilDriver.writeToCSV(query, max_num_tuples_Q, durationTime);
		System.out.println("Duration of the query " + query.queryName + " : " + durationTime + " ms");
		
	}
		
		
	/**
	* To avoid legal prosecution.
	* 
	*/
	private void Disclaimer() {
	
		System.out.print ("\n\n Any resemblance of Integers in this database to"
	         + " Integers living or dead\nis purely coincidental. The contents of "
	         + "this database do not reflect\nthe views of neither"
	         + " Aydin or Fatemeh \n\n");
	
	}
	

}



/**
* Public class of the JoinTest. Manipulate the parameters in the main
* 
*/
public class MyJoinTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
	    boolean sortstatus;
	    
	    // *********** PARAMETERS TO CHOOSE FOR TESTS***********
	    // *********** TASK WORKING : 1a, 1b, 2a, 2b, 2c ************
	// path to the Directory containing both the queries and relations .txt files 
		final String DATA_DIR = "QueriesData/"; 
	// Number of pages in the Database. For sysdef instantiation. 1000000 is nice
		int num_pgs = 1000000;
	// Number of pages in the Buffer. For sysdef instantiation. 5000 is nice
		int num_buf = 5000;
	// Number of pages in memory when calling the Sort class. For IESelfJoin, IEJoin and NLJ instantiation. 100 is nice
		int amt_mem = 100; 
	// Don't get it too much... Heap storage can support until 30 000 but more than 2500 start to make joins very long. 500 is nice for fast debug for example
		int max_num_tuples_Q = 3000; 
	// List here the queries you want to test : "1a","1b","2a","2b" or "2c"
	    List<String> queries = Arrays.asList("2a"); 
	 // If you want to use NLJ for the task 2 queries. For example, to compare IE Self Join or IE Join with the NLJ for task 2a and 2b or 2c
	    boolean with_NLJ = false; 
	 // If you want to use IE for the task 2 queries
	    boolean with_IE = true;
	 // If you want to print the resulting tuples while the queries are running. Beware that it will dramatically increase the time for large datasets
	    boolean verbose = false;
	    
	    
	    
		//MyJoinsDriver jjoin = new MyJoinsDriver(num_pgs, num_buf, amt_mem, 
		//										max_num_tuples_Q, 
		//										DATA_DIR);

		//sortstatus = jjoin.runTests(queries, with_NLJ, verbose, with_IE); // true if the tests succeed
		
		//if (sortstatus != true) {
			
		//    System.out.println("Error ocurred during join tests");
		    
		//}
		//else {
		
		//	System.out.println("join tests completed successfully");
	
		//}
	    int begin_num_tuples = 3000;
	    int end_num_tuples = 4000;
	    int step_num_tuples = 1000;
	    UtilDriver.fill_csv(queries, begin_num_tuples, end_num_tuples, step_num_tuples, with_IE);;
	    System.out.println("End of writing into file(s)");
		
	}

}
