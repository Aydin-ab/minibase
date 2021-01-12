package tests;

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




class MyJoinsDriver implements GlobalConst {
	
	private boolean OK = true;
	private boolean FAIL = false; 
	
	// ****   We read and save the queries informations into many String variable ****
	// ****   so that it is more readable and intuitive when we read the code later on ****
	// ****   We use them to write the queries in the console for example.
	// ****   Or for the Query_*_CondExpr functions ****
	
	// QueryString to read and prompt the queries in the console
	QueryString query1a = new QueryString("query_1a.txt");
	QueryString query1b = new QueryString("query_1b.txt");
	QueryString query2a = new QueryString("query_2a.txt");
	QueryString query2b = new QueryString("query_2b.txt");
	QueryString query2c = new QueryString("query_2c.txt");
			
	
	
	// Constructor of the JoinsDriver where we put the data into heapfiles.
	public MyJoinsDriver() {
		
		
		String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb"; 
	    
	    /*
	    ExtendedSystemDefs extSysDef = 
	      new ExtendedSystemDefs( "/tmp/minibase.jointestdb", "/tmp/joinlog",
				      1000,500,200,"Clock");
	    */

	    SystemDefs sysdef = new SystemDefs( dbpath, 1000, NUMBUF, "Clock" );
	    
	    
		int max_num_tuples_Q = 400;	// Don't get it too much...
		int max_num_tuples_R = 200;	// Max capacity of R
		int max_num_tuples_S = 300; // Max capacity of S

		boolean q_heap_exist = UtilDriver.File2Heap("q.txt", "Q.in", max_num_tuples_Q);
		if (!q_heap_exist) {
			System.err.println("File2Heap did not succeed \n" +
					"q.in heap file failed");
		}
		
		boolean R_heap_exist = UtilDriver.File2Heap("R.txt", "R.in", max_num_tuples_R);
		if (!R_heap_exist) {
			System.err.println("File2Heap did not succeed \n" +
					"R.in heap file failed");
		}
		
		boolean S_heap_exist = UtilDriver.File2Heap("S.txt", "S.in", max_num_tuples_S);
		if (!S_heap_exist) {
			System.err.println("File2Heap did not succeed \n" +
					"S.in heap file failed");
		}
		

	}
	
	
	public boolean runTests() {
		    
		Disclaimer();
		//Query_1_a();
	    Query_1_b();
	    //Query_2_a();
		//Query_2_b();
	    //Query_2_c();
	    
	    System.out.print ("Finished joins testing"+"\n");
	   
	    
	    return true;
		    
	}
	  
	
	private void SinglePredicate(CondExpr[] expr, QueryString query) {
	
	    expr[0].next  = null;
	    expr[0].op    = new AttrOperator(query.pred1_op);
	    expr[0].type1 = new AttrType(AttrType.attrSymbol);
	    expr[0].type2 = new AttrType(AttrType.attrSymbol);
	    // Outer is the first Relation
	    char operand1_attribute_char = query.pred1_operand1.charAt(2); // For example operand1 = R_3 so we extract the "3" here
	    int operand1_attribute_int = Integer.parseInt(String.valueOf(operand1_attribute_char)); // And change it from char "3" to int 3
	    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer), operand1_attribute_int);
	    // Inner is the second Relation
	    char operand2_attribute_char = query.pred1_operand2.charAt(2); // For example operand2 = S_3 so we extract the "3" here
	    int operand2_attribute_int = Integer.parseInt(String.valueOf(operand2_attribute_char)); // And change it from char "3" to int 3
	    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel), operand2_attribute_int);
	 
	    expr[1] = null;
	    
	}
	
	private void DoublePredicate(CondExpr[] expr, QueryString query) {
		// We have 2 predicates so we construct expr = [pred1, pred2, null]
		// I don't get why there isn't this format in JoinTest.. I find it easier and intuitive
		
		// First predicate
	    expr[0].next  = null;
	    expr[0].op    = new AttrOperator(query.pred1_op);
	    expr[0].type1 = new AttrType(AttrType.attrSymbol);
	    expr[0].type2 = new AttrType(AttrType.attrSymbol);
	    // Outer is the first Relation
	    char pred1_operand1_attribute_char = query.pred1_operand1.charAt(2); // For example operand1 = R_3 so we extract the "3" here
	    int pred1_operand1_attribute_int = Integer.parseInt(String.valueOf(pred1_operand1_attribute_char)); // And change it from char "3" to int 3
	    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer), pred1_operand1_attribute_int);
	    // Inner is the second Relation
	    char pred1_operand2_attribute_char = query.pred1_operand2.charAt(2); // For example operand2 = S_3 so we extract the "3" here
	    int pred1_operand2_attribute_int = Integer.parseInt(String.valueOf(pred1_operand2_attribute_char)); // And change it from char "3" to int 3
	    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel), pred1_operand2_attribute_int);
	 

	    
	    expr[1].next  = null;
	    expr[1].op    = new AttrOperator(query.pred2_op);
	    expr[1].type1 = new AttrType(AttrType.attrSymbol);
	    expr[1].type2 = new AttrType(AttrType.attrSymbol);
	    // Outer is the first Relation
	    char pred2_operand1_attribute_char = query.pred2_operand1.charAt(2); // For example operand1 = R_3 so we extract the "3" here
	    int pred2_operand1_attribute_int = Integer.parseInt(String.valueOf(pred2_operand1_attribute_char)); // And change it from char "3" to int 3
	    expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer), pred2_operand1_attribute_int);
	    // Inner is the second Relation
	    char pred2_operand2_attribute_char = query.pred2_operand2.charAt(2); // For example operand2 = S_3 so we extract the "3" here
	    int pred2_operand2_attribute_int = Integer.parseInt(String.valueOf(pred2_operand2_attribute_char)); // And change it from char "3" to int 3
	    expr[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel), pred2_operand2_attribute_int);
	 
	    expr[2] = null;

	    
	}
	
	public void Query_1_a() {
		
	    query1a.promptQuery();
		System.out.print ("\n(Tests FileScan, Projection, and Nested Loop Join)\n");

	    boolean status = OK;
		long startTime = System.currentTimeMillis(); // For measuring durations

		CondExpr[] outFilter = new CondExpr[2];
	 	outFilter[0] = new CondExpr();
	 	outFilter[1] = new CondExpr();
 
	 	SinglePredicate(outFilter, query1a);
	 	
	 	
	 	Tuple t = new Tuple();
	 	
	 	// ************* 		Rel_1 = R and Rel_2 = S in example			*************
	 	
	 	// The attributes types of the outer relation, which are all Integer for q, R and S.
	    AttrType [] Rel_1_types = new AttrType[4];
	    Rel_1_types[0] = new AttrType (AttrType.attrInteger);
	    Rel_1_types[1] = new AttrType (AttrType.attrInteger);
	    Rel_1_types[2] = new AttrType (AttrType.attrInteger);
	    Rel_1_types[3] = new AttrType (AttrType.attrInteger);

		
	    //SOS
	    short [] Rel_1_sizes = new short[0]; // We don't need this because we only deal with Integers.
	   
	    // Every relations q, R and S has 4 columns so these lines stay the same for all the relations.
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
	    	System.err.println ("*** Error setting up scan for sailors");
	    	Runtime.getRuntime().exit(1);
	      
	    }
	    
	 	// The attributes types of the inner relation, which are all Integer for q, R and S.
	    AttrType [] Rel_2_types = new AttrType[4];
	    Rel_2_types[0] = new AttrType (AttrType.attrInteger);
	    Rel_2_types[1] = new AttrType (AttrType.attrInteger);
	    Rel_2_types[2] = new AttrType (AttrType.attrInteger);
	    Rel_2_types[3] = new AttrType (AttrType.attrInteger);

	    //SOS
	    short [] Rel_2_sizes = new short[0]; // We don't need this because we only deal with Integers.
	   
	    
	    // The columns on which we project
	    FldSpec [] proj_list = new FldSpec[2];
	    // Outer is the first relation
	    char proj1_attribute_char = query1a.proj1.charAt(2); // For example proj1 = R_1 so we extract the "1" here
	    int proj1_attribute_int = Integer.parseInt(String.valueOf(proj1_attribute_char)); // And change it from char "1" to int 1
	    proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), proj1_attribute_int); 
	    // Outer is the second relation
	    char proj2_attribute_char = query1a.proj1.charAt(2); // For example proj1 = R_1 so we extract the "1" here
	    int proj2_attribute_int = Integer.parseInt(String.valueOf(proj2_attribute_char)); // And change it from char "1" to int 1
	    proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), proj2_attribute_int);

	    
	    // The attributes types of the final join table (which will always be int for q,R and S anyway)
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
		
			System.err.println ("*** Error preparing for nested_loop_join");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		
		}
		
		
		// Ok, now we start reading the results, print them on the console, count them 
		// and load everything in a txt file for analysis purpose
		t = null; // Initialise the current tuple
		int i = 0; // To count the tuples
		
		try {
		
			PrintWriter printer = new PrintWriter("output_query_1_a.txt"); // To save the results in a txt file
		
			// Iterate through the results
			while ((t = nlj.get_next()) != null) {
				t.print(jtype); // Print the tuple to the console
				int proj1_result = t.getIntFld(1); // Extract the first projected attribute
				int proj2_result = t.getIntFld(2); // Extract the second projected attribute
				printer.print("[" + proj1_result + ","  +  proj2_result +  "]\n"); // Save the tuple in the .txt file
				i++; // Incrementing the counter
			}
			printer.close(); // Open ressources
			
			// Print the total number of tuples results in the console
			System.out.println("Output Tuples for query_1_a : " + i);
		
		} 
		
		// From the printer
		catch (FileNotFoundException e1) {
			
			status = FAIL;
			e1.printStackTrace();
			System.err.println("It's because of the PrintWriter AGAIN OMFGGGGGRZAFAZF...");
		
		}
		
		catch (Exception e) {
		
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		
		}
		
		long elapsedTime = System.currentTimeMillis() - startTime;
		System.out.println("Duration of the query : " + elapsedTime + " ms");
	    
	}
	
	
	public void Query_1_b() {
		
		query1b.promptQuery();
		System.out.print ("\n(Tests FileScan, Projection, and Nested Loop Join)\n");

	    boolean status = OK;
		long startTime = System.currentTimeMillis(); // For measuring durations

		CondExpr[] outFilter = new CondExpr[3];
	 	outFilter[0] = new CondExpr();
	 	outFilter[1] = new CondExpr();
	 	outFilter[2] = new CondExpr();
 
	 	DoublePredicate(outFilter, query1b);
	 	
	 	
	 	Tuple t = new Tuple();
	 	
	 	// ************* 		Rel_1 = R and Rel_2 = S in example			*************
	 	
	 	// The attributes types of the outer relation, which are all Integer for q, R and S.
	    AttrType [] Rel_1_types = new AttrType[4];
	    Rel_1_types[0] = new AttrType (AttrType.attrInteger);
	    Rel_1_types[1] = new AttrType (AttrType.attrInteger);
	    Rel_1_types[2] = new AttrType (AttrType.attrInteger);
	    Rel_1_types[3] = new AttrType (AttrType.attrInteger);

		
	    //SOS
	    short [] Rel_1_sizes = new short[0]; // We don't need this because we only deal with Integers.

	    
	    // Every relations q, R and S has 4 columns so these lines stay the same for all the relations.
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
	    	System.err.println ("*** Error setting up scan for sailors");
	    	Runtime.getRuntime().exit(1);
	      
	    }
	    
	 	// The attributes types of the inner relation, which are all Integer for q, R and S.
	    AttrType [] Rel_2_types = new AttrType[4];
	    Rel_2_types[0] = new AttrType (AttrType.attrInteger);
	    Rel_2_types[1] = new AttrType (AttrType.attrInteger);
	    Rel_2_types[2] = new AttrType (AttrType.attrInteger);
	    Rel_2_types[3] = new AttrType (AttrType.attrInteger);

	    //SOS
	    short [] Rel_2_sizes = new short[1]; // We don't need this because we only deal with Integers.
	    Rel_2_sizes[0] = 0;
	    
	    // The columns on which we project
	    FldSpec [] proj_list = new FldSpec[2];
	    // Outer is the first relation
	    char proj1_attribute_char = query1b.proj1.charAt(2); // For example proj1 = R_1 so we extract the "1" here
	    int proj1_attribute_int = Integer.parseInt(String.valueOf(proj1_attribute_char)); // And change it from char "1" to int 1
	    proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), proj1_attribute_int); 
	    // Outer is the second relation
	    char proj2_attribute_char = query1b.proj2.charAt(2); // For example proj2 = S_1 so we extract the "1" here
	    int proj2_attribute_int = Integer.parseInt(String.valueOf(proj2_attribute_char)); // And change it from char "1" to int 1
	    proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), proj2_attribute_int);

	    
	    // The attributes types of the final join table (which will always be int for q,R and S anyway)
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
		
			System.err.println ("*** Error preparing for nested_loop_join");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		
		}
		
		
		// Ok, now we start reading the results, print them on the console, count them 
		// and load everything in a txt file for analysis purpose
		t = null; // Initialise the current tuple
		int i = 0; // To count the tuples
		
		try {
		
			PrintWriter printer = new PrintWriter("output_query_1_b.txt"); // To save the results in a txt file
		
			// Iterate through the results
			while ((t = nlj.get_next()) != null) {
				t.print(jtype); // Print the tuple to the console
				int proj1_result = t.getIntFld(1); // Extract the first projected attribute
				int proj2_result = t.getIntFld(2); // Extract the second projected attribute
				printer.print("[" + proj1_result + ","  +  proj2_result +  "]\n"); // Save the tuple in the .txt file
				i++; // Incrementing the counter
			}
			printer.close(); // Open ressources
			
			// Print the total number of tuples results in the console
			System.out.println("Output Tuples for query_1_b : " + i);
		
		} 
		
		// From the printer
		catch (FileNotFoundException e1) {
			
			status = FAIL;
			e1.printStackTrace();
			System.err.println("It's because of the PrintWriter AGAIN OMFGGGGGRZAFAZF...");
		
		}
		
		catch (Exception e) {
		
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		
		}
		
		long elapsedTime = System.currentTimeMillis() - startTime;
		System.out.println("Duration of query 1b : " + elapsedTime);
	    
	}

	public void Query_2_a() {
		
		query2a.promptQuery();
		System.out.print ("\n(Tests FileScan, Projection, and IE Self Join verified by NLJ)\n");

	    boolean status = OK;
		long startTime = System.currentTimeMillis(); // For measuring durations

		CondExpr[] outFilter = new CondExpr[2];
	 	outFilter[0] = new CondExpr();
	 	outFilter[1] = new CondExpr();
 
	 	SinglePredicate(outFilter, query2a);
	 	
	 	
	 	Tuple t = new Tuple();
	 	
	 	// ************* 		Rel = Q in example			*************
	 	
	 	// The attributes types of the outer relation, which are all Integer for q, R and S.
	    AttrType [] Rel_types = new AttrType[4];
	    Rel_types[0] = new AttrType (AttrType.attrInteger);
	    Rel_types[1] = new AttrType (AttrType.attrInteger);
	    Rel_types[2] = new AttrType (AttrType.attrInteger);
	    Rel_types[3] = new AttrType (AttrType.attrInteger);

		
	    //SOS
	    short [] Rel_sizes = new short[1]; // We don't need this because we only deal with Integers.
	    Rel_sizes[0] = 0; // Need to do that instead of an empty table
	    				  // or we get an error when sorting and i don't want to modify the Sort class...
	    
	    // Every relations q, R and S has 4 columns so these lines stay the same for all the relations.
	    FldSpec [] Rel_projection = new FldSpec[4];
	    Rel_projection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
	    Rel_projection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
	    Rel_projection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
	    Rel_projection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

	    
	    // File scan the outer relation rel1 (R in the example)
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
	    	// Cloning for the NLJ Comparison
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
	    	System.err.println ("*** Error setting up scan for Q");
	    	Runtime.getRuntime().exit(1);
	      
	    }
	    
	    
	    // The columns on which we project
	    FldSpec [] proj_list = new FldSpec[2];
	    // Outer is the first relation
	    char proj1_attribute_char = query2a.proj1.charAt(2); // For example proj1 = R_1 so we extract the "1" here
	    int proj1_attribute_int = Integer.parseInt(String.valueOf(proj1_attribute_char)); // And change it from char "1" to int 1
	    proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), proj1_attribute_int); 
	    // Outer is the second relation
	    char proj2_attribute_char = query2a.proj1.charAt(2); // For example proj1 = R_1 so we extract the "1" here
	    int proj2_attribute_int = Integer.parseInt(String.valueOf(proj2_attribute_char)); // And change it from char "1" to int 1
	    proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), proj2_attribute_int);

	    
	    // The attributes types of the final join table (which will always be int for q,R and S anyway)
 	    AttrType [] jtype = new AttrType[4];
	    jtype[0] = new AttrType (AttrType.attrInteger);
	    jtype[1] = new AttrType (AttrType.attrInteger);
	    jtype[2] = new AttrType (AttrType.attrInteger);
	    jtype[3] = new AttrType (AttrType.attrInteger);
	 
	    // IE Self Join and Nested Loop Join
	    IESelfJoin ieSF = null;
		NestedLoopsJoins nlj = null;
		try {
			
			ieSF = new IESelfJoin(Rel_types, 4, Rel_sizes, 10, am, amCloneIE, 
					outFilter, null, proj_list, 2, 1); // Last argument is 1 bcs 1 predicate
			nlj = new NestedLoopsJoins (Rel_types, 4, Rel_sizes,
					Rel_types, 4, Rel_sizes,
					10,
					amCloneNLJ, query2a.rel1 + ".in",
					outFilter, null, proj_list, 2);
			
		}
		catch (Exception e) {
			System.err.println ("*** Error preparing for IE Self Join");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		
		
		// Ok, now we start reading the results, print them on the console, count them 
		// and load everything in a txt file for analysis purpose
		t = null; // Initialise the current tuple
		int i = 0; // To count the tuples IESelfJOIN
		int j = 0; // To count the tuples NLJ
		try {
		
			PrintWriter printer = new PrintWriter("output_query_2_a.txt"); // To save the results in a txt file
		
			// Iterate through the results
			while ((t = ieSF.get_next()) != null) {
				t.print(jtype); // Print the tuple to the console
				int proj1_result = t.getIntFld(1); // Extract the first projected attribute
				int proj2_result = t.getIntFld(2); // Extract the second projected attribute
				printer.print("[" + proj1_result + ","  +  proj2_result +  "]\n"); // Save the tuple in the .txt file
				i++; // Incrementing the counter
			}
			System.out.println("\n\n Start NLJ \n\n");
			t = null;
			while ((t = nlj.get_next()) != null) {
				t.print(jtype); // Print the tuple to the console
				int proj1_result = t.getIntFld(1); // Extract the first projected attribute
				int proj2_result = t.getIntFld(2); // Extract the second projected attribute
				printer.print("[" + proj1_result + ","  +  proj2_result +  "]\n"); // Save the tuple in the .txt file
				j++; // Incrementing the counter
			}
			printer.close(); // Open ressources
			
			// Print the total number of tuples results in the console
			System.out.println("Output Tuples for query_2_a IESelfJoin : " + i);
			System.out.println("Output Tuples for query_2_a NLJ : " + j);

		
		} 
		
		// From the printer
		catch (FileNotFoundException e1) {
			
			status = FAIL;
			e1.printStackTrace();
			System.err.println("It's because of the PrintWriter AGAIN OMFGGGGGRZAFAZF...");
		
		}
		
		catch (Exception e) {
		
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		
		}
		
		long elapsedTime = System.currentTimeMillis() - startTime;
		System.out.println("Duration of the query : " + elapsedTime + " ms");
		
	}
	
	
	public void Query_2_b() {
		
		query2b.promptQuery();
		System.out.print ("\n(Tests FileScan, Projection, and IE Self Join verified by NLJ)\n");

	    boolean status = OK;
		long startTime = System.currentTimeMillis(); // For measuring durations

		CondExpr[] outFilter = new CondExpr[3];
	 	outFilter[0] = new CondExpr();
	 	outFilter[1] = new CondExpr();
	 	outFilter[2] = new CondExpr();

 
	 	DoublePredicate(outFilter, query2b);
	 	
	 	
	 	Tuple t = new Tuple();
	 	
	 	// ************* 		Rel = Q in example			*************
	 	
	 	// The attributes types of the outer relation, which are all Integer for q, R and S.
	    AttrType [] Rel_types = new AttrType[4];
	    Rel_types[0] = new AttrType (AttrType.attrInteger);
	    Rel_types[1] = new AttrType (AttrType.attrInteger);
	    Rel_types[2] = new AttrType (AttrType.attrInteger);
	    Rel_types[3] = new AttrType (AttrType.attrInteger);

		
	    //SOS
	    short [] Rel_sizes = new short[1]; // We don't need this because we only deal with Integers.
	    Rel_sizes[0] = 0; // Need to do that instead of an empty table
	    				  // or we get an error when sorting and i don't want to modify the Sort class...
	    
	    // Every relations q, R and S has 4 columns so these lines stay the same for all the relations.
	    FldSpec [] Rel_projection = new FldSpec[4];
	    Rel_projection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
	    Rel_projection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
	    Rel_projection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
	    Rel_projection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

	    
	    // File scan the outer relation rel1 (R in the example)
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
	    	System.err.println ("*** Error setting up scan for Q");
	    	Runtime.getRuntime().exit(1);
	      
	    }
	    
	    
	    // The columns on which we project
	    FldSpec [] proj_list = new FldSpec[2];
	    // Outer is the first relation
	    char proj1_attribute_char = query2a.proj1.charAt(2); // For example proj1 = R_1 so we extract the "1" here
	    int proj1_attribute_int = Integer.parseInt(String.valueOf(proj1_attribute_char)); // And change it from char "1" to int 1
	    proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), proj1_attribute_int); 
	    // Outer is the second relation
	    char proj2_attribute_char = query2a.proj1.charAt(2); // For example proj1 = R_1 so we extract the "1" here
	    int proj2_attribute_int = Integer.parseInt(String.valueOf(proj2_attribute_char)); // And change it from char "1" to int 1
	    proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), proj2_attribute_int);

	    
	    // The attributes types of the final join table (which will always be int for q,R and S anyway)
 	    AttrType [] jtype = new AttrType[4];
	    jtype[0] = new AttrType (AttrType.attrInteger);
	    jtype[1] = new AttrType (AttrType.attrInteger);
	    jtype[2] = new AttrType (AttrType.attrInteger);
	    jtype[3] = new AttrType (AttrType.attrInteger);
	 
	    // IE Self Join and Nested Loop Join
	    IESelfJoin ieSF = null;
		NestedLoopsJoins nlj = null;
		try {
			
			ieSF = new IESelfJoin(Rel_types, 4, Rel_sizes, 10, am, amCloneIE, 
					outFilter, null, proj_list, 2, 2); // Last argument is 1 bcs 1 predicate
			nlj = new NestedLoopsJoins (Rel_types, 4, Rel_sizes,
					Rel_types, 4, Rel_sizes,
					10,
					amCloneNLJ, query2a.rel1 + ".in",
					outFilter, null, proj_list, 2);
			
		}
		catch (Exception e) {
			System.err.println ("*** Error preparing for IE Self Join");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		
		
		// Ok, now we start reading the results, print them on the console, count them 
		// and load everything in a txt file for analysis purpose
		t = null; // Initialise the current tuple
		int i = 0; // To count the tuples of IE
		int j = 0; // To count the tuples of NLJ
		try {
		
			PrintWriter printer = new PrintWriter("output_query_2_b.txt"); // To save the results in a txt file
		
			// Iterate through the results
			while ((t = ieSF.get_next()) != null) {
				t.print(jtype); // Print the tuple to the console
				int proj1_result = t.getIntFld(1); // Extract the first projected attribute
				int proj2_result = t.getIntFld(2); // Extract the second projected attribute
				printer.print("[" + proj1_result + ","  +  proj2_result +  "]\n"); // Save the tuple in the .txt file
				i++; // Incrementing the counter
			}
			
			System.out.println("\n\n Start NLJ \n\n");
			
			t = null;
			while ((t = nlj.get_next()) != null) {
				t.print(jtype); // Print the tuple to the console
				int proj1_result = t.getIntFld(1); // Extract the first projected attribute
				int proj2_result = t.getIntFld(2); // Extract the second projected attribute
				printer.print("[" + proj1_result + ","  +  proj2_result +  "]\n"); // Save the tuple in the .txt file
				j++; // Incrementing the counter
			}
			
			printer.close(); // Open ressources
			
			// Print the total number of tuples results in the console
			System.out.println("Output Tuples for query_2_b IESelfJoin : " + i);
			System.out.println("Output Tuples for query_2_b NLJ : " + j);

		
		} 
		
		// From the printer
		catch (FileNotFoundException e1) {
			
			status = FAIL;
			e1.printStackTrace();
			System.err.println("It's because of the PrintWriter AGAIN OMFGGGGGRZAFAZF...");
		
		}
		
		catch (Exception e) {
		
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		
		}
		
		long elapsedTime = System.currentTimeMillis() - startTime;
		System.out.println("Duration of the query : " + elapsedTime + " ms");
		
	}
		
	public void Query_2_c() {
		
		query2c.promptQuery();
		System.out.print ("\n(Tests FileScan, Projection, and IE Join verified by NLJ)\n");

	    boolean status = OK;
		long startTime = System.currentTimeMillis(); // For measuring durations

		CondExpr[] outFilter = new CondExpr[3];
	 	outFilter[0] = new CondExpr();
	 	outFilter[1] = new CondExpr();
	 	outFilter[2] = new CondExpr();

 
	 	DoublePredicate(outFilter, query2b);
	 	
	 	
	 	Tuple t = new Tuple();
	 	
	 	// ************* 		Rel = Q in example			*************
	 	
	 	// The attributes types of the outer relation, which are all Integer for q, R and S.
	    AttrType [] Rel_types = new AttrType[4];
	    Rel_types[0] = new AttrType (AttrType.attrInteger);
	    Rel_types[1] = new AttrType (AttrType.attrInteger);
	    Rel_types[2] = new AttrType (AttrType.attrInteger);
	    Rel_types[3] = new AttrType (AttrType.attrInteger);

		
	    //SOS
	    short [] Rel_sizes = new short[1]; // We don't need this because we only deal with Integers.
	    Rel_sizes[0] = 0; // Need to do that instead of an empty table
	    				  // or we get an error when sorting and i don't want to modify the Sort class...
	    
	    // Every relations q, R and S has 4 columns so these lines stay the same for all the relations.
	    FldSpec [] Rel_projection = new FldSpec[4];
	    Rel_projection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
	    Rel_projection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
	    Rel_projection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
	    Rel_projection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

	    
	    // File scan the outer relation rel1 (R in the example)
	    FileScan outer = null; // For the 1st argument of outer for IEjoin
	    FileScan outerCloneIE = null; // For the 2nd argument of outer for IEjoin
	    FileScan inner = null; // For the 1st argument of inner for IEjoin
	    FileScan innerCloneIE = null; // For the 2nd argument of inner for IEjoin
	    FileScan am = null; // For the NLJ verification

	    // Here, everything is Q anyway
	    try {
	    
	    	outer  = new FileScan( query2c.rel1 + ".in", Rel_types, Rel_sizes, 
					  (short)4, (short)4,
					  Rel_projection, null);
		    outerCloneIE = new FileScan( query2c.rel1 + ".in", Rel_types, Rel_sizes, 
					  (short)4, (short)4,
					  Rel_projection, null); // This clone is useful for the ieJoin because else, the results do not work
		    
		    inner = new FileScan( query2c.rel1 + ".in", Rel_types, Rel_sizes, 
					  (short)4, (short)4,
					  Rel_projection, null);
		    innerCloneIE = new FileScan( query2c.rel1 + ".in", Rel_types, Rel_sizes, 
					  (short)4, (short)4,
					  Rel_projection, null);// This clone is useful for the ieJoin because else, the results do not work
	    	
		    am  = new FileScan( query2c.rel1 + ".in", Rel_types, Rel_sizes, 
					  (short)4, (short)4,
					  Rel_projection, null);
		    
	    }
	    
	    catch (Exception e) {
	    
	    	status = FAIL;
	    	System.err.println (""+e);
	    
	    }
	    
	    	    
	    
	    if (status != OK) {
	      
	    	//bail out
	    	System.err.println ("*** Error setting up scan for Q");
	    	Runtime.getRuntime().exit(1);
	      
	    }
	    
	    
	    // The columns on which we project
	    FldSpec [] proj_list = new FldSpec[2];
	    // Outer is the first relation
	    char proj1_attribute_char = query2a.proj1.charAt(2); // For example proj1 = R_1 so we extract the "1" here
	    int proj1_attribute_int = Integer.parseInt(String.valueOf(proj1_attribute_char)); // And change it from char "1" to int 1
	    proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), proj1_attribute_int); 
	    // Outer is the second relation
	    char proj2_attribute_char = query2a.proj2.charAt(2); // For example proj1 = R_1 so we extract the "1" here
	    int proj2_attribute_int = Integer.parseInt(String.valueOf(proj2_attribute_char)); // And change it from char "1" to int 1
	    proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), proj2_attribute_int);

	    
	    // The attributes types of the final join table (which will always be int for q,R and S anyway)
 	    AttrType [] jtype = new AttrType[4];
	    jtype[0] = new AttrType (AttrType.attrInteger);
	    jtype[1] = new AttrType (AttrType.attrInteger);
	    jtype[2] = new AttrType (AttrType.attrInteger);
	    jtype[3] = new AttrType (AttrType.attrInteger);
	 
	    // IE Self Join and Nested Loop Join
	    IEjoin ieJoin = null;
		NestedLoopsJoins nlj = null;
		try {
			
			ieJoin = new IEjoin(Rel_types, 4, Rel_sizes, Rel_types, 4, Rel_sizes, 10, 
					outer, outerCloneIE, inner, innerCloneIE, outFilter, 
					null, proj_list, 2);
			nlj = new NestedLoopsJoins (Rel_types, 4, Rel_sizes,
					Rel_types, 4, Rel_sizes,
					10,
					am, query2a.rel1 + ".in",
					outFilter, null, proj_list, 2);
			
		}
		catch (Exception e) {
			System.err.println ("*** Error preparing for IE Join");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		
		
		// Ok, now we start reading the results, print them on the console, count them 
		// and load everything in a txt file for analysis purpose
		t = null; // Initialise the current tuple
		int i = 0; // To count the tuples of IE
		int j = 0; // To count the tuples of NLJ
		try {
		
			PrintWriter printer = new PrintWriter("output_query_2_c.txt"); // To save the results in a txt file
		
			// Iterate through the results
			System.out.println("\n\n");
			while ((t = ieJoin.get_next()) != null) {
				t.print(jtype); // Print the tuple to the console
				int proj1_result = t.getIntFld(1); // Extract the first projected attribute
				int proj2_result = t.getIntFld(2); // Extract the second projected attribute
				printer.print("[" + proj1_result + ","  +  proj2_result +  "]\n"); // Save the tuple in the .txt file
				i++; // Incrementing the counter
			}
			
			System.out.println("\n\n Start NLJ \n\n");
			
			t = null;
			while ((t = nlj.get_next()) != null) {
				
				t.print(jtype); // Print the tuple to the console
				int proj1_result = t.getIntFld(1); // Extract the first projected attribute
				int proj2_result = t.getIntFld(2); // Extract the second projected attribute
				printer.print("[" + proj1_result + ","  +  proj2_result +  "]\n"); // Save the tuple in the .txt file
				j++; // Incrementing the counter
				
			}
			
			printer.close(); // Open ressources
			
			// Print the total number of tuples results in the console
			System.out.println("Output Tuples for query_2_c IE Join : " + i);
			System.out.println("Output Tuples for query_2_c NLJ : " + j);

		
		} 
		
		// From the printer
		catch (FileNotFoundException e1) {
			
			status = FAIL;
			e1.printStackTrace();
			System.err.println("It's because of the PrintWriter AGAIN OMFGGGGGRZAFAZF...");
		
		}
		
		catch (Exception e) {
		
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		
		}
		
		long elapsedTime = System.currentTimeMillis() - startTime;
		System.out.println("Duration of the query : " + elapsedTime + " ms");
		
	}
		
		
	

	private void Disclaimer() {
	
		System.out.print ("\n\n Any resemblance of Integers in this database to"
	         + " Integers living or dead\nis purely coincidental. The contents of "
	         + "this database do not reflect\nthe views of neither"
	         + " Aydin or Fatemeh \n\n");
	
	}
	

  

}




public class MyJoinTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
	    boolean sortstatus;

		MyJoinsDriver jjoin = new MyJoinsDriver();
		
		sortstatus = jjoin.runTests();
		
		if (sortstatus != true) {
			
		    System.out.println("Error ocurred during join tests");
		    
		}
		else {
		
			System.out.println("join tests completed successfully");
	
		}
		
	}

}
