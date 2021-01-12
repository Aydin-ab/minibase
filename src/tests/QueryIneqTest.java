package tests;
//originally from : joins.C

import iterator.*;
import iterator.Iterator;
import heap.*;
import global.*;
import index.*;
import java.io.*;
import java.util.*;
import java.lang.*;
import java.nio.file.Paths;
import tests.*;

import diskmgr.*;
import bufmgr.*;
import btree.*;
import catalog.*;

class JoinsDriverAssign implements GlobalConst {

	private boolean OK = true;
	private boolean FAIL = false;
	private String query_data_path, r_path, s_path, q_path, query_file_path, query_file;
	int max_iters;

	/**
	 * Constructor : reads the different tables and creates Heapfile for them 
	 * @throws HFBufMgrException 
	 * @throws HFDiskMgrException 
	 * @throws InvalidTupleSizeException 
	 * @throws InvalidSlotNumberException 
	 * @throws IOException 
	 */
	public JoinsDriverAssign(String r_file, String s_file, String q_file, String query_data_dir, int max_iters)
			throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException, HFBufMgrException, IOException 
	{

		// build tables
		boolean status = OK;
		int numS = 300;
		int numR = 200;
		int numQ = 2000000;
		int num_attrs = 4;
		this.max_iters = max_iters;
		this.query_data_path = System.getProperty("user.dir")+ "/" + query_data_dir;
		this.r_path = query_data_path + "/" + r_file + ".txt";
		this.s_path = query_data_path + "/" + s_file + ".txt";
		this.q_path = query_data_path + "/" + q_file + ".txt";
		this.query_file_path = query_data_path + "/" + query_file + ".txt";

		String dbpath = "/tmp/" + System.getProperty("user.name") + ".minibase.jointestdb";
		String logpath = "/tmp/" + System.getProperty("user.name") + ".joinlog";

		String remove_cmd = "/bin/rm -rf ";
		String remove_logcmd = remove_cmd + logpath;
		String remove_dbcmd = remove_cmd + dbpath;
		String remove_joincmd = remove_cmd + dbpath;

		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
			Runtime.getRuntime().exec(remove_joincmd);
		} catch (IOException e) {
			System.err.println("" + e);
		}


		// ExtendedSystemDefs extSysDef = new ExtendedSystemDefs("/tmp/minibase.jointestdb", "/tmp/joinlog", 1000,500,200,"Clock");

		SystemDefs sysdef = new SystemDefs(dbpath, 31258, NUMBUF, "Clock");

		// ************************************************************************//
		// 							Creating S Database 						   //
		// ************************************************************************//
		// Attributes for the 3 databases
		AttrType[] Stypes = new AttrType[num_attrs];
		for (int i=0; i<num_attrs; i++) {
			Stypes[i] = new AttrType(AttrType.attrInteger);
		}

		// SOS
		short[] Ssizes = new short[1];
		Ssizes[0] = 0;

		// Setting header for DB (same for the 3 databases: S, R and Q)
		Tuple t = new Tuple();
		try {
			t.setHdr((short) num_attrs, Stypes, Ssizes);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			status = FAIL;
			e.printStackTrace();
		}

		int size = t.size();

		// inserting the tuple into file "S"
		File sDB = new File(s_path);
		BufferedReader sDB_br;
		sDB_br = new BufferedReader(new FileReader(sDB));
		String sDB_lines;
		sDB_lines = sDB_br.readLine();
		RID srid;
		Heapfile sf = null;
		try {
			sf = new Heapfile(s_file + ".in");
		} catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			status = FAIL;
			e.printStackTrace();
		}

		t = new Tuple(size);
		try {
			t.setHdr((short) 4, Stypes, Ssizes);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			status = FAIL;
			e.printStackTrace();
		}

		while ((sDB_lines = sDB_br.readLine()) != null) {
			int i = 0;
			for (String s : sDB_lines.split(",")) {
				i++;
				try {
					t.setIntFld(i, Integer.parseInt(s));
				} catch (Exception e) {
					System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
					status = FAIL;
					e.printStackTrace();
				}
			}



			try {
				srid = sf.insertRecord(t.returnTupleByteArray());
			} catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				status = FAIL;
				e.printStackTrace();
			}
			if (status != OK) {
				// bail out
				System.err.println("*** Error creating relation for S");
				Runtime.getRuntime().exit(1);
			}
		}
		sDB_br.close();
		System.out.println("RecCnt of S : " + String.valueOf(sf.getRecCnt()));


		// ************************************************************************//
		// 						   Creating R Database 							   //
		// ************************************************************************//
		// creating the R relation
		AttrType[] Btypes = new AttrType[num_attrs];
		for (int i=0; i<num_attrs; i++) {
			Btypes[i] = new AttrType(AttrType.attrInteger);
		}

		short[] Bsizes = new short[0];

		t = new Tuple();
		try {
			t.setHdr((short) num_attrs, Btypes, Bsizes);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			status = FAIL;
			e.printStackTrace();
		}

		size = t.size();

		// inserting the tuple into file "R"
		File rDB = new File(r_path);
		BufferedReader rDB_br;
		rDB_br = new BufferedReader(new FileReader(rDB));
		String rDB_lines;
		rDB_lines = rDB_br.readLine();

		Heapfile rf = null;
		try {
			rf = new Heapfile(r_file + ".in");
		} catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			status = FAIL;
			e.printStackTrace();
		}

		t = new Tuple(size);
		try {
			t.setHdr((short) 4, Stypes, Ssizes);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			status = FAIL;
			e.printStackTrace();
		}
		RID rrid;
		int l =0;
		while ((rDB_lines = rDB_br.readLine()) != null && l < max_iters) {
			l++;
			int i = 0;
			for (String s : rDB_lines.split(",")) {
				i++;
				try{
					t.setIntFld(i, Integer.parseInt(s));
				} catch (Exception e) {
					System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
					status = FAIL;
					e.printStackTrace();
				}
			}

			try {
				rrid = rf.insertRecord(t.returnTupleByteArray());
			} catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				status = FAIL;
				e.printStackTrace();
			}
			if (status != OK) {
				// bail out
				System.err.println("*** Error creating relation for S");
				Runtime.getRuntime().exit(1);
			}
		}
		rDB_br.close();
		System.out.println("RecCnt of R : " + String.valueOf(rf.getRecCnt()));

		// ************************************************************************//
		// 							Creating Q Database 						   //
		// ************************************************************************//
		// creating the R relation
		AttrType[] Qtypes = new AttrType[num_attrs];
		for (int i=0; i<num_attrs; i++) {
			Qtypes[i] = new AttrType(AttrType.attrInteger);
		}

		short[] Qsizes = new short[0];

		t = new Tuple();
		try {
			t.setHdr((short) num_attrs, Qtypes, Qsizes);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			status = FAIL;
			e.printStackTrace();
		}

		size = t.size();

		// inserting the tuple into file "q"
		File qDB = new File(q_path);
		BufferedReader qDB_br;
		qDB_br = new BufferedReader(new FileReader(qDB));
		String qDB_lines;
		qDB_lines = qDB_br.readLine();

		Heapfile qf = null;
		try {
			qf = new Heapfile(q_file + ".in");
		} catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			status = FAIL;
			e.printStackTrace();
		}

		t = new Tuple(size);
		try {
			t.setHdr((short) 4, Stypes, Ssizes);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			status = FAIL;
			e.printStackTrace();
		}
		RID qrid;
		int p =0;
		while ((qDB_lines = qDB_br.readLine()) != null && p < max_iters) {
			p++;
			int i = 0;
			for (String s : qDB_lines.split(",")) {
				i++;
				try{
					t.setIntFld(i, Integer.parseInt(s));
				} catch (Exception e) {
					System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
					status = FAIL;
					e.printStackTrace();
				}
			}

			try {
				qrid = qf.insertRecord(t.returnTupleByteArray());
			} catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				status = FAIL;
				e.printStackTrace();
			}
			if (status != OK) {
				// bail out
				System.err.println("*** Error creating relation for S");
				Runtime.getRuntime().exit(1);
			}
		}
		qDB_br.close();
		System.out.println("RecCnt of Q : " + String.valueOf(qf.getRecCnt()));

	}

	// This is the join algorithm with two predicades involving two relations
	// We know that int len_in1 = len_in2 = 4 and AttrType in1 = AttrType in2 = [int, int, int, int]

	// Be careful when running all the tests at once: a BufferMgr error can be output 
	// It is encouraged to run each test independantly and comment the others.
	public boolean runTests(boolean verbose) throws IOException {

	//	Query1a("query_1a", verbose);
	//	Query1b("query_1b", verbose);
	//	Query2a("query_2a", verbose);
	//	Query2b("query_2b", verbose);
		Query2c("query_2c", verbose);
		System.out.print("Finished joins testing" + "\n");

		return true;
	}
	
	// Condition Expression for Single Predicate
	private void CondExpr_single(CondExpr[] outFilter, FileQuery query) {
		// Since we have a single condition, outFilter is an array of size 2: [CondExpr, null]
		outFilter[0].next  = null;
		outFilter[0].op    = new AttrOperator(Integer.parseInt(query.getConStrings_1()[1]));
		outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
		outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
		outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),
				Integer.parseInt(
						query.getConStrings_1()[0]
								.split("_", 0)[1])); // # of the column from the outer relation to use for selection
		outFilter[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),
				Integer.parseInt(
						query.getConStrings_1()[2]
								.split("_", 0)[1])); // # of the column from the inner relation to use for selection

		outFilter[1] = null;
	}

	// Condition Expression for two predicate
	private void CondExpr_double(CondExpr[] outFilter, FileQuery query) {
		// Since we have 2 conditions, outFilter is an array of size : [CondExpr_1, CondExpr_2, null]
		outFilter[0].next  = null;
		outFilter[0].op    = new AttrOperator(Integer.parseInt(query.getConStrings_1()[1]));
		outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
		outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
		outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),
				Integer.parseInt(
						query.getConStrings_1()[0]
								.split("_", 0)[1])); // # of the column from the outer relation to use for first selection
		outFilter[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),
				Integer.parseInt(
						query.getConStrings_1()[2]
								.split("_", 0)[1])); // # of the column from the inner relation to use for first selection

		outFilter[1].next  = null;
		outFilter[1].op    = new AttrOperator(Integer.parseInt(query.getConStrings_2()[1]));
		outFilter[1].type1 = new AttrType(AttrType.attrSymbol);
		outFilter[1].type2 = new AttrType(AttrType.attrSymbol);
		outFilter[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),
				Integer.parseInt(
						query.getConStrings_2()[0]
								.split("_", 0)[1])); // # of the column from the outer relation to use for second selection
		outFilter[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),
				Integer.parseInt(
						query.getConStrings_2()[2]
								.split("_", 0)[1])); // # of the column from the inner relation to use for second selection

		outFilter[2] = null;
	}

	//Query for task 1a: Single predicate Nested Loop Join
	public void Query1a(String query_file, boolean verbose) throws IOException {
		if (verbose) {
			System.out.print("**********************Query_1a starting *********************\n");
			System.out.print("Query: Single Predicate Inequality Join \n" 
					+ "  SELECT   S.attributeN, R.attributeN \n" 
					+ "  FROM     S, R \n"
					+ "  WHERE    S.atributeM op R.atributeM\n" + "Plan used:\n"
					+ " Sort (Pi(S.attributeN, R.attributeN) (Sigma(S.attributeM op R.attributeM )  "
					+ "|><|  Pi(S.attributeN, R.attributeN) (S  |><|  R)))\n\n"
					+ "(Tests File scan, Projection, sort and simple nested-loop join.)\n\n");
		}

		boolean status = OK;
		long startTime = System.currentTimeMillis();
		try {
			// First we try to read the query file in order to extract
			// relations names, projection and condition columns
			FileQuery query = new FileQuery(query_file + ".txt", query_data_path + "/", 1);

			CondExpr [] outFilter  = new CondExpr[2];
			outFilter[0] = new CondExpr();
			outFilter[1] = new CondExpr();

			CondExpr_single(outFilter, query);

			Tuple t = new Tuple();
			t = null;
			// Inner relation attribute types (all integers)
			AttrType Stypes[] = {
					new AttrType(AttrType.attrInteger),
					new AttrType(AttrType.attrInteger),
					new AttrType(AttrType.attrInteger),
					new AttrType(AttrType.attrInteger)
			};

			// We probably won't need this because we are only working with integers
			short []   Ssizes = new short[0];

			// Outer relation attribute types (all integers)
			AttrType [] Rtypes = {
					new AttrType(AttrType.attrInteger),
					new AttrType(AttrType.attrInteger),
					new AttrType(AttrType.attrInteger),
					new AttrType(AttrType.attrInteger)
			};

			// We probably won't need this because we are only working with integers
			short  []  Rsizes = new short[1];
			Rsizes[0] = 0;

			// Select columns to work with
			// Those are not the final columns to show
			// but the ones we will be working with to do the join
			FldSpec [] Sprojection = {
					new FldSpec(new RelSpec(RelSpec.outer), 1),
					new FldSpec(new RelSpec(RelSpec.outer), 2),
					new FldSpec(new RelSpec(RelSpec.outer), 3),
					new FldSpec(new RelSpec(RelSpec.outer), 4),
			};

			// Select columns to show (projection)
			// This piece of information is fetched from the query file
			FldSpec [] Projection = {
					new FldSpec(new RelSpec(RelSpec.outer), 
							Integer.parseInt(
									query.getAttributeStrings()[0]
											.split("_", 0)[1])),
					new FldSpec(new RelSpec(RelSpec.innerRel),
							Integer.parseInt(
									query.getAttributeStrings()[1]
											.split("_", 0)[1]))
			};

			//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
			//***************  create a scan on the heapfile  ***************
			//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

			// Open and scan one of the database as heapfile
			iterator.Iterator am = null;

			try {
				am = new FileScan(query.getRelationsStrings()[0] + ".in", 
						Stypes, Ssizes, (short) 4, (short) 4, Sprojection, null);
			} catch (Exception e) {
				status = FAIL;
				System.err.println("" + e);
			}

			// Nested Loop Join
			NestedLoopsJoins nlj = null;
			try {
				nlj = new NestedLoopsJoins (Stypes, 4, Ssizes,
						Rtypes, 4, Rsizes,
						10,
						am, query.getRelationsStrings()[1] + ".in",
						outFilter, null, Projection, 2);
			}
			catch (Exception e) {
				System.err.println ("*** Error preparing for nested_loop_join");
				System.err.println (""+e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}

			AttrType[] jtype = { new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger),
					new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)};

			t = null;
			int i = 0;
			PrintWriter pw = new PrintWriter("output_" + query_file + ".txt");
			try {
				while ((t = nlj.get_next()) != null) {
					i++;
					t.print(jtype); // print results
					pw.print("[" + t.getIntFld(1) + ","  +  t.getIntFld(2) +  "]\n"); // get tuples in .txt file
				}
				pw.close();
				// print the total number of returned tuples
				System.out.println("Output Tuples for " + query_file + ": " + i);
			} catch (Exception e) {
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}

			try {
				nlj.close();
			} catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}

			if (status != OK) {
				//bail out
				Runtime.getRuntime().exit(1);
			}

		} catch (FileNotFoundException e1) {
			status = FAIL;
			e1.printStackTrace();
		}
		long elapsedTime = System.currentTimeMillis() - startTime;
		writeToFile(query_file + "_nlj.csv", max_iters, elapsedTime); // get running time in csv file
	}

	// Query for task 1b: Two predicate Nested Loop Join 
	public void Query1b(String query_file, boolean verbose) throws IOException {
		if (verbose) {
			System.out.print("**********************Query_1b starting *********************\n");
			System.out.print("Query: Two Predicates Inequality Join \n" 
					+ "  SELECT   S.attributeN, R.attributeN \n" 
					+ "  FROM     S, R \n"
					+ "  WHERE    S.atributeM op1 R.atributeM\n"
					+ "  AND      S.atributeQ op2 R.atributeQ\n"
					+ "(Tests File scan, Projection, sort and simple nested-loop join.)\n\n");
		}

		boolean status = OK;
		long startTime = System.currentTimeMillis();
		try {
			// First we try to read the query file in order to extract
			// relations names, projection and condition columns
			FileQuery query = new FileQuery(query_file + ".txt", query_data_path + "/", 2);

			CondExpr [] outFilter  = new CondExpr[3];
			outFilter[0] = new CondExpr();
			outFilter[1] = new CondExpr();
			outFilter[2] = new CondExpr();

			CondExpr_double(outFilter, query);

			Tuple t = new Tuple();
			t = null;

			// Inner relation attribute types (all integers)
			AttrType Stypes[] = {
					new AttrType(AttrType.attrInteger),
					new AttrType(AttrType.attrInteger),
					new AttrType(AttrType.attrInteger),
					new AttrType(AttrType.attrInteger)
			};
			short []   Ssizes = new short[0];

			// Outer relation attribute types (all integers)
			AttrType [] Rtypes = {
					new AttrType(AttrType.attrInteger),
					new AttrType(AttrType.attrInteger),
					new AttrType(AttrType.attrInteger),
					new AttrType(AttrType.attrInteger)
			};
			short  []  Rsizes = new short[1];
			Rsizes[0] = 0;

			// Select columns to work with
			// Those are not the final columns to show
			// but the ones we will be working with to do the join
			FldSpec [] Sprojection = {
					new FldSpec(new RelSpec(RelSpec.outer), 1),
					new FldSpec(new RelSpec(RelSpec.outer), 2),
					new FldSpec(new RelSpec(RelSpec.outer), 3),
					new FldSpec(new RelSpec(RelSpec.outer), 4),
			};

			// Select columns to show (projection)
			// This piece of information is fetched from the query file
			FldSpec [] Projection = {
					new FldSpec(new RelSpec(RelSpec.outer), 
							Integer.parseInt(
									query.getAttributeStrings()[0]
											.split("_", 0)[1])),
					new FldSpec(new RelSpec(RelSpec.innerRel),
							Integer.parseInt(
									query.getAttributeStrings()[1]
											.split("_", 0)[1]))
			};

			//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
			//***************  create a scan on the heapfile  ***************
			//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

			// Open and scan one of the database as heapfile
			iterator.Iterator am = null;

			try {
				am = new FileScan(query.getRelationsStrings()[0] + ".in", 
						Stypes, Ssizes, (short) 4, (short) 4, Sprojection, null);
			} catch (Exception e) {
				status = FAIL;
				System.err.println("" + e);
			}

			// Nested Loop Join
			NestedLoopsJoins nlj = null;
			try {
				nlj = new NestedLoopsJoins (Stypes, 4, Ssizes,
						Rtypes, 4, Rsizes,
						10,
						am, query.getRelationsStrings()[0] + ".in",
						outFilter, null, Projection, 2);
			}
			catch (Exception e) {
				System.err.println ("*** Error preparing for nested_loop_join");
				System.err.println (""+e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}

			AttrType[] jtype = { new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger),
					new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)};

			t = null;
			int i = 0;
			PrintWriter pw = new PrintWriter("output_" + query_file + ".txt");
			try {
				while ((t = nlj.get_next()) != null) {
					i++;
					t.print(jtype); // print results
					pw.print("[" + t.getIntFld(1) + ","  +  t.getIntFld(2) +  "]\n"); // print results in .txt file
				}
				pw.close();
				// print the total number of returned tuples
				System.out.println("Output Tuples for " + query_file + ": " + i);
			} catch (Exception e) {
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}

			try {
				nlj.close();
			} catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}

			if (status != OK) {
				//bail out
				Runtime.getRuntime().exit(1);
			}

		} catch (FileNotFoundException e1) {
			status = FAIL;
			e1.printStackTrace();
		}
		long elapsedTime = System.currentTimeMillis() - startTime;
		writeToFile(query_file + "_nlj.csv", max_iters, elapsedTime); // get time in .csv file
	}

	// Query for task 2a: Single predicate IE Self Join
	public void Query2a(String query_file, boolean verbose) throws IOException {
		if (verbose) {
			System.out.print("**********************Query2a starting *********************\n");
			System.out.print("Query: Single Predicate Self Inequality Join " 
			+ "  SELECT   Q.attributeN, Q.attributeN \n" 
					+ "  FROM   Q \n"
					+ "  WHERE    Q.atributeM op Q.atributeM\n" + "Plan used:\n"
					+ " Sort (Pi(Q.attributeN, Q.attributeN) (Sigma(Q.attributeM op Q.attributeM )  "
					+ "|><|  Pi(Q.attributeN, Q.attributeN) (Q  |><|  Q)))\n\n"
					+ "(File scan, Projection, IE self join.)\n\n");
		}

		boolean status = OK;
		long startTime = System.currentTimeMillis();

		try {
			// First we try to read the query file in order to extract
			// relations names, projection and condition columns
			FileQuery query = new FileQuery(query_file + ".txt", query_data_path + "/", 1);

			CondExpr [] outFilter  = new CondExpr[2];
			outFilter[0] = new CondExpr();
			outFilter[1] = new CondExpr();

			CondExpr_single(outFilter, query);

			Tuple t = new Tuple();
			t = null;
			// Inner relation attribute types (all integers)
			AttrType Qtypes[] = {
					new AttrType(AttrType.attrInteger),
					new AttrType(AttrType.attrInteger),
					new AttrType(AttrType.attrInteger),
					new AttrType(AttrType.attrInteger)
			};

			// We probably won't need this because we are only working with integers
			short []   Qsizes = new short[1];
			Qsizes[0] = (short) 0;

			// Select columns to work with
			// Those are not the final columns to show
			// but the ones we will be working with to do the join
			FldSpec [] Qprojection = {
					new FldSpec(new RelSpec(RelSpec.outer), 1),
					new FldSpec(new RelSpec(RelSpec.outer), 2),
					new FldSpec(new RelSpec(RelSpec.outer), 3),
					new FldSpec(new RelSpec(RelSpec.outer), 4),
			};

			// Select columns to show (projection)
			// This piece of information is fetched from the query file
			FldSpec [] Projection = {
					new FldSpec(new RelSpec(RelSpec.outer), 
							Integer.parseInt(
									query.getAttributeStrings()[0]
											.split("_", 0)[1])),
					new FldSpec(new RelSpec(RelSpec.innerRel),
							Integer.parseInt(
									query.getAttributeStrings()[1]
											.split("_", 0)[1]))
			};

			//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
			//***************  create a scan on the heapfile  ***************
			//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

			// Open and scan one of the database as heapfile
			// Two same iterator are created for the sort on the heapfile: 
			// two sort on the same heapfile gives an error
			iterator.Iterator am = null;
			iterator.Iterator am2 = null;

			try {
				am = new FileScan(query.getRelationsStrings()[0] + ".in", 
						Qtypes, Qsizes, (short) 4, (short) 4, Qprojection, null);
				am2 = new FileScan(query.getRelationsStrings()[0] + ".in", 
						Qtypes, Qsizes, (short) 4, (short) 4, Qprojection, null);
			} catch (Exception e) {
				status = FAIL;
				System.err.println("" + e);
			}

			// Nested Loop Join
			IESelfJoin ieSelfJoin = null;
			try {
				ieSelfJoin = new IESelfJoin(Qtypes, 4, Qsizes, 10, am, am2, 
						outFilter, null, Projection, 2, 1);
			}
			catch (Exception e) {
				System.err.println ("*** Error preparing for nested_loop_join");
				System.err.println (""+e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}

			AttrType[] jtype = { new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)};

			t = null;
			int i = 0;
			PrintWriter pw = new PrintWriter("output_" + query_file + ".txt");
			try {
				while ((t = ieSelfJoin.get_next()) != null) {
					i++;
					t.print(jtype); // print tuple results
					pw.print("[" + t.getIntFld(1) + ","  +  t.getIntFld(2) +  "]\n"); // output in file
				}
				pw.close();
				// print the total number of returned tuples
				System.out.println("Output Tuples for " + query_file + ": " + i);
			} catch (Exception e) {
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}

			try {
				ieSelfJoin.close();
			} catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}

			if (status != OK) {
				//bail out
				Runtime.getRuntime().exit(1);
			}

		} catch (FileNotFoundException e1) {
			status = FAIL;
			e1.printStackTrace();
		}
		long elapsedTime = System.currentTimeMillis() - startTime;
		writeToFile(query_file + ".csv", max_iters, elapsedTime); // time data for plot
	}

	// Query of task 2b: Two predicate IE Self Join
	public void Query2b(String query_file, boolean verbose) throws IOException {
		if (verbose) {
			System.out.print("**********************Query2b starting *********************\n");
			System.out.print("Query: Two Predicate Self Inequality Join " 
					+ "  SELECT   Q.attributeN, Q.attributeN \n" 
					+ "  FROM   Q, Q\n"
					+ "  WHERE    Q.atributeM op1 Q.atributeM\n"
					+ "  AND Q.attributeO op2 Q.attributeO\n");

		}

		boolean status = OK;
		long startTime = System.currentTimeMillis();

		try {
			// First we try to read the query file in order to extract
			// relations names, projection and condition columns
			FileQuery query = new FileQuery(query_file + ".txt", query_data_path + "/", 2);

			CondExpr [] outFilter  = new CondExpr[3];
			outFilter[0] = new CondExpr();
			outFilter[1] = new CondExpr();
			outFilter[2] = new CondExpr();

			CondExpr_double(outFilter, query);

			Tuple t = new Tuple();
			t = null;
			// Inner relation attribute types (all integers)
			AttrType Qtypes[] = {
					new AttrType(AttrType.attrInteger),
					new AttrType(AttrType.attrInteger),
					new AttrType(AttrType.attrInteger),
					new AttrType(AttrType.attrInteger)
			};

			// We probably won't need this because we are only working with integers
			short []   Qsizes = new short[1];
			Qsizes[0] = (short) 0;

			// Select columns to work with
			// Those are not the final columns to show
			// but the ones we will be working with to do the join
			FldSpec [] Qprojection = {
					new FldSpec(new RelSpec(RelSpec.outer), 1),
					new FldSpec(new RelSpec(RelSpec.outer), 2),
					new FldSpec(new RelSpec(RelSpec.outer), 3),
					new FldSpec(new RelSpec(RelSpec.outer), 4),
			};

			// Select columns to show (projection)
			// This piece of information is fetched from the query file
			FldSpec [] Projection = {
					new FldSpec(new RelSpec(RelSpec.outer), 
							Integer.parseInt(
									query.getAttributeStrings()[0]
											.split("_", 0)[1])),
					new FldSpec(new RelSpec(RelSpec.innerRel),
							Integer.parseInt(
									query.getAttributeStrings()[1]
											.split("_", 0)[1]))
			};

			//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
			//***************  create a scan on the heapfile  ***************
			//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

			// Open and scan one of the database as heapfile
			// Two same iterator are created for the sort on the heapfile: 
			// two sort on the same heapfile gives an error
			iterator.Iterator am = null;
			iterator.Iterator am2 = null;

			try {
				am = new FileScan(query.getRelationsStrings()[0] + ".in", 
						Qtypes, Qsizes, (short) 4, (short) 4, Qprojection, null);
				am2 = new FileScan(query.getRelationsStrings()[0] + ".in", 
						Qtypes, Qsizes, (short) 4, (short) 4, Qprojection, null);
			} catch (Exception e) {
				status = FAIL;
				System.err.println("" + e);
			}

			// Nested Loop Join
			IESelfJoin ieSelfJoin = null;
			try {
				ieSelfJoin = new IESelfJoin(Qtypes, 4, Qsizes, 10, am, am2, 
						outFilter, null, Projection, 2, 2);
			}
			catch (Exception e) {
				System.err.println ("*** Error preparing for nested_loop_join");
				System.err.println (""+e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}

			AttrType[] jtype = { new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger),
					new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)};

			t = null;
			int i = 0;
			PrintWriter pw = new PrintWriter("output_" + query_file + ".txt");
			try {
				while ((t = ieSelfJoin.get_next()) != null) {
					i++;
					t.print(jtype); // print tuple results
					pw.print("[" + t.getIntFld(1) + ","  +  t.getIntFld(2) +  "]\n"); // output in files
				}
				pw.close();
				// print the total number of returned tuples
				System.out.println("Output Tuples for " + query_file + ": " + i);
			} catch (Exception e) {
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}

			try {
				ieSelfJoin.close();
			} catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}

			if (status != OK) {
				//bail out
				Runtime.getRuntime().exit(1);
			}

		} catch (FileNotFoundException e1) {
			status = FAIL;
			e1.printStackTrace();
		}
		long elapsedTime = System.currentTimeMillis() - startTime;
		writeToFile(query_file + "_sj.csv", max_iters, elapsedTime); // time data for plot
	}

	// Query for task 2c: Two predicate Inequality Join on two different tables
	public void Query2c(String query_file, boolean verbose) throws IOException {
		if (verbose) {
			System.out.print("**********************Query2c starting *********************\n");
			System.out.print("Query: Two Predicate Inequality Join " 
					+ "  SELECT   R.attributeN, Q.attributeN \n" 
					+ "  FROM   R, Q \n"
					+ "  WHERE    R.atributeM op1 Q.atributeM\n"
					+ "  AND R.attributeO op2 Q.attributeO\n");
		}

		boolean status = OK;
		long startTime = System.currentTimeMillis();

		try {
			// First we try to read the query file in order to extract
			// relations names, projection and condition columns
			FileQuery query = new FileQuery(query_file + ".txt", query_data_path + "/", 2);

			CondExpr [] outFilter  = new CondExpr[3];
			outFilter[0] = new CondExpr();
			outFilter[1] = new CondExpr();
			outFilter[2] = new CondExpr();

			CondExpr_double(outFilter, query);

			Tuple t = new Tuple();
			t = null;
			// Inner relation attribute types (all integers)
			AttrType Qtypes[] = {
					new AttrType(AttrType.attrInteger),
					new AttrType(AttrType.attrInteger),
					new AttrType(AttrType.attrInteger),
					new AttrType(AttrType.attrInteger)
			};

			// We probably won't need this because we are only working with integers
			short []   Qsizes = new short[1];
			Qsizes[0] = 0;

			// Select columns to work with
			// Those are not the final columns to show
			// but the ones we will be working with to do the join
			FldSpec [] Qprojection = {
					new FldSpec(new RelSpec(RelSpec.outer), 1),
					new FldSpec(new RelSpec(RelSpec.outer), 2),
					new FldSpec(new RelSpec(RelSpec.outer), 3),
					new FldSpec(new RelSpec(RelSpec.outer), 4),
			};

			// Select columns to show (projection)
			// This piece of information is fetched from the query file
			FldSpec [] Projection = {
					new FldSpec(new RelSpec(RelSpec.outer), 
							Integer.parseInt(
									query.getAttributeStrings()[0]
											.split("_", 0)[1])),
					new FldSpec(new RelSpec(RelSpec.innerRel),
							Integer.parseInt(
									query.getAttributeStrings()[1]
											.split("_", 0)[1]))
			};

			//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
			//***************  create a scan on the heapfile  ***************
			//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

			// Open and scan one of the database as heapfile
			// Two times two same iterator are created for the sort on the heapfile: 
			// two sort on the same heapfile gives an error
			iterator.Iterator outer1 = null;
			iterator.Iterator outer2 = null;
			iterator.Iterator inner1 = null;
			iterator.Iterator inner2 = null;

			try {
				outer1 = new FileScan(query.getRelationsStrings()[0] + ".in", 
						Qtypes, Qsizes, (short) 4, (short) 4, Qprojection, null);
				outer2 = new FileScan(query.getRelationsStrings()[0] + ".in", 
						Qtypes, Qsizes, (short) 4, (short) 4, Qprojection, null);
			} catch (Exception e) {
				status = FAIL;
				System.err.println("" + e);
			}

			try {
				inner1 = new FileScan(query.getRelationsStrings()[1] + ".in", 
						Qtypes, Qsizes, (short) 4, (short) 4, Qprojection, null);
				inner2 = new FileScan(query.getRelationsStrings()[1] + ".in", 
						Qtypes, Qsizes, (short) 4, (short) 4, Qprojection, null);
			} catch (Exception e) {
				status = FAIL;
				System.err.println("" + e);
			}

			// Nested Loop Join
			IEjoin ieJoin = null;
			try {
				ieJoin = new IEjoin(Qtypes, 4, Qsizes, Qtypes, 4, Qsizes, 10, 
						outer1, outer2, inner1, inner2, outFilter, 
						null, Projection, 2);
			}
			catch (Exception e) {
				System.err.println ("*** Error preparing for nested_loop_join");
				System.err.println (""+e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}

			AttrType[] jtype = { new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger),
					new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)};
			PrintWriter pw = new PrintWriter("output_" + query_file + "_ie.txt");
			t = null;
			int i = 0;
			try {
				while ((t = ieJoin.get_next()) != null) {
					i++;
					t.print(jtype); // result print
					pw.print("[" + t.getIntFld(1) + ","  +  t.getIntFld(2) +  "]\n"); // output file
				}
				pw.close();
				// print the total number of returned tuples
				System.out.println("Output Tuples for " + query_file + ": " + i);
			} catch (Exception e) {
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}

			try {
				ieJoin.close();
			} catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}

			if (status != OK) {
				//bail out
				Runtime.getRuntime().exit(1);
			}

		} catch (FileNotFoundException e1) {
			status = FAIL;
			e1.printStackTrace();
		}
		long elapsedTime = System.currentTimeMillis() - startTime;
		writeToFile(query_file + "_ie.csv", max_iters, elapsedTime); // time data for figures
	}

	public void writeToFile(String file, int iters, long elapsedTime) throws IOException {
		BufferedWriter bw = 
				new BufferedWriter(new FileWriter(file, true));
		bw.write(iters + "," + elapsedTime);
		bw.newLine();
		bw.flush();
		bw.close();
	}
}

public class QueryIneqTest {
	public static void main(String argv[]) throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException, HFBufMgrException, IOException
	{
		boolean sortstatus;
		//SystemDefs global = new SystemDefs("bingjiedb", 100, 70, null);
		//JavabaseDB.openDB("/tmp/nwangdb", 5000);
		
		// For loop to increment the size of tuple entries to stress algorithms 
		// and get the figures against execution time 
		int first_row = 400; 
		int last_row = 400;
		// Testing for different numbers of rows in Q: 500; 1,000; 1,500; 2,000; ...; 500,000
		for (int i=first_row; i<=last_row; i+=1) 
		{ 
			System.out.println("Testing for " + Integer.toString(i) + " entries ...");
			JoinsDriverAssign jjoin = new JoinsDriverAssign("R", "S", "Q", "QueriesData", i);
			
			sortstatus = jjoin.runTests(false);
			if (sortstatus != true) {
				System.out.println("Error ocurred during join tests\n");
			}
			else {
				System.out.println("join tests completed successfully\n");
			}
		}
	}
}

