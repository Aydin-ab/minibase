package iterator;

import java.io.IOException;

import bufmgr.PageNotReadException;
import global.AttrOperator;
import global.AttrType;
import global.TupleOrder;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;
import java.util.ArrayList;


/**
* IEJoin class.
* The join is in the constructor rather than in the get_next() 
* because I think get_next() should stick to its literal name 
* and just give the next tuple
* It's more intuitive to think that the join is being done in the construction
* (At least for me)
* 
*/
public class IEJoin extends Iterator {

	private AttrType      _in1[],  _in2[];
	private int in1_len;
	private int in2_len;    
	private Iterator outer;
	private Iterator inner;
	private CondExpr OutputFilter[];
	private int n_buf_pgs;        // # of buffer pages available.
	private boolean done;      // Is the join complete
	private Tuple Jtuple;           // Joined tuple
	private FldSpec perm_mat[];
	private int nOutFlds;

	// My parameters
	private int[] perm_ope_1; // Permutation array of pred_2_op1 relative to pred_1_op1 (the 1st operands of the predicates)
	private int[] perm_ope_2; // Permutation array of pred_2_op2 relative to pred_1_op2 (the 2nd operands of the predicates)
	private int[] offset_pred_1; //Offsets array of the first predicate
	private int[] offset_pred_2; //Offsets array of the first predicate
	private int[] bitmap; // Bitmap array

	private Sort pred_1_op1; // Operand 1 of predicate 1
	private Sort pred_1_op2; // Operand 2 of predicate 1
	private Sort pred_2_op1; // Operand 1 of predicate 2
	private Sort pred_2_op2; // Operand 2 of predicate 2
	private ArrayList<Tuple> _pred_1_op1; // ArrayList of pred_1_op1
	private ArrayList<Tuple> _pred_1_op2; // ArrayList of pred_1_op2
	private ArrayList<Tuple> _pred_2_op1; // ArrayList of pred_2_op1
	private ArrayList<Tuple> _pred_2_op2; // CLone ArrayList of pred_2_op2
	private ArrayList<Tuple> finalJoin; // ArrayList of the final join list


    /**
    * Constructor of the IEJoin. 
    *Initialize the two relations which are joined, including relation type
    *Create the resulting join relation in the this.finalJoin attribute.
    *
    *@param in1  AttrType[] : list containing field types of relation 1.
    *@param _in1_len int ; # of columns in relation 1.
    *@param t1_str_sizes short[] : list shows the length of the string fields in relation 1.
    *@param in2  AttrType[] : list containing field types of relation 2.
    *@param _in2_len int ; # of columns in relation 2
    *@param t2_str_sizes short[] : list shows the length of the string fields in relation 2.
    *@param amt_of_mem  int : IN PAGES. To be fixed in the main of the test
    *@param outer1  Iterator : access method for first outer relation
    *@param outer1  Iterator : access method for second outer relation
    *@param inner1  Iterator : access method for first inner relation
    *@param inner1  Iterator : access method for second inner relation
    *@param _OutputFilter CondExpr[] : select expressions
    *@param proj_list FldSpec[] : shows what input fields go where in the output tuple
    *@param n_out_flds int : number of outer relation fields
    *
    *@exception IOException : some I/O fault
    *@exception NestedLoopException : exception from this class
    *
	*/
	public IEJoin( 	AttrType in1[],    
					int _in1_len,           
					short t1_str_sizes[],
					AttrType in2[],         
					int _in2_len,           
					short t2_str_sizes[],   
					int amt_of_mem,        
					Iterator outer1, 
					Iterator outer2,
					Iterator inner1, 
					Iterator inner2,
					CondExpr _OutputFilter[],      
					FldSpec proj_list[],
					int n_out_flds
					) throws IOException,NestedLoopException
	{
		
		_in1 = new AttrType[in1.length];
		System.arraycopy(in1,0,_in1,0,in1.length);
		in1_len = _in1_len;
		
		_in2 = new AttrType[in2.length];
		System.arraycopy(in2,0,_in2,0,in2.length);
		in2_len = _in2_len;

		outer =outer1;
		inner = inner1;
		Jtuple = new Tuple();
		OutputFilter = _OutputFilter;
		
		n_buf_pgs    = amt_of_mem; // num pages for heapfile

		done  = false;

		perm_mat = proj_list;
		nOutFlds = n_out_flds;
		
		AttrType[] Jtypes = new AttrType[n_out_flds]; // for future join projection

		short[] t_size;

		perm_mat = proj_list;
		nOutFlds = n_out_flds;
		
		try {
			
			t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
					in1, in1_len, in1, in1_len,
					t1_str_sizes, t1_str_sizes,
					proj_list, nOutFlds);
		
		}
		catch (TupleUtilsException e){
			
			throw new NestedLoopException(e, "TupleUtilsException : IEJoins.java");
		
		}
		
		// Now we start the prep to join (I believe it's more logical to join in the constructor than in a get_next()
		// Because get_next() should stick to its literal name and just give the next tuple
		// It's more intuitive to think that the sort is being done in the construction

		
		// Sort for first predicate
		pred_1_op1 = null; // Operand 1
		pred_1_op2 = null; // Operand 2
		try {
			
			if (OutputFilter[0].op.attrOperator == AttrOperator.aopLT 
					|| OutputFilter[0].op.attrOperator == AttrOperator.aopLE)
			{
		
			TupleOrder desc = new TupleOrder(TupleOrder.Descending);
			pred_1_op1 = new Sort (in1, (short) in1_len, t1_str_sizes, (iterator.Iterator) outer1, 
									OutputFilter[0].operand1.symbol.offset, 
									desc, 0, n_buf_pgs);
			pred_1_op2 = new Sort (in2, (short) in2_len, t2_str_sizes, (iterator.Iterator) inner1, 
									OutputFilter[0].operand2.symbol.offset, 
									desc, 0, n_buf_pgs);
		
			} 
			else if (OutputFilter[0].op.attrOperator == AttrOperator.aopGT 
					|| OutputFilter[0].op.attrOperator == AttrOperator.aopGE) 
			{
				
				TupleOrder asc = new TupleOrder(TupleOrder.Ascending);
				pred_1_op1 = new Sort (in1, (short) in1_len, t1_str_sizes, (iterator.Iterator) outer1, 
										OutputFilter[0].operand1.symbol.offset, 
										asc, (short) 0, n_buf_pgs);
				pred_1_op2 = new Sort (in2, (short) in2_len, t2_str_sizes, (iterator.Iterator) inner1, 
										OutputFilter[0].operand2.symbol.offset, 
										asc, 0, n_buf_pgs);
			
			} 
			else {
				
				System.out.println("Unknown operator ?");
				
			}
			
		}
		catch(Exception e){
			
			System.err.println ("*** Error in Sort for 1st predicate in IEJoin");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
			
		}

		try {
			
			// Sort for second predicate
			pred_2_op1 = null; // operand 1
			pred_2_op2 = null; // operand 2
			if  (OutputFilter[1].op.attrOperator == AttrOperator.aopLT 
					|| OutputFilter[1].op.attrOperator == AttrOperator.aopLE)
			{
				
				TupleOrder asc = new TupleOrder(TupleOrder.Ascending);
				pred_2_op1 = new Sort (in1, (short) in1_len, t1_str_sizes,
						(iterator.Iterator) outer2, OutputFilter[1].operand1.symbol.offset, asc, t1_str_sizes[0], n_buf_pgs);
				pred_2_op2 = new Sort (in2, (short) in2_len, t2_str_sizes,
						(iterator.Iterator) inner2, OutputFilter[1].operand2.symbol.offset, asc, t2_str_sizes[0], n_buf_pgs);
			
			}
			else if (OutputFilter[1].op.attrOperator == AttrOperator.aopGT
					|| OutputFilter[1].op.attrOperator == AttrOperator.aopGE) {
	
				TupleOrder desc = new TupleOrder(TupleOrder.Descending);
				pred_2_op1 = new Sort (in1, (short) in1_len, t1_str_sizes,
						(iterator.Iterator) outer2, OutputFilter[1].operand1.symbol.offset, desc, t1_str_sizes[0], n_buf_pgs);
				pred_2_op2 = new Sort (in2, (short) in2_len, t2_str_sizes,
						(iterator.Iterator) inner2, OutputFilter[1].operand2.symbol.offset, desc, t2_str_sizes[0], n_buf_pgs);
			
			} else {
			
				System.out.println("Unknown operator ?");
			
			}
		
		}
		catch(Exception e){
			
			System.err.println ("*** Error in Sort for 2nd predicate in IEJoin");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
			
		}


		// Tuple to create ArrayList by iteration
		Tuple tup = new Tuple();
		Tuple tup_guy = new Tuple();
		Tuple tup_guy2 = new Tuple();
		Tuple tup_mommy = new Tuple();
	
		try {
			
			// ArrayList of pred_1_op1. Sorry..
			_pred_1_op1 = new ArrayList<Tuple>();
			while ((tup_guy2 = pred_1_op1.get_next()) != null) {	
			
				Tuple x = new Tuple(tup_guy2);
				_pred_1_op1.add(x);
			
			}
			
			// ArrayList of pred_1_op2. Sorry..
			_pred_1_op2 = new ArrayList<Tuple>();
			while ((tup = pred_1_op2.get_next()) != null) {	
			
				Tuple x = new Tuple(tup); 
				_pred_1_op2.add(x);
			
			}

			// ArrayList of pred_2_op1. Sorry..
			_pred_2_op1 = new ArrayList<Tuple>();
			while ((tup_mommy = pred_2_op1.get_next()) != null) {	
			
				Tuple x = new Tuple(tup_mommy);
				_pred_2_op1.add(x);
			
			}
			// ArrayList of pred_2_op2. Sorry..
			_pred_2_op2 = new ArrayList<Tuple>();
			while ((tup_guy = pred_2_op2.get_next()) != null) {	
			
				Tuple x = new Tuple(tup_guy);
				_pred_2_op2.add(x);
		
			}
		
		}
		catch(Exception e){
			
			System.err.println ("*** Error in get_next() when creating the Arraylist in IEJoin");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
			
		}
		

		// Number of tuples of the first operand of first predicate
		int size_pred_1_op1 = _pred_1_op1.size(); 
		// Number of tuples of the second operand of first predicate
		int size_pred_1_op2 = _pred_1_op2.size();
		// Bitmap and 
		bitmap = new int[size_pred_1_op2];
		
		
		// ** Permutation Arrays Initialization
		
		// Permutation array of the first operands and of the second operands
		perm_ope_1 = new int[size_pred_1_op1]; 
		// Initialization of the permutation array of the first operands
		try {
			
			int p = 0;
			for (Tuple t : _pred_2_op1) {
				
				int q = 0;
				for (Tuple s: _pred_1_op1) {
					
					if (areSame(t, s)) {
						
						perm_ope_1[p] = q;
						break;
						
					}
					q++;
					
				}
				p++;
				
			}
			
		}
		catch(Exception e){
			
			System.err.println ("*** Error when initalisating the permutation array perm_ope_1 in IEJoin");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
			
		}
		
		// Permutation array of the first operands and of the second operands
		perm_ope_2 = new int[size_pred_1_op2]; 
		// Initialization of the permutation array of the second operands
		try {
			
			int i = 0;
			for (Tuple tp : _pred_2_op2) {
				
				int j = 0;
				for (Tuple sp: _pred_1_op2) {
					
					if (areSame(tp, sp)) {
				
						perm_ope_2[i] = j;
						break;
				
					}
					j++;
			
				}
				bitmap[i] = 0;
				i++;
			
			}
			
		}
		catch(Exception e){
			
			System.err.println ("*** Error when initalisating the permutation array perm_ope_2 in IEJoin");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
			
		}
		
		
		// ** Offset arrays Initialization
		
		// Offset array of the first predicate
		offset_pred_1 = new int[size_pred_1_op1]; 
		offset_pred_2 = new int[size_pred_1_op2]; 
		
		try {
			
			int y = 0;
			for (Tuple t1 : _pred_1_op1) {
				
				int x = 0;
				for (Tuple s1: _pred_1_op2) {
					
					if (t1.getIntFld(OutputFilter[0].operand1.symbol.offset) 
							== s1.getIntFld(OutputFilter[0].operand2.symbol.offset)) 
					{
					
						offset_pred_1[y] = x;
						break;
				
					}
					x++;
			
				}
				y++;
			
			}
			
		}
		catch(Exception e){
			
			System.err.println ("*** Error when initalisating the Offset array offset_pred_1 in IEJoin");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
			
		}
		
		// Offset array of the second predicate
		offset_pred_2 = new int[size_pred_1_op1]; 

		try {
			
			// Offset array L2 - L2prime
			int v = 0;
			for (Tuple t2 : _pred_2_op1) {
			
				int u = 0;
				for (Tuple s2 : _pred_2_op2) {
					
					if (t2.getIntFld(OutputFilter[1].operand1.symbol.offset) 
							== s2.getIntFld(OutputFilter[1].operand2.symbol.offset)) 
					{
					
						offset_pred_2[v] = u;
						break;
				
					}
					u++;
			
				}
				v++;
		
			}
			
		}
		catch(Exception e){
			
			System.err.println ("*** Error when initalisating the Offset array offset_pred_2 in IEJoin");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
			
		}

		// Need a corrector if inequalities are strict or not 
		int corrector;
		if ((OutputFilter[0].op.toString() == "aopGE" 
				|| OutputFilter[0].op.toString() == "aopLE") 
					&& 
			(OutputFilter[1].op.toString() == "aopGE" 
				|| OutputFilter[1].op.toString() == "aopLE")) {
			
			corrector = 0;
		
		}
		else {
		
			corrector = 1;
		
		}
			
		// Final list of Join results
		finalJoin = new ArrayList<Tuple>() ;

		
		// ** Start of Join and Projection
		// Initialization of the iterating tuples
		int off_pred1; // Offset from predicate 1
		int off_pred2; // Offset from predicate 2

		
		for (int i1 = 0; i1 < _pred_1_op1.size() ; i1++) {
			
			off_pred2 = offset_pred_2[i1];
			// First we update the bitmap with the position array
			for (int j1=0; j1 < Math.min(off_pred2, _pred_2_op1.size()); j1++) {
				
				bitmap[perm_ope_2[j1]]=1;
			
			}
			off_pred1 = offset_pred_1[perm_ope_1[i1]];
			
			// Then we loop to check for possible joins
			for (int k = off_pred1 + corrector ; k < _pred_1_op2.size() ; k++) {
				
				// We can join if the corresponding bit is 1
				if (bitmap[k] == 1) { 
					
					// Tuple from predicate 1
					Tuple tup_pred1 = new Tuple(); // 
					tup_pred1 = null;
					tup_pred1 = _pred_1_op1.get(i1);
					
					// Tuple from predicate 2
					Tuple tup_pred2 = new Tuple();
					tup_pred2 = null;
					tup_pred2 = _pred_2_op2.get(k);
					
					// We join and project
					try {
						
						Projection.Join(tup_pred1, _in1, 
								tup_pred2, _in2, 
								Jtuple, perm_mat, nOutFlds);
						Tuple jtuple = new Tuple(Jtuple);
						finalJoin.add(jtuple);
						
					}
					catch(Exception e){
						
						System.err.println ("*** Error when Joining and Projecting in IEJoin");
						System.err.println (""+e);
						e.printStackTrace();
						Runtime.getRuntime().exit(1);
						
					}
					
				}
		
			}
		
		}

	}
	

	/**
	* Get next tuple from the resulting Join list. 
	* 
	* @return t Tuple : Next tuple in the resulting join
	* 
	*/
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
	InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
	LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {

		Tuple t = new Tuple();
		t = null;
		if (finalJoin.size() > 0) {
			
			int last = finalJoin.size() - 1;
			t = finalJoin.get(last);
			finalJoin.remove(last);
			
		}
		return t;

	} 

	
	/**
   	* Implement the abstract method close() from super class Iterator to finish cleaning up.
   	* 
   	*@exception IOException I/O error from lower layers
   	*@exception JoinsException join error from lower layers
   	*@exception IndexException index access error 
   	*/
	public void close() throws IOException, JoinsException, SortException, IndexException {
		
		if (!closeFlag) {

			try {
			
				outer.close();
				inner.close();
			
			}
			catch (Exception e) {
			
				throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
			
			}
			closeFlag = true;
		
		}
		
	}
	
	
	/**
	* Check if tuples are the same. 
	* 
	* @param  T1 : Tuple tuple to be compared
	* @param  T1 : Tuple tuple to be compared
	* 
	* @return <code> true </code> if the tuples are the same
	* 
	* @exception FieldNumberOutOfBoundException Just like the name says
	* @exception IOException I/O error from lower layers
	* 
	*/
	public boolean areSame(Tuple T1, Tuple T2) throws FieldNumberOutOfBoundException, IOException {
		if (T1.getIntFld(1) == T2.getIntFld(1)) {
			
			if (T1.getIntFld(2) == T2.getIntFld(2)) {
				
				if (T1.getIntFld(3) == T2.getIntFld(3)) {
					
					if (T1.getIntFld(4) == T2.getIntFld(4)) {
						return true;
					}
					else { return false; }
					
				}
				else { return false; }
				
			}
			else { return false; }
			
		}
		else { return false; }

	}
	
}