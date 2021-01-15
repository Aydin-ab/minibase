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
* IESelfJoin class.
* The join is in the constructor rather than in the get_next() 
* because I think get_next() should stick to its literal name 
* and just give the next tuple
* It's more intuitive to think that the join is being done in the construction
* (At least for me)
* 
*/
public class IESelfJoin extends Iterator {

	private AttrType      _in1[];
	private int        in1_len;
	private Iterator  rel_outer_1;
	private Iterator  rel_outer_2;
	private CondExpr OutputFilter[];
	private int        n_buf_pgs;        // # of buffer pages available.
	private Tuple     Jtuple;           // Joined tuple  
	private FldSpec   perm_mat[];
	private int        nOutFlds;
	// My parameters
	private Sort L1;
	private Sort L2; // For 2 predicates (task 2.b)
	private ArrayList<Tuple> _L1; // ArrayList of L1
	private ArrayList<Tuple> _L2; // ArrayList of L2 for 2 predicates (task 2.b)
	private ArrayList<Tuple> finalJoin; // ArrayList of the final join list
	private int[] perm; // Permutation array for 2 predicates (task 2.b)
	private int[] bitmap; // Bitmap array for 2 predicates (task 2.b)

	
	  /**
	   * Constructor of the IESelfJoin. 
	   *Initialize the two relations which are joined, including relation type
	   *Create the resulting join relation in the this.finalJoin attribute.
	   *
	   *@param in1  AttrType[] : list containing field types of Q.
	   *@param _in1_len int ; # of columns in Q.
	   *@param t1_str_sizes short[] : list shows the length of the string fields.
	   *@param amt_of_mem  int : IN PAGES. To be fixed in the main of the test
	   *@param am1  Iterator : access method for left i/p to join
	   *@param am2  Iterator : CLONE access method for left i/p to join
	   *@param _outputFilter CondExpr[] : select expressions
	   *@param proj_list FldSpec[] : shows what input fields go where in the output tuple
	   *@param n_out_flds int : number of outer relation fields
	   *
	   *@exception IOException : some I/O fault
	   *@exception NestedLoopException : exception from this class
	   *
	   */
	public IESelfJoin( 	AttrType[] 	in1,    
						int    		_in1_len,           
						short[]   	t1_str_sizes,   
						int    		amt_of_mem,        
						Iterator    am1,
						Iterator    am2,      
						CondExpr[]	_outputFilter,      
						FldSpec[] 	proj_list,
						int        	n_out_flds,
						int 		num_pred // Number of predicates
						) throws IOException,NestedLoopException
	{
		
		_in1 = new AttrType[in1.length];
		System.arraycopy(in1,0,_in1,0,in1.length);
		in1_len = _in1_len;

		rel_outer_1 = am1;
		rel_outer_2 = am2;
		Jtuple = new Tuple();
		OutputFilter = _outputFilter;

		n_buf_pgs    = amt_of_mem; // num pages for heapfile
		
		AttrType[] Jtypes = new AttrType[n_out_flds]; // for future Join projection

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
			
			throw new NestedLoopException(e, "TupleUtilsException : IESelfJoins.java");
		
		}
		
		
		// Now we start the prep to join (I believe it's more logical to join in the constructor than in a get_next()
		// Because get_next() should stick to its literal name and just give the next tuple
		// It's more intuitive to think that the sort is being done in the construction

		// Outer
		L1 = null;
		// Now we sort the outer, wrether it's 1 predicate or 2
		try {
			
			if (OutputFilter[0].op.attrOperator == AttrOperator.aopLE 
				|| OutputFilter[0].op.attrOperator == AttrOperator.aopLT){
			
				TupleOrder desc = new TupleOrder(TupleOrder.Descending);
				L1 = new Sort (in1, (short) in1_len, t1_str_sizes, (iterator.Iterator) am1, 
								OutputFilter[0].operand1.symbol.offset, 
								desc, (short) 0, n_buf_pgs);
			
			}
			else if (OutputFilter[0].op.attrOperator == AttrOperator.aopGE
					 || OutputFilter[0].op.attrOperator == AttrOperator.aopGT) {
				
				TupleOrder asc = new TupleOrder(TupleOrder.Ascending);
				L1 = new Sort (in1, (short) in1_len, t1_str_sizes, (iterator.Iterator) am1, 
								OutputFilter[0].operand1.symbol.offset, 
								asc, t1_str_sizes[0], n_buf_pgs);
				
			} 
			else {
				
				System.err.println("Unknown operator ?");
				
			}
		}
		
		catch (Exception e) {
			
				System.err.println ("*** Error during Sort of outer for IE Self Join");
				System.err.println (""+e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
				
		}
			
		// ArrayList of L1. Sorry..
		Tuple tup = new Tuple();
		tup = null;
		_L1 = new ArrayList<Tuple>(); 
		try{ 
			
			while ((tup = L1.get_next()) != null) {
			
				Tuple _tup = new Tuple(tup); 
				_L1.add(_tup);
			
			}
			
		}
		
		catch (Exception e) {
			
			System.err.println ("*** Error during ArrayList of L1 for IE Self Join");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
			
		}
		
		
		// Now we start to fill the join relation
		finalJoin = new ArrayList<Tuple>() ; // Initialization
			
		// If 1 predicate (Task 2.a)
		if (num_pred == 1) {
			
			// Loop through sorted relation on the condition attribute
			for (int i=0; i<_L1.size(); i++) {
				
				for (int j=0; j <= i-1; j++) {
					
					try {
						
						Tuple t1 = _L1.get(i);
						Tuple t2 = _L1.get(j);
						Projection.Join(t1, _in1, 
								t2, _in1, 
								Jtuple, perm_mat, nOutFlds);

						Tuple jtuple = new Tuple(Jtuple);
						finalJoin.add(jtuple);

					} 
					
					catch (Exception e) {
						
						System.err.println ("*** Error during Projection for IE Self Join on 1 predicate");
						System.err.println (""+e);
						e.printStackTrace();
						Runtime.getRuntime().exit(1);
						
					}
				
				}
				
			}
			
		} 
		
		// If 2 predicates (Task 2.b)
		else if (num_pred == 2) {
			
			// Inner
			L2 = null;
			
			// Now we sort the inner
			try {
					
				
				if (OutputFilter[1].op.attrOperator == AttrOperator.aopLE 
					|| OutputFilter[1].op.attrOperator == AttrOperator.aopLT) {
					
					TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
					L2 = new Sort (in1, (short) in1_len, t1_str_sizes,
									(iterator.Iterator) am2,
									OutputFilter[1].operand1.symbol.offset,
									ascending, 
									t1_str_sizes[0], n_buf_pgs);
				
				} 
				else if (OutputFilter[1].op.attrOperator == AttrOperator.aopGE 
						 || OutputFilter[1].op.attrOperator == AttrOperator.aopGT) {

					TupleOrder desc = new TupleOrder(TupleOrder.Descending);
					L2 = new Sort (in1, (short) in1_len, t1_str_sizes,
							(iterator.Iterator) am2, OutputFilter[1].operand1.symbol.offset, desc, t1_str_sizes[0], n_buf_pgs);
				
				}
				else {
					
					System.out.println("Unknown operator ?");
				
				}
				
			}
			
			catch (Exception e) {
				
				System.err.println ("*** Error during Sort of inner for IE Self Join");
				System.err.println (""+e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			
			}
				
			// ArrayList of L2. Sorry..
			_L2 = new ArrayList<Tuple>();
			try {
				
				while ((tup = L2.get_next()) != null) {
					
					Tuple x = new Tuple(tup);
					_L2.add(x);
				
				}
			
			}
			
			catch (Exception e) {
				
				System.err.println ("*** Error during ArrayList of L2 for IE Self Join");
				System.err.println (""+e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			
			}

			// Bitmap + Permutation initialization
			int N = _L1.size();

			bitmap = new int[N]; // Bitmap
			perm = new int[N]; // Permutation

			int i = 0;
			try {
				
				for (Tuple t : _L2) {
					
					int j = 0;
					for (Tuple s: _L1) {
						
						if (areSame(t, s)) {
							
							perm[i] = j;
							break;
							
						}
						j++;
						
					}
					bitmap[i] = 0;
					i++;
					
				}
				
			}
			
			catch (Exception e) {
				
				System.err.println ("Error : " + e);
				System.err.println ("*** Error during initialization of Bitmap and Permutation array for IE Self Join");
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
				
			}
				
			// Joining tuples from L1 
			Tuple join_tuple_1 = new Tuple();
			Tuple join_tuple_2 = new Tuple(); 
			join_tuple_1 = null;
			join_tuple_2 = null;
			
			// To get the equivalent index in permutation array of the tuple
			int index; 
			
			for (int _i = 0; _i < _L1.size() ; _i++) {
				// First we update the bitmap with the position array
				index = perm[_i] ;
				bitmap[index] = 1 ;
				
				// Then we loop to check for possible joins
				for (int j = index + 1; j < _L1.size() ; j++) {
					
					// We can join if the corresponding bit is 1
					if (bitmap[j] == 1) { 
						
							join_tuple_1 = _L1.get(j);
							join_tuple_2 = _L1.get(index);
							
						try {
							
							// We join and project
							Projection.Join(join_tuple_1, _in1, 
									join_tuple_2, _in1, 
									Jtuple, perm_mat, nOutFlds);
							
						} 
						
						catch (Exception e) {
							
							// TODO Auto-generated catch block
							e.printStackTrace();
							
						}
						
						// We add the result to the result relation
						Tuple jtuple = new Tuple(Jtuple);
						finalJoin.add(jtuple);
						
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
				
				rel_outer_1.close();
				rel_outer_2.close();
			}
			catch (Exception e) {
			
				throw new JoinsException(e, "IESelfJoin.java: error in closing iterator.");
			
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