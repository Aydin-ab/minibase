package iterator;
import iterator.*;
import java.util.Stack;

import java.io.IOException;

import bufmgr.PageNotReadException;
import global.AttrOperator;
import global.AttrType;
import global.RID;
import global.TupleOrder;
import heap.FieldNumberOutOfBoundException;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;
import index.IndexException;
import java.util.ArrayList;
import java.util.ListIterator;


/**
* JoinsDriver for the IEJoins assignment of lab3 of DBSys course. 
* 
*/
public class IESelfJoin extends Iterator {

	private AttrType      _in1[];
	private   int        in1_len;
	private   Iterator  rel_outer_1;
	private   Iterator  rel_outer_2;
	private   CondExpr OutputFilter[];
	private   int        n_buf_pgs;        // # of buffer pages available.
	private   Tuple     Jtuple;           // Joined tuple  
	private   FldSpec   perm_mat[];
	private   int        nOutFlds;
	// My parameters
	private Sort L1;
	private Sort L2;
	private ArrayList<Tuple> _L1; // ArrayList of L1
	private ArrayList<Tuple> _L2; // ArrayList of L2
	private ArrayList<Tuple> finalJoin; // ArrayList of the final join list
	private int[] P; // Permutation array
	private int[] B; // Bitmap array

	
	  /**
	   * Constructor of the IESelfJoin.
	   * 
	   *Initialize the two relations which are joined, including relation type
	   *
	   *@param in1  AttrType[] : list containing field types of R.
	   *@param _in1_len int ; # of columns in R.
	   *@param t1_str_sizes short[] : list shows the length of the string fields.
	   *@param amt_of_mem  int : IN PAGES. To be fixed in the main of the test
	   *@param am1  Iterator : access method for left i/p to join
	   *@param am2  Iterator : CLONE access method for left i/p to join
	   *@param _outputFilter CondExpr[] : select expressions
	   *@param proj_list FldSpec[] : shows what input fields go where in the output tuple
	   *@param n_out_flds int : number of outer relation fields
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
						FldSpec[]   	proj_list,
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
		
		// Jtypes created for join projection of result tuples
		AttrType[] Jtypes = new AttrType[n_out_flds];

		short[] t_size;

		perm_mat = proj_list;
		nOutFlds = n_out_flds;
		try {
			t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
					in1, in1_len, in1, in1_len,
					t1_str_sizes, t1_str_sizes,
					proj_list, nOutFlds);
		}catch (TupleUtilsException e){
			throw new NestedLoopException(e, "TupleUtilsException is caught by IESelfJoins.java");
		}
		
		
		// Now we sort (I believe it's more logical to sort in the constructor than in a get_next()
		// Because get_next() should stick to its literal name and just give the next tuple
		// It's more intuitive to think that the sort is being done in the construction

		// Outer
		L1 = null;
		// Now we sort the outer
		try {
			if (OutputFilter[0].op.attrOperator == AttrOperator.aopLT || OutputFilter[0].op.attrOperator == AttrOperator.aopLE)
			{
				TupleOrder descending = new TupleOrder(TupleOrder.Descending);
				L1 = new Sort (in1, (short) in1_len, t1_str_sizes,
						(iterator.Iterator) am1, OutputFilter[0].operand1.symbol.offset, descending, (short) 0, n_buf_pgs);
			}
			else if (OutputFilter[0].op.attrOperator == AttrOperator.aopGT || OutputFilter[0].op.attrOperator == AttrOperator.aopGE) {
				TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
				L1 = new Sort (in1, (short) in1_len, t1_str_sizes,
						(iterator.Iterator) am1, OutputFilter[0].operand1.symbol.offset, ascending, t1_str_sizes[0], n_buf_pgs);
				
			} else {
				System.out.println("BAD OPERATOR GIVEN");
			}
			// L1 array created
			Tuple tuple = new Tuple();
			tuple = null;
			_L1 = new ArrayList<Tuple>();
			while ((tuple = L1.get_next()) != null)
			{	
				Tuple x = new Tuple(tuple); 
				_L1.add(x);
			}
			// Case of Two predicate Self Join
			if (num_pred == 2 ) {
				// inner table sorted to get L2 iterator
				L2 = null;
				if (OutputFilter[1].op.attrOperator == AttrOperator.aopGT || OutputFilter[1].op.attrOperator == AttrOperator.aopGE) {

					TupleOrder descending = new TupleOrder(TupleOrder.Descending);
					L2 = new Sort (in1, (short) in1_len, t1_str_sizes,
							(iterator.Iterator) am2, OutputFilter[1].operand1.symbol.offset, descending, t1_str_sizes[0], n_buf_pgs);
				} else if (OutputFilter[1].op.attrOperator == AttrOperator.aopLT || OutputFilter[1].op.attrOperator == AttrOperator.aopLE)
				{
					TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
					L2 = new Sort (in1, (short) in1_len, t1_str_sizes,
							(iterator.Iterator) am2, OutputFilter[1].operand1.symbol.offset, ascending, t1_str_sizes[0], n_buf_pgs);
				} else {
					System.out.println("BAD OPERATOR GIVEN");
				}
				
				// L2 array created
				_L2 = new ArrayList<Tuple>();
				while ((tuple = L2.get_next()) != null)
				{	
					Tuple x = new Tuple(tuple);
					_L2.add(x);
				}

				// Creating Permutation array and Bit array
				int N = 0;
				N = _L1.size();

				P = new int[N]; // Permutation array
				B = new int[N]; // Bit array
				
				// Permutation array filled and Bit array initialized with zeros
				int i = 0;
				for (Tuple t : _L2) {
					int j = 0;
					for (Tuple s: _L1) {
						if (equalTuples(t, s)) {
							P[i] = j;
							break;
						}
						j++;
					}
					B[i] = 0;
					i++;
				}
			}


			finalJoin = new ArrayList<Tuple>() ;
			
			if (num_pred == 1) {
				// Single predicate IESelfJoin
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

						} catch (FieldNumberOutOfBoundException e) {
							e.printStackTrace();
						}
					}
				}
				
			// Two predicates Self Join (algorithm from the paper)
			} else if (num_pred == 2){
				int pos; // position of ith tuple element of permutation list
				Tuple tuple1 = new Tuple(); // tuple to join
				tuple1 = null;
				Tuple tuple2 = new Tuple(); // tuple to join
				tuple2 = null;
				for (int i = 0; i < _L1.size() ; i++) {
					pos = P[i] ;
					B[pos] = 1 ; // Bit array filled
					for (int j = pos + 1; j < _L1.size() ; j++) {
						if (B[j] == 1) { // condition to join tuples to tuple j with Bit array 
								tuple1 = _L1.get(j);
								tuple2 = _L1.get(pos);
							Projection.Join(tuple1, _in1, 
									tuple2, _in1, 
									Jtuple, perm_mat, nOutFlds);
							Tuple jtuple = new Tuple(Jtuple);
							finalJoin.add(jtuple);
						}
					}
				}
				System.out.print("out");
			} else {
				System.out.println("Too many conditions");
			}
		}
		catch (Exception e) {
			System.err.println ("*** Error preparing for IE Self Join");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

	}
	
	// Boolean method to test equality between 2 tuples
	public boolean equalTuples(Tuple T1, Tuple T2) throws FieldNumberOutOfBoundException, IOException {
		if (T1.getIntFld(1) == T2.getIntFld(1) &&
				T1.getIntFld(2) == T2.getIntFld(2) &&
				T1.getIntFld(3) == T2.getIntFld(3) &&
				T1.getIntFld(4) == T2.getIntFld(4)) {
			return true;
		}
		return false;
	}
	
	// Backup Function for join for debugging
	public void join(Tuple T1, Tuple T2, Tuple jtuple) throws FieldNumberOutOfBoundException, IOException {
		jtuple.setIntFld(1, T1.getIntFld(perm_mat[0].offset));
		jtuple.setIntFld(2, T2.getIntFld(perm_mat[1].offset));
	}

	// Function get_next to get the next result tuple of the algorithm IESelfJoin
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
	InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
	LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		
		Tuple jt = null;
		if (finalJoin.size() > 0) {
			jt = finalJoin.get(0);
			finalJoin.remove(0);
		}
		return jt;
		
	} 
	
	// close function to close scan of heapfile
	public void close() throws IOException, JoinsException, SortException, IndexException {
		if (!closeFlag) {

			try {
				rel_outer_1.close();
				rel_outer_2.close();
			}catch (Exception e) {
				throw new JoinsException(e, "IESelfJoin.java: error in closing iterator.");
			}
			closeFlag = true;
		}
	}
}