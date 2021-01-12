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

public class IESelfJoin extends Iterator {

	private AttrType      _in1[];
	private   int        in1_len;
	private   Iterator  outer;
	private   Iterator  outer2;
	private   short t2_str_sizescopy[];
	private   CondExpr OutputFilter[];
	private   CondExpr RightFilter[];
	private   int        n_buf_pgs;        // # of buffer pages available.
	private   boolean        done,         // Is the join complete
	get_from_outer;                 		// if TRUE, a tuple is got from outer
	private   Tuple     outer_tuple;
	private   Tuple     Jtuple;           // Joined tuple  
	private   FldSpec   perm_mat[];
	private   int        nOutFlds;
	private   Heapfile  hf;
	private boolean Eq;
	private int _conds;
	private Sort L1;
	private Sort L2;
	private int N;
	private int[] P;
	private int[] B;
	private ArrayList<Tuple> list, list2, tuplesJoinList;

	public IESelfJoin( AttrType    in1[],    
			int     len_in1,           
			short   t1_str_sizes[],   
			int     amt_of_mem,        
			Iterator     am1,
			Iterator     am2,      
			CondExpr outFilter[],      
			CondExpr rightFilter[],    
			FldSpec   proj_list[],
			int        n_out_flds,
			int conds // number of condition
			) throws IOException,NestedLoopException
	{
		_conds = conds;
		_in1 = new AttrType[in1.length];
		System.arraycopy(in1,0,_in1,0,in1.length);
		in1_len = len_in1;

		outer = am1;
		outer2 = am2;
		Jtuple = new Tuple();
		OutputFilter = outFilter;
		RightFilter  = rightFilter;

		n_buf_pgs    = amt_of_mem; // num pages for heapfile
		done  = false;
		get_from_outer = true;
		
		// Jtypes created for join projection of result tuples
		AttrType[] Jtypes = new AttrType[n_out_flds];

		short[] t_size;

		perm_mat = proj_list;
		nOutFlds = n_out_flds;
		try {
			t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
					in1, len_in1, in1, len_in1,
					t1_str_sizes, t1_str_sizes,
					proj_list, nOutFlds);
		}catch (TupleUtilsException e){
			throw new NestedLoopException(e, "TupleUtilsException is caught by IESelfJoins.java");
		}

		// Sorting outer with sort and result in iterator
		L1 = null;
		try {
			if (outFilter[0].op.attrOperator == AttrOperator.aopGT || outFilter[0].op.attrOperator == AttrOperator.aopGE) {
				TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
				L1 = new Sort (in1, (short) len_in1, t1_str_sizes,
						(iterator.Iterator) am1, outFilter[0].operand1.symbol.offset, ascending, t1_str_sizes[0], amt_of_mem);
				
			} else if (outFilter[0].op.attrOperator == AttrOperator.aopLT || outFilter[0].op.attrOperator == AttrOperator.aopLE)
			{
				TupleOrder descending = new TupleOrder(TupleOrder.Descending);
				L1 = new Sort (in1, (short) len_in1, t1_str_sizes,
						(iterator.Iterator) am1, outFilter[0].operand1.symbol.offset, descending, (short) 0, amt_of_mem);
			} else {
				System.out.println("BAD OPERATOR GIVEN");
			}
			// L1 array created
			Tuple tuple = new Tuple();
			tuple = null;
			list = new ArrayList<Tuple>();
			while ((tuple = L1.get_next()) != null)
			{	
				Tuple x = new Tuple(tuple); 
				list.add(x);
			}
			// Case of Two predicate Self Join
			if (conds == 2 ) {
				// inner table sorted to get L2 iterator
				L2 = null;
				if (outFilter[1].op.attrOperator == AttrOperator.aopGT || outFilter[1].op.attrOperator == AttrOperator.aopGE) {

					TupleOrder descending = new TupleOrder(TupleOrder.Descending);
					L2 = new Sort (in1, (short) len_in1, t1_str_sizes,
							(iterator.Iterator) am2, outFilter[1].operand1.symbol.offset, descending, t1_str_sizes[0], amt_of_mem);
				} else if (outFilter[1].op.attrOperator == AttrOperator.aopLT || outFilter[1].op.attrOperator == AttrOperator.aopLE)
				{
					TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
					L2 = new Sort (in1, (short) len_in1, t1_str_sizes,
							(iterator.Iterator) am2, outFilter[1].operand1.symbol.offset, ascending, t1_str_sizes[0], amt_of_mem);
				} else {
					System.out.println("BAD OPERATOR GIVEN");
				}
				
				// L2 array created
				list2 = new ArrayList<Tuple>();
				while ((tuple = L2.get_next()) != null)
				{	
					Tuple x = new Tuple(tuple);
					list2.add(x);
				}

				// Creating Permutation array and Bit array
				int N = 0;
				N = list.size();

				P = new int[N]; // Permutation array
				B = new int[N]; // Bit array
				
				// Permutation array filled and Bit array initialized with zeros
				int i = 0;
				for (Tuple t : list2) {
					int j = 0;
					for (Tuple s: list) {
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


			tuplesJoinList = new ArrayList<Tuple>() ;
			
			if (conds == 1) {
				// Single predicate IESelfJoin
				for (int i=0; i<list.size(); i++) {
					for (int j=0; j <= i-1; j++) {
						try {
							Tuple t1 = list.get(i);
							Tuple t2 = list.get(j);
							Projection.Join(t1, _in1, 
									t2, _in1, 
									Jtuple, perm_mat, nOutFlds);
	
							Tuple jtuple = new Tuple(Jtuple);
							tuplesJoinList.add(jtuple);

						} catch (FieldNumberOutOfBoundException e) {
							e.printStackTrace();
						}
					}
				}
				
			// Two predicates Self Join (algorithm from the paper)
			} else if (conds == 2){
				int pos; // position of ith tuple element of permutation list
				Tuple tuple1 = new Tuple(); // tuple to join
				tuple1 = null;
				Tuple tuple2 = new Tuple(); // tuple to join
				tuple2 = null;
				for (int i = 0; i < list.size() ; i++) {
					pos = P[i] ;
					B[pos] = 1 ; // Bit array filled
					for (int j = pos + 1; j < list.size() ; j++) {
						if (B[j] == 1) { // condition to join tuples to tuple j with Bit array 
								tuple1 = list.get(j);
								tuple2 = list.get(pos);
							Projection.Join(tuple1, _in1, 
									tuple2, _in1, 
									Jtuple, perm_mat, nOutFlds);
							Tuple jtuple = new Tuple(Jtuple);
							tuplesJoinList.add(jtuple);
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
		if (tuplesJoinList.size() > 0) {
			jt = tuplesJoinList.get(0);
			tuplesJoinList.remove(0);
		}
		return jt;
		
	} 
	
	// close function to close scan of heapfile
	public void close() throws IOException, JoinsException, SortException, IndexException {
		if (!closeFlag) {

			try {
				outer.close();
				outer2.close();
			}catch (Exception e) {
				throw new JoinsException(e, "IESelfJoin.java: error in closing iterator.");
			}
			closeFlag = true;
		}
	}
}