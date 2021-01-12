package iterator;

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

public class IEjoin extends Iterator {

	private AttrType      _in1[],  _in2[];
	private   int        in1_len, in2_len;
    
	private Iterator     outer;
	private Iterator     inner;
	private short   t1_str_sizes[];
	private   short t2_str_sizescopy[];
	private   CondExpr OutputFilter[];
	private   CondExpr RightFilter[];
	private   int        n_buf_pgs;        // # of buffer pages available.
	private   boolean        done;      // Is the join complete
	private   Tuple     outer_tuple, inner_tuple;
	private   Tuple     Jtuple;           // Joined tuple
	private   FldSpec   perm_mat[];
	private   int        nOutFlds;
	private   Heapfile  hf;
	private boolean Eq;
	private Sort L1;
	private Sort L2;
	private Sort L1prime;
	private Sort L2prime;
	private int N;
	private int M;
	private int[] P;
	private int[] P_prime;
	private int[] O_P1;
	private int[] O_P2;
	private int[] B;
	private ArrayList<Tuple> list1, list2, list1prime, list2prime, tuplesJoinList;

	public IEjoin( AttrType    in1[],    
			int     len_in1,           
			short   t1_str_sizes[],
			AttrType    in2[],         
			int     len_in2,           
			short   t2_str_sizes[],   
			int     amt_of_mem,        
			Iterator     outer1, 
			Iterator     outer2,
			Iterator     inner1, 
			Iterator     inner2,
			CondExpr outFilter[],      
			CondExpr rightFilter[],    
			FldSpec   proj_list[],
			int        n_out_flds
			) throws IOException,NestedLoopException
	{
		_in1 = new AttrType[in1.length];
		_in2 = new AttrType[in2.length];
		System.arraycopy(in1,0,_in1,0,in1.length);
		System.arraycopy(in2,0,_in2,0,in2.length);
		in1_len = len_in1;
		in2_len = len_in2;

		t2_str_sizescopy =  t2_str_sizes;
		inner_tuple = new Tuple();
		Jtuple = new Tuple();
		OutputFilter = outFilter;
		RightFilter  = rightFilter;
		
		outer =outer1;
		inner = inner1;

		n_buf_pgs    = amt_of_mem;
		done  = false;

		perm_mat = proj_list;
		nOutFlds = n_out_flds;
		
		// Jtypes for Join projection
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
		
		// Sorting array regarding needed columns with taking into account order depending on operator
		L1 = null;
		try {
			if (outFilter[0].op.attrOperator == AttrOperator.aopGT || outFilter[0].op.attrOperator == AttrOperator.aopGE) {
				TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
				L1 = new Sort (in1, (short) len_in1, t1_str_sizes,
						(iterator.Iterator) outer1, outFilter[0].operand1.symbol.offset, ascending, t1_str_sizes[0], amt_of_mem);
				L1prime = new Sort (in2, (short) len_in2, t2_str_sizes,
						(iterator.Iterator) inner1, outFilter[0].operand2.symbol.offset, ascending, t2_str_sizes[0], amt_of_mem);
			} else if (outFilter[0].op.attrOperator == AttrOperator.aopLT || outFilter[0].op.attrOperator == AttrOperator.aopLE)
			{
				TupleOrder descending = new TupleOrder(TupleOrder.Descending);
				L1 = new Sort (in1, (short) len_in1, t1_str_sizes,
						(iterator.Iterator) outer1, outFilter[0].operand1.symbol.offset, descending, t1_str_sizes[0], amt_of_mem);
				L1prime = new Sort (in2, (short) len_in2, t2_str_sizes,
						(iterator.Iterator) inner1, outFilter[0].operand2.symbol.offset, descending, t2_str_sizes[0], amt_of_mem);
			} else {
				System.out.println("BAD OPERATOR GIVEN");
			}


			L2 = null;
			if (outFilter[1].op.attrOperator == AttrOperator.aopGT || outFilter[1].op.attrOperator == AttrOperator.aopGE) {

				TupleOrder descending = new TupleOrder(TupleOrder.Descending);
				L2 = new Sort (in1, (short) len_in1, t1_str_sizes,
						(iterator.Iterator) outer2, outFilter[1].operand1.symbol.offset, descending, t1_str_sizes[0], amt_of_mem);
				L2prime = new Sort (in2, (short) len_in2, t2_str_sizes,
						(iterator.Iterator) inner2, outFilter[1].operand2.symbol.offset, descending, t2_str_sizes[0], amt_of_mem);
			} else if (outFilter[1].op.attrOperator == AttrOperator.aopLT || outFilter[1].op.attrOperator == AttrOperator.aopLE)
			{
				TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
				L2 = new Sort (in1, (short) len_in1, t1_str_sizes,
						(iterator.Iterator) outer2, outFilter[1].operand1.symbol.offset, ascending, t1_str_sizes[0], amt_of_mem);
				L2prime = new Sort (in2, (short) len_in2, t2_str_sizes,
						(iterator.Iterator) inner2, outFilter[1].operand2.symbol.offset, ascending, t2_str_sizes[0], amt_of_mem);
			} else {
				System.out.println("BAD OPERATOR GIVEN");
			}


			// Initiating tuples for sorted array regarding to needed columns
			Tuple tuple1p = new Tuple();
			Tuple tuple2p = new Tuple();
			Tuple tuple1 = new Tuple();
			Tuple tuple2 = new Tuple();
			// L1prime array created
			list1prime = new ArrayList<Tuple>();
			while ((tuple1p = L1prime.get_next()) != null)
			{	
				Tuple x = new Tuple(tuple1p); 
				list1prime.add(x);
			}
			// L2prime array created
			list2prime = new ArrayList<Tuple>();
			while ((tuple2p = L2prime.get_next()) != null)
			{	
				Tuple x = new Tuple(tuple2p);
				list2prime.add(x);
			}
			// L1 array created
			list1 = new ArrayList<Tuple>();
			while ((tuple1 = L1.get_next()) != null)
			{	
				Tuple x = new Tuple(tuple1);
				list1.add(x);
			}
			// L2 array created
			list2 = new ArrayList<Tuple>();
			while ((tuple2 = L2.get_next()) != null)
			{	
				Tuple x = new Tuple(tuple2);
				list2.add(x);
			}

			// size of T1 (L1 and L2) (east)
			int N = list1.size(); 
			// size of T2 (L1prime and L2prime) (west)
			int M = list1prime.size();

			P = new int[N]; //permutation arrays (L2 wrt L1)
			P_prime = new int[M]; //permutation arrays (L2prime wrt L1prime)
			O_P1 = new int[N]; //Offsets arrays (L1 wrt L1prime)
			O_P2 = new int[N]; //Offsets arrays (L2 wrt L2prime)
			B = new int[M]; // bit arrays (table T2)

			// Permutation array Table 2
			int i = 0;
			for (Tuple tp : list2prime) {
				int j = 0;
				for (Tuple sp: list1prime) {
					if (equalTuples(tp, sp)) {
						P_prime[i] = j;
						break;
					}
					j++;
				}
				B[i] = 0;
				i++;
			}

			// Permutation array Table 1
			int p = 0;
			for (Tuple t : list2) {
				int q = 0;
				for (Tuple s: list1) {
					if (equalTuples(t, s)) {
						P[p] = q;
						break;
					}
					q++;
				}
				p++;
			}

			// Offset array L1 - L1prime
			int y = 0;
			for (Tuple t1 : list1) {
				int x = 0;
				for (Tuple s1: list1prime) {
					if (t1.getIntFld(outFilter[0].operand1.symbol.offset) == s1.getIntFld(outFilter[0].operand2.symbol.offset)) {
						O_P1[y] = x;
						break;
					}
					x++;
				}
				y++;
			}

			// Offset array L2 - L2prime
			int v = 0;
			for (Tuple t2 : list2) {
				int u = 0;
				for (Tuple s2 : list2prime) {
					if (t2.getIntFld(outFilter[1].operand1.symbol.offset) == s2.getIntFld(outFilter[1].operand2.symbol.offset)) {
						O_P2[v] = u;
						break;
					}
					u++;
				}
				v++;
			}

			// Strict or equal inequality offset
			int offset;
			if ((outFilter[0].op.toString() == "aopGE" || outFilter[0].op.toString() == "aopLE") && 
					(outFilter[1].op.toString() == "aopGE" || outFilter[1].op.toString() == "aopLE")) {
				Eq = true;
				offset = 0;
			}else {
				Eq = false;
				offset = 1;
			}
			
			// List of joined tuple results
			tuplesJoinList = new ArrayList<Tuple>() ;

			Tuple tuplea = new Tuple();
			tuplea = null;
			Tuple tupleb = new Tuple();
			tupleb = null;

			int off2;
			int off1;
			for (int i1 = 0; i1 < list1.size() ; i1++) {
				off2 = O_P2[i1]; // Offset2
				for (int j1=0; j1 < Math.min(off2, list2.size()); j1++) {
					
					B[P_prime[j1]]=1; // Bit array filled
				}
				off1 = O_P1[P[i1]]; // Offset 1
				for (int k = off1 + offset ; k < list1prime.size() ; k++) {
					if (B[k] == 1) { // condition to join tuples to tuple j with Bit array
						
						tuplea = list1.get(i1);
						tupleb = list2prime.get(k);
						Projection.Join(tuplea, _in1, 
								tupleb, _in2, 
								Jtuple, perm_mat, nOutFlds);
						Tuple jtuple = new Tuple(Jtuple);
						tuplesJoinList.add(jtuple);
						
					}
				}
			}
			System.out.print("out");
		}
		catch (Exception e) {
			System.err.println ("*** Error preparing for nested_loop_join");
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

	}

	// Boolean method to test equality between 2 tuples
	public boolean equalTuples(Tuple T1, Tuple T2) throws FieldNumberOutOfBoundException, IOException {
		if (T1.getIntFld(1) != T2.getIntFld(1) &&
				T1.getIntFld(2) != T2.getIntFld(2) &&
				T1.getIntFld(3) != T2.getIntFld(3) &&
				T1.getIntFld(4) != T2.getIntFld(4)) {
			return false;
		}
		return true;
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
				inner.close();
			}catch (Exception e) {
				throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
			}
			closeFlag = true;
		}
	}
}