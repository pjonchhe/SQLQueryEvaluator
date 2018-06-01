package edu.buffalo.www.cse4562;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.schema.Column;

public class CompareOverride implements Comparator<ArrayList<PrimitiveValue>> {
	List<Integer> orderByIndices = new ArrayList<Integer>();
	boolean isAsc;
	PrimitiveValue v1,v2,result_prim = null;
	Boolean r=null;
	String temp_v1, temp_v2;
	int result_int,ret_result,cnt;
	
	Eval eval = new Eval() {

		@Override
		public PrimitiveValue eval(Column arg0) throws SQLException {
			return null;
		}};


	public CompareOverride(List<Integer> orderByInd, boolean isAsc) {
		this.orderByIndices = orderByInd;
		this.isAsc = isAsc;

	} 
	@Override

	public int compare(ArrayList<PrimitiveValue> arg0, ArrayList<PrimitiveValue> arg1) {	
		
		cnt=0;
		while(cnt<orderByIndices.size())
		{
			v1=arg0.get(orderByIndices.get(cnt));
			v2=arg1.get(orderByIndices.get(cnt));
			
			if(v1 instanceof StringValue)
			{
				r=true;
				temp_v1 = v1.toString();
				temp_v2 = v2.toString();
				result_int = temp_v1.compareTo(temp_v2);
				if(isAsc)
				{
					ret_result = result_int;
				}
				else
				{
					ret_result = result_int * (-1);
				}
			}
			else
			{
				try 
				{
					result_prim = eval.eval( new GreaterThan(v1,v2));
					r = result_prim.toBool();
				} 
				catch (SQLException e1) 
				{
					e1.printStackTrace();
				}
				
				if(isAsc)
				{
					if(r == false)
					{
						ret_result = -1;
					}
					else
					{
						ret_result =  1;
					}
				}
				else
				{
					if(r == false)
					{
						ret_result = 1;
					}
					else
					{
						ret_result = -1;
					}
				}
				
				
			}
			
			if(result_int != 0 || r != true)
			{
				break;
			}

			cnt++;
		}
		
		return ret_result;
	}
 }
