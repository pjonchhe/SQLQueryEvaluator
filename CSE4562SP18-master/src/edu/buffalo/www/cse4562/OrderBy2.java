package edu.buffalo.www.cse4562;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class OrderBy2 extends RelationalAlgebra2
{

	ArrayList<ArrayList<PrimitiveValue>> otable = new ArrayList<ArrayList<PrimitiveValue>>();

	int  tai =0;
	int orderByIndex_1, orderByIndex_2;
	List<Integer> orderByIndices = new ArrayList<Integer>();
	List<OrderByElement> element; 
	Tuple ne;
	public String subQuery_alias;	
	 

	@Override
	boolean api(Tuple tupleobj) throws SQLException {
		
		return false;
	}

	@Override
	List<Column> open() throws IOException {
		colNamesChild = leftChild.open();
		colNamesParent.addAll(colNamesChild);
		
		for(int i=0;i<element.size();i++)
		{
			orderByIndices.add(colNamesChild.indexOf((Column) element.get(i).getExpression()));
		}
		return colNamesParent;
	}

	@Override
	void close() {
		
	}

	@Override
	Tuple retNext() throws SQLException {

	    Tuple tupleobj = leftChild.retNext();
		if(tupleobj!=null)
		{
		   	
			otable.add(new ArrayList<PrimitiveValue>());

			for(int i = 0; i < tupleobj.tuple.size(); i++)
			{
				 otable.get(tai).add(tupleobj.tuple.get(i));  
				 
			}
			tai++;
				
		// System.out.println("OOO:"+otable);	
		}
		return tupleobj;
	}
	public void sortAndPrint(int ll) throws InvalidPrimitive
	{
         int c=0;

		 Collections.sort(otable, new CompareOverride(orderByIndices, element.get(0).isAsc()));
		 
		 for(int i=0;i<otable.size();i++)
		 {
		     if(c==ll) {break;}
			 for(int j = 0; j < otable.get(i).size() - 1; j++)
			 {
				System.out.print(otable.get(i).get(j) + "|");
			 }
			 System.out.println(otable.get(i).get(otable.get(i).size() - 1));
			 c++;
		 }
	}

	@Override
	boolean hasNext() throws SQLException {
		return false;
	}

	@Override
	void reset() {
		
	}

 	 
}
