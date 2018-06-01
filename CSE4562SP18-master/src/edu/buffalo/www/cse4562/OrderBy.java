package edu.buffalo.www.cse4562;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class OrderBy extends Tuple implements RelationalAlgebra
{
	ArrayList<ArrayList<PrimitiveValue>> otable = new ArrayList<ArrayList<PrimitiveValue>>();

	int orderByIndex_1, orderByIndex_2, tai =0;
	List<OrderByElement> element; 
	Tuple ne;
	public String subQuery_alias;
	@Override
	public boolean api(Tuple tupleobj) throws SQLException {

		
		otable.add(new ArrayList<PrimitiveValue>());
		orderByIndex_1 = tupleobj.colNames.indexOf((Column) element.get(0).getExpression());
		if(element.size() > 1)
			orderByIndex_2 = tupleobj.colNames.indexOf((Column) element.get(1).getExpression());
		else
			orderByIndex_2 = -1;

		for(int i = 0; i < tupleobj.tuple.size(); i++)
		{
			 otable.get(tai).add(tupleobj.tuple.get(i));  
			 
		}
		tai++;
			
		return true;
	}
	public void sortAndPrint(int ll) throws InvalidPrimitive
	{
         int c=0;
         //System.out.println("c:"+c);
         //System.out.println("orde:"+element.get(0));
		 //Collections.sort(otable, new CompareOverride(orderByIndex_1, orderByIndex_2, element.get(0).isAsc()));
		 
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
		

 	 
}
