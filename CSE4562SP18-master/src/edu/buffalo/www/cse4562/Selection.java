package edu.buffalo.www.cse4562;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
public class Selection extends Tuple implements RelationalAlgebra 
{
  public Expression expression;	

  public boolean api(Tuple tupleobj) throws SQLException
  {
	  //String s1="selection";
	  //System.out.println(s1);
	  List<String> sl = new ArrayList<String>();
	  //int ts=tupleobj.columnNames.size();
	  int ts = tupleobj.colNames.size();
	  for(int j=0;j<ts;j++)
	  {
		//String tt= tupleobj.columnNames.get(j);
		String tt = tupleobj.colNames.get(j).getColumnName();
		sl.add(tt);
	  }
	  
	  Eval eval = new Eval() {
		  @Override
		  public PrimitiveValue eval(Column arg0) throws SQLException {
			  // TODO Auto-generated method stub
			  //String columnName = arg0.getColumnName();
			  //String lowercolname = columnName.toLowerCase();
			  //int index = sl.indexOf(columnName);
			  //int index = tupleobj.columnNames.indexOf(lowercolname);
			  int index = tupleobj.colNames.indexOf(arg0);
			  if(index == -1)
				{
					int size = tupleobj.colNames.size();
					//System.out.println("s:"+size);
					for(int it = 0; it < size; it++)
					{
	                      //System.out.print(arg0.getTable().getName()); 
	                      //System.out.print(" "+tupleobj.colNames.get(it).getTable());///.getAlias());	
	                      //System.out.print(" x "+arg0.getColumnName()); 
	                      //System.out.println(" "+tupleobj.colNames.get(it).getColumnName());
						if((arg0.getTable().getName().equalsIgnoreCase(tupleobj.colNames.get(it).getTable().getAlias())) && 
						   (arg0.getColumnName().equalsIgnoreCase(tupleobj.colNames.get(it).getColumnName())))
						{
							index = it;
							break;
						}
					}
				}
			return tupleobj.tuple.get(index);
			}
		  
		    @Override
		    public PrimitiveValue eval(InExpression arg0) throws SQLException {
				
				PrimitiveValue leftExpVal= eval(arg0.getLeftExpression());
				 //System.out.println("k:"+leftExpVal); 
				if(arg0.getItemsList() instanceof ItemsList)
				{	

			    	ExpressionList itemvalues = (ExpressionList) arg0.getItemsList();
			    	
			    	if(itemvalues.getExpressions().contains((Expression)leftExpVal))
			    	{
			    		return BooleanValue.TRUE;
			    	}
			    	else
			    	{
			    		return BooleanValue.FALSE;
			    	}
			    	
				}
				else if(arg0.getItemsList() instanceof SubSelect)
				{
					//not implemented
					return BooleanValue.TRUE;
				}
				else
				{
					return BooleanValue.TRUE;
				}
				
			}
		  
		  };
	  //System.out.println("exp:"+expression);	  
	  PrimitiveValue type = eval.eval(expression);
	  return type.toBool();
  }
}
