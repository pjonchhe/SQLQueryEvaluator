package edu.buffalo.www.cse4562;
import java.io.IOException;
import java.sql.SQLException;

import net.sf.jsqlparser.statement.select.PlainSelect;

public class ScanPlainSelect  extends Tuple implements RelationalAlgebra 
{
   
   public PlainSelect query;
   public RelTreeObj[] treebounds = new RelTreeObj[2];
   String subSelctAlias="";
   
   public void open() throws IOException
   {
	   
	  // treebounds = Main.createTree(this.query, this.subSelctAlias);
   }

   
   public boolean api(Tuple tupleobj)
   {
 	  String s1="scanning";
 	 // System.out.println(s1);
 	  return true;
   }
   
   public Tuple retNext()
   {
	      
	  try {
		return ParseTree(this.treebounds[1]);
	} catch (IOException | SQLException e) {
		// TODO Auto-generated catch block
		
		e.printStackTrace();
		return null;
	}
	
	   
  }
   
   public void reset()
   {
	   RelTreeObj leafnode = this.treebounds[1];
	   if(leafnode.getOperator() instanceof Scan)
		{
			Scan table = (Scan)leafnode.getOperator();
			if(table.isOpen)
			{
				table.isOpen = false;
			}
		}
   }
   
   private Tuple ParseTree(RelTreeObj leafnode) throws IOException, SQLException
	{		

		if(leafnode.getOperator() instanceof Scan)
		{
			Scan table = (Scan)leafnode.getOperator();
			if(!table.isOpen)
			{
				table.open();
			}
			
			Tuple tupleobj = new Tuple();
			RelTreeObj parentnode = null;
			
			if(table.hasNext())
			{
				tupleobj = table.retNext();
				parentnode = leafnode.retParent();
				
				while(parentnode != null)
				{
					
					if(parentnode.operator.api(tupleobj)==true)
					{
				     
					 parentnode = parentnode.retParent();
					 
					}
					else
					{
						if(table.hasNext())
						{
							tupleobj = table.retNext();
						}
						//break;
					}
				}
				
				
			}
			
			return tupleobj;
		}
		else
		{
			Join join = (Join)leafnode.getOperator();
			Tuple tupleobj = new Tuple();
			RelTreeObj parentnode = null;

			
			if(join.api(tupleobj))
			{
				parentnode = leafnode.retParent();
				
				//added join 3 table join
				if(parentnode.getOperator() instanceof Join)
				{
					Join innode  = (Join)parentnode.getOperator();
					innode.current_left_tuple = tupleobj;
					parentnode.operator = (RelationalAlgebra)innode;
					tupleobj = ParseTree(parentnode);
					return tupleobj;	
					
				}
				
				while(parentnode != null)
				{
					
					if(parentnode.operator.api(tupleobj)==true)
					{

						 parentnode = parentnode.retParent();
					}
					else
					{
						tupleobj.tuple.clear();
						//return null;
						break;
					}
				}
				
			}
			return tupleobj;	
		}
		
	}
}
