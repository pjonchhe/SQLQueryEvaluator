package edu.buffalo.www.cse4562;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import net.sf.jsqlparser.statement.select.FromItem;

public class SelfJoin implements RelationalAlgebra{
	
	public FromItem fromitem;
	public List<Tuple> retTuple;
	int i=0,j=0,c_flag=0;
	
	public void open() throws IOException, SQLException
	{
		c_flag=0;
		Scan scan = new Scan();
		scan.fromitem = this.fromitem;
		scan.open();
		
		while(scan.hasNext())
		{
			retTuple.add(scan.retNext());
		}
	}
	
	public boolean api(Tuple tupleobj)
	{
		System.out.print("calling api");
		return true;

	}
	
	
	public Tuple retNext() throws SQLException
	 {
		Tuple t = new Tuple();
		for(;i<retTuple.size();i++)
		{
			for(;j<retTuple.size();j++)
			{
				if(i!=j)
				{
					t = retTuple.get(i);
					t.tuple.addAll(retTuple.get(j).tuple);
					t.colNames.addAll(retTuple.get(j).colNames);
					return t;
				}
				else
				{
					return null;
				}
			}
		}
		c_flag= 1;
		return null;
	 }

}