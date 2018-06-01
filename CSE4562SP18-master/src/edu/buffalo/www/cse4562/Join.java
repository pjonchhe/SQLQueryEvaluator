package edu.buffalo.www.cse4562;

import java.io.IOException;
import java.sql.SQLException;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

public class Join implements RelationalAlgebra{

	public RelationalAlgebra node1, node2;
	public Tuple current_left_tuple;
	@Override
	public boolean api(Tuple tupleobj) throws SQLException {
		// TODO Auto-generated method stub
		if(node1 instanceof Scan && node2 instanceof Scan)
		{
 
			Scan scan1 = (Scan)node1;
			Scan scan2  = (Scan)node2;
			Tuple rightTuple = new Tuple();			
			
			while ((current_left_tuple != null) || scan1.hasNext())
			{
				//System.out.println("this: " +scan1.hasNext());
				if(current_left_tuple == null)
				{
				//	System.out.println("aaglu ayu");
					current_left_tuple = scan1.retNext();
				//	System.out.println("aaglu ayur : " +current_left_tuple.record.toString());
				}
 
				while(scan2.hasNext())
				{
					//System.out.println("aaglu ret ayu");
					rightTuple = scan2.retNext();
				//	System.out.println("aaglu ret ayu : "+ rightTuple.record.toString());
					tupleobj.tuple.clear();
					tupleobj.colNames.clear();
					tupleobj.tuple.addAll(current_left_tuple.tuple);
					tupleobj.colNames.addAll(current_left_tuple.colNames);
					tupleobj.tuple.addAll(rightTuple.tuple);
					tupleobj.colNames.addAll(rightTuple.colNames);
					if(scan2.testing == true)
					{
						Eval eval = new Eval() {

							@Override
							public PrimitiveValue eval(Column arg0) throws SQLException {
							//	System.out.println("ts:"+t.tuple+" andarg : "+arg0);
								// TODO Auto-generated method stub
								//String columnName = arg0.getColumnName();
								//String lowercolname = columnName.toLowerCase();
								//int index = sl.indexOf(columnName);
								//int index = t.columnNames.indexOf(lowercolname);
								int index = tupleobj.colNames.indexOf(arg0);
								//below code changes is add to handle alias case. In case of alias, arg0's table name has the alias. So table name needs to be compared with alias.
								if(index == -1)
								{
									int size = tupleobj.colNames.size();
									for(int it = 0; it < size; it++)
									{
										//System.out.print(" "+t.colNames.get(it).getTable().getAlias());
										//System.out.print("x"+arg0.getColumnName());
										//System.out.println(" "+t.colNames.get(it).getColumnName());
										//System.out.println(arg0.getColumnName().equalsIgnoreCase(t.colNames.get(it).getColumnName()));
										if((arg0.getTable().getName().equalsIgnoreCase(tupleobj.colNames.get(it).getTable().getAlias())) && 
										   (arg0.getColumnName().equalsIgnoreCase(tupleobj.colNames.get(it).getColumnName())))
										{
											index = it;
											break;
										}
									}
									
								}
							//	System.out.println("te:"+t.tuple+"index:"+index);
								if(index > -1)
								{
									return tupleobj.tuple.get(index);
								}
								else
								{
									return null;
								}
								
							}
						 };
						 PrimitiveValue cond = eval.eval(scan2.expression);
						 if(cond.toBool() == true)
							 return true;
						 else
						 {
							 continue;
						 }
					}
					return true;
				}
				current_left_tuple = null;
				try {
					scan2.reset();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				};
			}
		}
		else if(node1 instanceof Join && node2 instanceof Scan)
		{
			//Scan scan1 = (Scan)node1;
			Scan scan2  = (Scan)node2;
			Tuple rightTuple = new Tuple();

 
				while(scan2.hasNext())
				{
					//System.out.println("aaglu ret ayu");
					rightTuple = scan2.retNext();
				//	System.out.println("aaglu ret ayu : "+ rightTuple.record.toString());
					tupleobj.tuple.clear();
					tupleobj.colNames.clear();
					tupleobj.tuple.addAll(current_left_tuple.tuple);
					tupleobj.colNames.addAll(current_left_tuple.colNames);
					tupleobj.tuple.addAll(rightTuple.tuple);
					tupleobj.colNames.addAll(rightTuple.colNames);
					return true;
				}
				//current_left_tuple = null;
				try {
					scan2.reset();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				};
			
		}
		else if((node1 instanceof Join && node2 instanceof ScanPlainSelect))
		{
			
			ScanPlainSelect scan2  = (ScanPlainSelect)node2;
			Tuple rightTuple = new Tuple();

 
				while(true)
				{
					rightTuple = scan2.retNext();
					if(rightTuple.tuple.isEmpty())
					{
						break;
					}
					tupleobj.tuple.clear();
					tupleobj.colNames.clear();
					tupleobj.tuple.addAll(current_left_tuple.tuple);
					tupleobj.colNames.addAll(current_left_tuple.colNames);
					tupleobj.tuple.addAll(rightTuple.tuple);
					tupleobj.colNames.addAll(rightTuple.colNames);
					return true;
				}
				scan2.reset();
			
			
			
		}
		else if(node1 instanceof ScanPlainSelect && node2 instanceof Scan)
		{
			Scan scan1 = (Scan)node2;
			ScanPlainSelect scan2  = (ScanPlainSelect)node1;
			Tuple rightTuple = new Tuple();
			
			
			
			
			while ((current_left_tuple != null) || scan1.hasNext())
			{
				//System.out.println("this: " +scan1.hasNext());
				if(current_left_tuple == null)
				{
				//	System.out.println("aaglu ayu");
					current_left_tuple = scan1.retNext();
				//	System.out.println("aaglu ayur : " +current_left_tuple.record.toString());
				}
 
				while(true)
				{
					//System.out.println("aaglu ret ayu");
					rightTuple = scan2.retNext();
					if(rightTuple.tuple.isEmpty())
					{
						break;
					}
				//	System.out.println("aaglu ret ayu : "+ rightTuple.record.toString());
					tupleobj.tuple.clear();
					tupleobj.colNames.clear();
					tupleobj.tuple.addAll(current_left_tuple.tuple);
					tupleobj.colNames.addAll(current_left_tuple.colNames);
					tupleobj.tuple.addAll(rightTuple.tuple);
					tupleobj.colNames.addAll(rightTuple.colNames);
					return true;
				}
				current_left_tuple = null;
				scan2.reset();;
			}
		}
		return false;
	}

}
