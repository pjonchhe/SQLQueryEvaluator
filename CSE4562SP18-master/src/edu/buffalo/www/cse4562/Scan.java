package edu.buffalo.www.cse4562;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItem;

public class Scan  extends Tuple implements RelationalAlgebra 
{
   
   public FromItem fromitem;
   Reader reader=null;
   public String tablename;
   CSVParser parser=null;
   CreateTable create=new CreateTable();
   Tuple tupleobj = null;
   Iterator<CSVRecord> tupplelist = null;
   public boolean isOpen = false;
   public Expression expression;
   public boolean testing =false;
   
   public void open() throws IOException
   {
	   
	   tablename = ((Table) fromitem).getName();
	   reader = Files.newBufferedReader(Paths.get("D://Eclipse//dbb//CSE4562SP18//"+tablename+".csv"));
	   parser = CSVParser.parse(reader, CSVFormat.DEFAULT.withDelimiter('|'));
	   CreateTable temp = Main.map.get(tablename.toLowerCase());
	   List<ColumnDefinition> temp_colDef = new ArrayList<ColumnDefinition>();
	   temp_colDef = temp.getColumnDefinitions();
	   create.setColumnDefinitions(temp_colDef);
	   create.setIndexes(temp.getIndexes());
	   Table temp_table = new Table(temp.getTable().getName());
	   create.setTable(temp_table);
	   create.setTableOptionsStrings(temp.getTableOptionsStrings());
	   create.getTable().setAlias(fromitem.getAlias());
	   tupleobj = new Tuple();
	   tupplelist = parser.iterator();
	   
	 //  System.err.println("size : " +parser.getRecords().size());
	  // System.out.println("size11 : " +parser.getRecords().size());
	   isOpen = true;
   }
   
   public void reset() throws IOException
   {
	   reader = Files.newBufferedReader(Paths.get("D://Eclipse//dbb//CSE4562SP18//\"+tablename+\".csv"));
	   parser = CSVParser.parse(reader, CSVFormat.DEFAULT.withDelimiter('|'));
	   tupplelist = parser.iterator(); 
	   isOpen = false;
   }
   
   boolean hasNext()
   {
	   return tupplelist.hasNext();
   }
	
   public boolean api(Tuple tupleobj)
   {
 	  String s1="scanning";
 	 // System.out.println(s1);
 	  return true;
   }
   
   public Tuple retNext() throws SQLException
   {
	      
	   if(tupplelist.hasNext())
	   {
		//Expression left = expression.
		CSVRecord tupple = tupplelist.next();
		tupleobj.record = tupple;
		tupleobj.tuple.clear();
		//tupleobj.columnNames.clear();
		tupleobj.colNames.clear();
		//System.out.println("coldef:"+create.getColumnDefinitions());
		int numColumns = create.getColumnDefinitions().size();
		for(int i = 0; i < numColumns; i++)
		{
			//String dataType = create.getColumnDefinitions().get(i).getColDataType().toString();
			ColDataType dataType = create.getColumnDefinitions().get(i).getColDataType();
			String colName = create.getColumnDefinitions().get(i).getColumnName();
			String lowercolname = colName.toLowerCase();
				//System.out.println("l:"+dataType);
			//tupleobj.columnNames.add(lowercolname);
			Column tempCol = new Column(create.getTable(), colName);
			tupleobj.colNames.add(tempCol);
			
			if(dataType.getDataType().equals("INTEGER"))
			{
				//System.out.println("fdffd555");
				String temp = tupple.get(i);
				PrimitiveValue d = new LongValue(Long.valueOf(temp));
				tupleobj.tuple.add(d);
			}
			else if(dataType.getDataType().equals("INT"))
			{
				//System.out.println("fdffd555");
				String temp = tupple.get(i);
				PrimitiveValue d = new LongValue(Long.valueOf(temp));
				tupleobj.tuple.add(d);
			}
			else if(dataType.getDataType().equals("STRING"))
			{
				String temp = tupple.get(i);
				PrimitiveValue d = new StringValue(temp);
				tupleobj.tuple.add(d);
			}
			else if(dataType.getDataType().equals("DATE"))
			{
				//System.out.println("gonr");
				String temp = tupple.get(i);
				PrimitiveValue d = new DateValue(temp);
				//System.out.println("d");
				tupleobj.tuple.add(d);
			}
			else if(dataType.getDataType().equals("VARCHAR"))
			{
				String temp = tupple.get(i);
				PrimitiveValue d = new StringValue(temp);
				tupleobj.tuple.add(d);
			}
			else if(dataType.getDataType().equals("CHAR"))
			{
				String temp = tupple.get(i);
				PrimitiveValue d = new StringValue(temp);
				tupleobj.tuple.add(d);
			}
			else if(dataType.getDataType().equals("DOUBLE"))
			{
				String temp = tupple.get(i);
				PrimitiveValue d = new DoubleValue(temp);
				tupleobj.tuple.add(d);
			}
			else
			{
				//System.out.println("dffddf");
				//int err = 3/0;
				
			}
		
		tupleobj.table = create;
		//System.out.println("InScan_retNext:"+tupleobj.table.getColumnDefinitions());
		//System.out.println("InSCan:"+fromitem.getAlias());
		tupleobj.table.getTable().setAlias(fromitem.getAlias());//Changes 3/15 ////////////
		
		//System.out.println("In_Scann:"+tupleobj.table.getTable().getAlias());
   
   }
	/*	for(int i = 0; i < tupleobj.tuple.size() - 1; i++)
		{
			System.err.print(tupleobj.tuple.get(i) + "|");
		}
		///System.out.println("not_going");
		System.err.println(tupleobj.tuple.get(tupleobj.tuple.size() - 1));
		*/
		if(testing == true)
		{

			AndExpression exp = (AndExpression)expression;
			GreaterThan left = (GreaterThan) ((AndExpression)(exp.getLeftExpression())).getRightExpression();
			GreaterThan right = (GreaterThan)exp.getRightExpression();
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
			 
			 PrimitiveValue type_left = eval.eval(left);
			 PrimitiveValue type_right = eval.eval(right);
			  
			if((type_left != null && !type_left.toBool()) || (type_right !=null && !type_right.toBool()))
			{
				if(tupplelist.hasNext())
				{
					tupleobj=retNext();
				}
				else
				{
					return tupleobj;
				}
			}
		
		}
   return tupleobj;
   }
   else
   {
	   return null;
   }
	   
  }
}
