package edu.buffalo.www.cse4562;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class Projection2 extends RelationalAlgebra2{

	public List<SelectItem> projection;
	public String subQuery_alias="";
	Tuple t;
	
	Eval eval = new Eval() {

		@Override
		public PrimitiveValue eval(Column arg0) throws SQLException {
			int index = colNamesChild.indexOf(arg0);
			//below code changes is add to handle alias case. In case of alias, arg0's table name has the alias. So table name needs to be compared with alias.
			if(index == -1)
			{
				int size = colNamesChild.size();
				for(int it = 0; it < size; it++)
				{
					if((arg0.getTable().getName().equalsIgnoreCase(colNamesChild.get(it).getTable().getAlias())) && 
							(arg0.getColumnName().equalsIgnoreCase(colNamesChild.get(it).getColumnName())))
					{
						index = it;
						break;
					}
				}

			}
			return t.tuple.get(index);

		}
	};

	@Override
	boolean api(Tuple tupleobj) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	List<Column> open() throws IOException {
		colNamesChild = leftChild.open();
		colNamesParent.addAll(colNamesChild);
		List<Column> tempColumnNames = new ArrayList<Column>();
		int ps= projection.size();	
		for(int j=0;j<ps;j++)
		{	
			SelectItem i = projection.get(j);
			if(i instanceof SelectExpressionItem)
			{
				// System.out.println("selexpr");
				SelectExpressionItem k = (SelectExpressionItem)i;
				String alias = k.getAlias();
				Expression expr = k.getExpression();
				// System.out.println("sel :"+expr);
				//PrimitiveValue type = eval.eval(expr);
				//tempTuple.add(type);
				if(alias != null)
				{
					//need to modify schema;
					//int test = 0;
					Column col = new Column();
					col.setColumnName(alias);
					Table tab_temp = new Table();
					col.setTable(tab_temp);
					tempColumnNames.add((Column)col);
				}
				else 
				{
					if(expr instanceof Column)
					{
						tempColumnNames.add((Column)expr);
					}
					else
					{
						Column col = new Column();
						col.setColumnName(expr.toString());
						Table tab_temp = new Table();
						col.setTable(tab_temp);
						tempColumnNames.add((Column)col);
					}
					
				}
			}
			else if(i instanceof AllColumns )
			{
				//	 System.out.println("saalc");
				//tempTuple.addAll(t.tuple);
				tempColumnNames.addAll(colNamesChild);
			}
			else if (i instanceof AllTableColumns)
			{
				//	 System.out.println("alltabc");
				AllTableColumns tab = (AllTableColumns) i;
				Table tab_name = tab.getTable();
				int numCols = colNamesChild.size();
				for(int ind = 0; ind < numCols; ind++)
				{

					// System.out.println("inside for");
					if(colNamesChild.get(ind).getTable().getAlias() != null)
					{
						// System.out.println("pehif+ pec : "+tab_name.getName()+" and tabname +"+ t.colNames.get(ind).getTable().getName()+" and ali+"+t.colNames.get(ind).getTable().getAlias());

						if((colNamesChild.get(ind).getTable().getName().equalsIgnoreCase(tab_name.getName())) || (colNamesChild.get(ind).getTable().getAlias().equalsIgnoreCase(tab_name.getName())))
						{

							// System.out.println("beef");
							//PrimitiveValue type = eval.eval(colNamesChild.get(ind));
							//tempTuple.add(type);
							Table temp_tab = new Table(colNamesChild.get(ind).getTable().getName());
							temp_tab.setAlias(colNamesChild.get(ind).getTable().getAlias());
							Column temp = new Column(temp_tab, colNamesChild.get(ind).getColumnName());
							tempColumnNames.add(temp);

						}
					}

				}	   
			}
		}
		if(subQuery_alias != null && !subQuery_alias.isEmpty()) //Changes 3/15
		{ 
			int size = tempColumnNames.size();
			for(int i = 0; i < size; i++)
			{

				//System.out.println("Alias set+"+tempColumnNames.get(i).getTable()+" and subQuery_alias + "+ subQuery_alias);
				tempColumnNames.get(i).getTable().setAlias(subQuery_alias);
			}
		}
		colNamesParent.clear();
		colNamesParent.addAll(tempColumnNames);
		return colNamesParent;
	}

	@Override
	void close() {
		// TODO Auto-generated method stub

	}

	@Override
	Tuple retNext() throws SQLException {
		int ps= projection.size();
		t = leftChild.retNext();
		if(t == null)
		{
			return null;
		}
		int ts = colNamesChild.size();
		List<String> sl = new ArrayList<String>();
		List<PrimitiveValue> tempTuple = new ArrayList<PrimitiveValue>();
		for(int j=0;j<ts;j++)
		{
			String tt = colNamesChild.get(j).getColumnName();
			sl.add(tt);
		}

		
		for (int j = 0; j < ps; j++)
		{
			SelectItem i = projection.get(j);
			if(i instanceof SelectExpressionItem)
			{
				SelectExpressionItem k = (SelectExpressionItem)i;

				Expression expr = k.getExpression();
				PrimitiveValue type = eval.eval(expr);

				tempTuple.add(type);
			}
			else if(i instanceof AllColumns )
			{
				tempTuple.addAll(t.tuple);
			}
			else if (i instanceof AllTableColumns)
			{
				//	 System.out.println("alltabc");
				AllTableColumns tab = (AllTableColumns) i;
				Table tab_name = tab.getTable();
				int numCols = colNamesChild.size();
				for(int ind = 0; ind < numCols; ind++)
				{

					// System.out.println("inside for");
					if(colNamesChild.get(ind).getTable().getAlias() != null)
					{
						// System.out.println("pehif+ pec : "+tab_name.getName()+" and tabname +"+ t.colNames.get(ind).getTable().getName()+" and ali+"+t.colNames.get(ind).getTable().getAlias());

						if((colNamesChild.get(ind).getTable().getName().equalsIgnoreCase(tab_name.getName())) || (colNamesChild.get(ind).getTable().getAlias().equalsIgnoreCase(tab_name.getName())))
						{
							// System.out.println("beef");
							PrimitiveValue type = eval.eval(colNamesChild.get(ind));
							tempTuple.add(type);
						}
					}

				}
			}

		}


		t.tuple.clear();
		t.tuple.addAll(tempTuple);

		return t;
	}

	@Override
	boolean hasNext() throws SQLException {
		return leftChild.hasNext();
	}
	
	@Override
	void reset() {
		leftChild.reset();
		
	}


}
