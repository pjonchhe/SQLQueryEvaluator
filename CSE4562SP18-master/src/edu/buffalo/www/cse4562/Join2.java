/*package edu.buffalo.www.cse4562;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.schema.Column;

public class Join2 extends RelationalAlgebra2{

	public Tuple current_left_tuple;
	@Override
	boolean api(Tuple tupleobj) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}
	
	
	@Override
	Tuple retNext() throws SQLException {
		
		Tuple tupleobj = new Tuple();
		
		Tuple rightTuple = new Tuple();			
		
		while (true)
		{
			
			if(current_left_tuple == null)
			{
				current_left_tuple = leftChild.retNext();
				if(current_left_tuple==null)
				{
					break;
				}
			}
 
			while((rightTuple=rightChild.retNext())!=null)
			{
				tupleobj.tuple.clear();
				tupleobj.colNames.clear();
				tupleobj.tuple.addAll(current_left_tuple.tuple);
				//tupleobj.colNames.addAll(current_left_tuple.colNames);
				tupleobj.tuple.addAll(rightTuple.tuple);
				//tupleobj.colNames.addAll(rightTuple.colNames);

				return tupleobj;
			}
			current_left_tuple = null;
			rightChild.reset();
		}
			
			
		return null;
		
		
	}
	
	@Override
	List<Column> open() throws IOException {
		
		List<Column> leftChildCols = new ArrayList<Column>();
		List<Column> rightChildCols = new ArrayList<Column>();
		
		leftChildCols = leftChild.open();
		rightChildCols = rightChild.open();
		colNamesChild.addAll(leftChildCols);
		colNamesChild.addAll(rightChildCols);
		colNamesParent.addAll(colNamesChild);
		return colNamesParent;
	}

	@Override
	void close() {
		// TODO Auto-generated method stub
		
	}


	@Override
	boolean hasNext() throws SQLException {
		return leftChild.hasNext();
	}


	@Override
	void reset() {
		leftChild.reset();
		
	}


}*/

package edu.buffalo.www.cse4562;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;

public class Join2 extends RelationalAlgebra2{

	public Tuple current_left_tuple;
	HashMap<String,  ArrayList<Tuple>> hashJoin;
	Integer init = 0;
	Iterator<Tuple> listItr = null;
	List<Tuple> tupleList;
	Column key;
	Column leftKey;
	boolean useHashJoin = false;
	List<Column> leftChildCols = new ArrayList<Column>();
	List<Column> rightChildCols = new ArrayList<Column>();
	
	@Override
	boolean api(Tuple tupleobj) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}
	
	
	@Override
	Tuple retNext() throws SQLException {
		
		if(this.parent instanceof Selection2)
		{
			Selection2 selObj = (Selection2)(this.parent);
			Expression condExp = selObj.expression;
			if(condExp instanceof EqualsTo)
			{
				if(init == 0)
				{
					Column leftExp = (Column)((BinaryExpression)condExp).getLeftExpression();
					Column rightExp = (Column)((BinaryExpression)condExp).getRightExpression();
					Tuple childTuple = new Tuple();	
					hashJoin = new HashMap<String, ArrayList<Tuple>>();
					
					
					String leftTableName = leftExp.getTable().getName();
					String rightTableName = rightExp.getTable().getName();
				//	String leftChildTableName = "",leftChildTableAliasName="";
					String rightChildTableName = "",rightChildTableAliasName="";
					
					if(this.leftChild instanceof Scan2)
					{
						Scan2 leftChild = (Scan2)(this.leftChild);
						FromItem table = leftChild.fromitem;
				//		leftChildTableName = ((Table)table).getName();
				//		leftChildTableAliasName = ((Table)table).getAlias();
					}
					else if(this.leftChild instanceof Selection2)
					{
						Selection2 leftChild = (Selection2)(this.leftChild);
						Expression expression = leftChild.expression;
				//		Column colName = (Column)((BinaryExpression)expression).getLeftExpression();
				//		leftChildTableName = colName.getTable().getName();
				//		leftChildTableAliasName = colName.getTable().getAlias();
					}
					
					if(this.rightChild instanceof Scan2)
					{
						Scan2 rightChild = (Scan2)(this.rightChild);
						FromItem table = rightChild.fromitem;
						rightChildTableName = ((Table)table).getName();
						rightChildTableAliasName = ((Table)table).getAlias();
					}
					else if(this.rightChild instanceof Selection2)
					{
						Selection2 rightChild = (Selection2)(this.rightChild);
						Expression expression = rightChild.expression;
						while(expression instanceof Column == false)
						{
							expression = ((BinaryExpression)expression).getLeftExpression();
						}
						Column colName = (Column)expression;
						rightChildTableName = colName.getTable().getName();
						rightChildTableAliasName = colName.getTable().getAlias();
					}
					
					//if(rightChildTableName.equals(rightTableName) || rightChildTableAliasName.equals(rightTableName))
					if(rightTableName != null)
					{
						if((childTables.contains(rightTableName) || childTableAliases.contains(rightTableName)) && ((childTables.indexOf(rightTableName) == childTables.size() - 1) || (childTableAliases.indexOf(rightTableName) == childTableAliases.size() - 1)))
						{
							key = rightExp;
							useHashJoin = true;
							leftKey = leftExp;
						}
						else
						{
							key = leftExp;
							useHashJoin = true;
							leftKey = rightExp;
						}
					}
					else
					{
						boolean found = false;
						for(int i = 0; i < leftChildCols.size(); i++)
						{
							Column temp = leftChildCols.get(i);
							if(temp.getColumnName().equals(leftExp.getColumnName()))
							{
								found = true;
								break;
							}
						}
						if(found)
						{
							key = rightExp;
							useHashJoin = true;
							leftKey = leftExp;
						}
						else
						{
							key = leftExp;
							useHashJoin = true;
							leftKey = rightExp;
						}
					}
					if(useHashJoin)
					{
						while((childTuple=rightChild.retNext())!=null)
						{
							Tuple retTuple = new Tuple();
							retTuple.tuple.addAll(childTuple.tuple);
							Eval eval = new Eval() {
								@Override
								public PrimitiveValue eval(Column arg0) throws SQLException {
									int index = rightChildCols.indexOf(arg0);
									if(index == -1)
									{
										int size = rightChildCols.size();
										for(int it = 0; it < size; it++)
										{
											if((arg0.getTable().getName().equalsIgnoreCase(rightChildCols.get(it).getTable().getAlias())) && 
													(arg0.getColumnName().equalsIgnoreCase(rightChildCols.get(it).getColumnName())))
											{
												index = it;
												break;
											}
										}
									}
									return retTuple.tuple.get(index);
								}
					
							};
							String colValKey = eval.eval(key).toString();
							if(hashJoin.containsKey(colValKey))
							{
								hashJoin.get(colValKey).add(retTuple);
								
							}
							else	
							{
								ArrayList<Tuple> tupleList = new ArrayList<Tuple>();
								tupleList.add(retTuple);
								hashJoin.put(colValKey, tupleList);
							}
						}
					}
					init = 1;
				}
			}
		}
		
		Tuple tupleobj = new Tuple();
		
		Tuple rightTuple = new Tuple();			
		
		if(useHashJoin)
		{
			while (true)
			{
				
				if(current_left_tuple == null)
				{
					current_left_tuple = leftChild.retNext();
					if(current_left_tuple==null)
					{
						break;
					}
					Eval eval = new Eval() {
						@Override
						public PrimitiveValue eval(Column arg0) throws SQLException {
							int index = leftChildCols.indexOf(arg0);
							if(index == -1)
							{
								int size = leftChildCols.size();
								for(int it = 0; it < size; it++)
								{
									if((arg0.getTable().getName().equalsIgnoreCase(leftChildCols.get(it).getTable().getAlias())) && 
											(arg0.getColumnName().equalsIgnoreCase(leftChildCols.get(it).getColumnName())))
									{
										index = it;
										break;
									}
								}
							}
							return current_left_tuple.tuple.get(index);
						}
			
					};
					String colValKey = eval.eval(leftKey).toString();
					tupleList = new ArrayList<Tuple>();
					tupleList = hashJoin.get(colValKey);
					if(tupleList != null)
						listItr = tupleList.iterator();
					
				}
	 
			//	while(listItr!=null && (rightTuple=listItr.next())!=null)
				while(listItr!=null && listItr.hasNext())
				{
					rightTuple=listItr.next();
					tupleobj.tuple.clear();
					tupleobj.colNames.clear();
					tupleobj.tuple.addAll(current_left_tuple.tuple);
					//tupleobj.colNames.addAll(current_left_tuple.colNames);
					tupleobj.tuple.addAll(rightTuple.tuple);
					//tupleobj.colNames.addAll(rightTuple.colNames);
	
					return tupleobj;
				}
				current_left_tuple = null;
				rightChild.reset();
			}
		}
		
		else
		{
			while(true)
			{
				if(current_left_tuple == null)
				{
					current_left_tuple = leftChild.retNext();
					if(current_left_tuple==null)
					{
						break;
					}
				}
	 
				while((rightTuple=rightChild.retNext())!=null)
				{
					tupleobj.tuple.clear();
					tupleobj.colNames.clear();
					tupleobj.tuple.addAll(current_left_tuple.tuple);
					//tupleobj.colNames.addAll(current_left_tuple.colNames);
					tupleobj.tuple.addAll(rightTuple.tuple);
					//tupleobj.colNames.addAll(rightTuple.colNames);

					return tupleobj;
				}
				current_left_tuple = null;
				rightChild.reset();
			
			}
		}
			
			
		return null;
		
		
	}
	
	@Override
	List<Column> open() throws IOException {
		
		leftChildCols = leftChild.open();
		rightChildCols = rightChild.open();
		colNamesChild.addAll(leftChildCols);
		colNamesChild.addAll(rightChildCols);
		colNamesParent.addAll(colNamesChild);
		return colNamesParent;
	}

	@Override
	void close() {
		// TODO Auto-generated method stub
		
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
