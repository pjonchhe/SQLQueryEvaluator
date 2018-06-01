/*package edu.buffalo.www.cse4562;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.schema.Column;

public class Aggregate2 extends RelationalAlgebra2 {
	
	List<Column> groupByColumns = new ArrayList<Column>();
	List<Function> aggrFunctions = new ArrayList<Function>();
	List<Integer> groupByIndex = new ArrayList<Integer>();
	List<Integer> functionIndex = new ArrayList<Integer>(); 
	HashMap<String,  ArrayList<Tuple>> hashAggr;
//	HashMap<String,  Long> hashSum=new HashMap<>();
	Iterator<String> hashItr;
	String groupByColVals="";
	Integer init=0,aggrTupleSent=0;

	@Override
	boolean api(Tuple tupleobj) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	List<Column> open() throws IOException {
		colNamesChild = leftChild.open();
		if(this.groupByColumns!=null)
		{
			for(int i=0;i<colNamesChild.size();i++)
			{
				for(int j=0;j<groupByColumns.size();j++)
				{
					if(colNamesChild.get(i).getColumnName().equalsIgnoreCase(groupByColumns.get(j).getColumnName()))
					{
						groupByIndex.add(i);
					}
				}
			}
		}
		return colNamesParent;
	}

	@Override
	void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	Tuple  retNext() throws SQLException {
		if(this.groupByColumns!=null)
		{
			if(init==0)
			{
				Tuple childTuple = new Tuple();	
				hashAggr=new HashMap<String, ArrayList<Tuple>>();
				 while((childTuple=leftChild.retNext())!=null)
				 {
					Tuple retTuple = new Tuple();
					retTuple.tuple.addAll(childTuple.tuple);
					for(int i=0;i<groupByIndex.size();i++)
					{
						groupByColVals = groupByColVals+retTuple.tuple.get(groupByIndex.get(i))+"||";
					}
					
					if(hashAggr.containsKey(groupByColVals))
					{
						hashAggr.get(groupByColVals).add(retTuple);
						
					}
					else	
					{
						ArrayList<Tuple> tupleList = new ArrayList<Tuple>();
						tupleList.add(retTuple);
						hashAggr.put(groupByColVals, tupleList);
					}
		
					groupByColVals="";
					
				}
			init=1;
			hashItr = hashAggr.keySet().iterator();
			}
			String keyVal="";
			List<Tuple> groupByTuples =new ArrayList<Tuple>();
			while(hashItr.hasNext())
			{
				keyVal = hashItr.next();
				groupByTuples = hashAggr.get(keyVal);
				//List<PrimitiveValue> aggrResults = new ArrayList<PrimitiveValue>();
				Tuple retTuple=new Tuple();
				
				for(int i=0;i<colNamesParent.size();i++)
				{
					if(groupByIndex.contains(i))
					{
						retTuple.tuple.add(groupByTuples.get(0).tuple.get(i));
					}
					else
					{
						if(aggrFunctions.get(functionIndex.indexOf(i)).getName().equalsIgnoreCase("sum"))
						{
							retTuple.tuple.add(getSumAggr(groupByTuples,functionIndex.get(functionIndex.indexOf(i))));
						}
						else if(aggrFunctions.get(functionIndex.indexOf(i)).getName().equalsIgnoreCase("avg"))
						{
							retTuple.tuple.add(getAvgAggr(groupByTuples,functionIndex.get(functionIndex.indexOf(i))));
						}
					}
				}
					
				
							
				
				return retTuple;
				
			}
			return null;
		}
		else
		{
			if(aggrTupleSent == 0)
			{
				Tuple recTuple = new Tuple();
				Tuple retTuple = new Tuple();
				int init =0,aggrIndex;
				List<Object> aggrValues = new ArrayList<Object>();
				List<Integer> aggrTypes = new ArrayList<Integer>(); // 0 for long and 1 for double
				while((recTuple = leftChild.retNext())!=null)
				{
					for(int i=0;i<aggrFunctions.size();i++)
					{
						aggrIndex = functionIndex.get(i);
						if(aggrFunctions.get(i).getName().equalsIgnoreCase("sum"))
						{
							
							if(init==0)
							{
								PrimitiveValue val = recTuple.tuple.get(aggrIndex);
								if(val instanceof LongValue)
								{
									aggrValues.add(recTuple.tuple.get(aggrIndex).toLong());
									aggrTypes.add(0);
								}
								else if(val instanceof DoubleValue)
								{
									aggrValues.add(recTuple.tuple.get(aggrIndex).toDouble());
									aggrTypes.add(1);
								}
								
								
							}
							else
							{
								
								if(aggrTypes.get(i)==0)
								{
									long longVal = (long)aggrValues.get(i) + recTuple.tuple.get(aggrIndex).toLong();
									aggrValues.set(i, longVal);
								}
								else
								{
									double doubleVal = ((double) aggrValues.get(i)) + recTuple.tuple.get(aggrIndex).toDouble();
									aggrValues.set(i, doubleVal);
								}
							}
	
						}
						
					}
					init=1;
				}
				
				aggrTupleSent=1;
			//	retTuple.tuple.addAll((Collection<? extends PrimitiveValue>) aggrValues);
				PrimitiveValue d;
				for(int i=0;i<aggrValues.size();i++)
				{
					if(aggrTypes.get(i)==0)
					{
						d = new LongValue((long) aggrValues.get(i));
					}
					else
					{
						d = new DoubleValue((double) aggrValues.get(i));
					}
					retTuple.tuple.add(d);
				}
				
				
				return retTuple;
			}
			else
			{
				return null;
			}
		}
	}

	@Override
	boolean hasNext() throws SQLException {
		return false; //since it is a blocking operator
	}

	@Override
	void reset() {
		leftChild.reset();
		
	}
	
	PrimitiveValue getSumAggr(List<Tuple> allTuples,Integer aggrIndex) throws SQLException
	{
		Eval eval = new Eval() {
			public PrimitiveValue eval(Column arg0) throws SQLException {
				return null;
			}

		};
		
		Iterator<Tuple> tupleItr = allTuples.iterator();
		PrimitiveValue retLong = new LongValue(0);
		PrimitiveValue retDouble = new DoubleValue(0.0);
		int retType=-1; // 0 for long and 1 for double
		while(tupleItr.hasNext())
		{
			PrimitiveValue val = tupleItr.next().tuple.get(aggrIndex);
			if(val instanceof LongValue)
			{
				Expression add = new Addition(retLong,val);
				retLong = eval.eval(add);
				
				retType=0;
			}
			else if(val instanceof DoubleValue)
			{
				Expression add = new Addition(retDouble,val);
				retDouble = eval.eval(add);
				
				retType=1;
			}
			
		}
		
		if(retType==0)
		{
			return retLong;
		}
		else
		{
			return retDouble;
		}
		
	}
	PrimitiveValue getAvgAggr(List<Tuple> allTuples,Integer aggrIndex) throws InvalidPrimitive
	{
		Iterator<Tuple> tupleItr = allTuples.iterator();
		long longSum=0;
		double doubleSum=0.0; 
		int retType=-1; // 0 for long and 1 for double
		int cnt = allTuples.size();
		while(tupleItr.hasNext())
		{
			PrimitiveValue val = tupleItr.next().tuple.get(aggrIndex);
			if(val instanceof LongValue)
			{
				longSum=longSum+val.toLong();
				retType=0;
			}
			else if(val instanceof DoubleValue)
			{
				doubleSum = doubleSum+val.toDouble();
				retType=1;
			}
			
		}
		
		if(retType==0)
		{
			PrimitiveValue d = new LongValue(longSum/cnt);
			return d;
		}
		else
		{
			PrimitiveValue d = new DoubleValue(doubleSum/cnt);
			return d;
		}
		
	}

}
*/





package edu.buffalo.www.cse4562;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.schema.Column;

public class Aggregate2 extends RelationalAlgebra2 {
	
	List<Column> groupByColumns = new ArrayList<Column>();
	List<Function> aggrFunctions = new ArrayList<Function>();
	List<Integer> groupByIndex = new ArrayList<Integer>();
	List<Integer> functionIndex = new ArrayList<Integer>(); 
	HashMap<String,Tuple> hashAggr;
	HashMap<String,Integer> aggrKeyCnt;
//	HashMap<String,  Long> hashSum=new HashMap<>();
	Iterator<String> hashItr;
	String groupByColVals="";
	Integer init=0,aggrTupleSent=0;
	Eval eval = new Eval() {
		public PrimitiveValue eval(Column arg0) throws SQLException {
			return null;
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
		if(this.groupByColumns!=null)
		{
			for(int i=0;i<colNamesChild.size();i++)
			{
				for(int j=0;j<groupByColumns.size();j++)
				{
					if(colNamesChild.get(i).getColumnName().equalsIgnoreCase(groupByColumns.get(j).getColumnName()))
					{
						groupByIndex.add(i);
					}
				}
			}
		}
		return colNamesParent;
	}

	@Override
	void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	Tuple  retNext() throws SQLException {
		if(this.groupByColumns!=null)
		{
			if(init==0)
			{
				Tuple childTuple;
				hashAggr=new HashMap<String, Tuple>();
				aggrKeyCnt = new HashMap<String, Integer>();
				while((childTuple=leftChild.retNext())!=null)
				{
					for(int i=0;i<groupByIndex.size();i++)
					{
						groupByColVals = groupByColVals+childTuple.tuple.get(groupByIndex.get(i))+"||";
					}
					
					if(hashAggr.containsKey(groupByColVals))
					{
						
						computeAllStreamAggr(hashAggr.get(groupByColVals),childTuple);
						hashAggr.put(groupByColVals, childTuple);
						aggrKeyCnt.put(groupByColVals, aggrKeyCnt.get(groupByColVals)+1);
						
					}
					else	
					{
						hashAggr.put(groupByColVals, childTuple);
						aggrKeyCnt.put(groupByColVals, 1);
					}
		
					groupByColVals="";
				}
				init=1;
				hashItr = hashAggr.keySet().iterator();
				
				
			}
			String keyVal="";
			//List<Tuple> groupByTuples =new ArrayList<Tuple>();
			int avgCnt=0;
			Tuple retTuple;
			PrimitiveValue val;
			while(hashItr.hasNext())
			{
				keyVal = hashItr.next();
				avgCnt = aggrKeyCnt.get(keyVal);
				
				retTuple = hashAggr.get(keyVal);
				for(int i=0;i<aggrFunctions.size();i++)
				{
					if(aggrFunctions.get(i).getName().equalsIgnoreCase("avg"))
					{
						val = retTuple.tuple.get(functionIndex.get(i));
						Expression divide = new Division(val,new LongValue(avgCnt));
						retTuple.tuple.set(functionIndex.get(i), eval.eval(divide)) ;
					}
					
				}
				
				return retTuple;				
			}
			return null;
		}
		else
		{
			if(aggrTupleSent == 0)
			{
				Tuple recTuple = new Tuple();
				Tuple retTuple = new Tuple();
				int init =0,aggrIndex;
				List<Object> aggrValues = new ArrayList<Object>();
				List<Integer> aggrTypes = new ArrayList<Integer>(); // 0 for long and 1 for double
				while((recTuple = leftChild.retNext())!=null)
				{
					for(int i=0;i<aggrFunctions.size();i++)
					{
						aggrIndex = functionIndex.get(i);
						if(aggrFunctions.get(i).getName().equalsIgnoreCase("sum"))
						{
							
							if(init==0)
							{
								PrimitiveValue val = recTuple.tuple.get(aggrIndex);
								if(val instanceof LongValue)
								{
									aggrValues.add(recTuple.tuple.get(aggrIndex).toLong());
									aggrTypes.add(0);
								}
								else if(val instanceof DoubleValue)
								{
									aggrValues.add(recTuple.tuple.get(aggrIndex).toDouble());
									aggrTypes.add(1);
								}
								
								
							}
							else
							{
								
								if(aggrTypes.get(i)==0)
								{
									long longVal = (long)aggrValues.get(i) + recTuple.tuple.get(aggrIndex).toLong();
									aggrValues.set(i, longVal);
								}
								else
								{
									double doubleVal = ((double) aggrValues.get(i)) + recTuple.tuple.get(aggrIndex).toDouble();
									aggrValues.set(i, doubleVal);
								}
							}
	
						}
						
					}
					init=1;
				}
				
				aggrTupleSent=1;
			//	retTuple.tuple.addAll((Collection<? extends PrimitiveValue>) aggrValues);
				PrimitiveValue d;
				for(int i=0;i<aggrValues.size();i++)
				{
					if(aggrTypes.get(i)==0)
					{
						d = new LongValue((long) aggrValues.get(i));
					}
					else
					{
						d = new DoubleValue((double) aggrValues.get(i));
					}
					retTuple.tuple.add(d);
				}
				
				
				return retTuple;
			}
			else
			{
				return null;
			}
		}
	}

	@Override
	boolean hasNext() throws SQLException {
		return false; //since it is a blocking operator
	}

	@Override
	void reset() {
		leftChild.reset();
		
	}
	
	PrimitiveValue getSumAggr(List<Tuple> allTuples,Integer aggrIndex) throws SQLException
	{
		Eval eval = new Eval() {
			public PrimitiveValue eval(Column arg0) throws SQLException {
				return null;
			}

		};
		
		Iterator<Tuple> tupleItr = allTuples.iterator();
		PrimitiveValue retLong = new LongValue(0);
		PrimitiveValue retDouble = new DoubleValue(0.0);
		int retType=-1; // 0 for long and 1 for double
		while(tupleItr.hasNext())
		{
			PrimitiveValue val = tupleItr.next().tuple.get(aggrIndex);
			if(val instanceof LongValue)
			{
				Expression add = new Addition(retLong,val);
				retLong = eval.eval(add);
				
				retType=0;
			}
			else if(val instanceof DoubleValue)
			{
				Expression add = new Addition(retDouble,val);
				retDouble = eval.eval(add);
				
				retType=1;
			}
			
		}
		
		if(retType==0)
		{
			return retLong;
		}
		else
		{
			return retDouble;
		}
		
	}
	PrimitiveValue getAvgAggr(List<Tuple> allTuples,Integer aggrIndex) throws InvalidPrimitive
	{
		Iterator<Tuple> tupleItr = allTuples.iterator();
		long longSum=0;
		double doubleSum=0.0; 
		int retType=-1; // 0 for long and 1 for double
		int cnt = allTuples.size();
		while(tupleItr.hasNext())
		{
			PrimitiveValue val = tupleItr.next().tuple.get(aggrIndex);
			if(val instanceof LongValue)
			{
				longSum=longSum+val.toLong();
				retType=0;
			}
			else if(val instanceof DoubleValue)
			{
				doubleSum = doubleSum+val.toDouble();
				retType=1;
			}
			
		}
		
		if(retType==0)
		{
			PrimitiveValue d = new LongValue(longSum/cnt);
			return d;
		}
		else
		{
			PrimitiveValue d = new DoubleValue(doubleSum/cnt);
			return d;
		}
		
	}
	
	void computeAllStreamAggr(Tuple aggrTuple, Tuple nextTuple) throws SQLException //computes sum and avg functions
	{
		int aggrIndex=0;
		PrimitiveValue val;
		
		for(int i=0;i<aggrFunctions.size();i++)
		{
			aggrIndex = functionIndex.get(i);
			//if(aggrFunctions.get(i).getName().equalsIgnoreCase("avg") || aggrFunctions.get(i).getName().equalsIgnoreCase("sum"))
			//{
				
				val = aggrTuple.tuple.get(aggrIndex);
				val = eval.eval(new Addition(val,nextTuple.tuple.get(aggrIndex)));
				nextTuple.tuple.set(aggrIndex, val);
				
			//}
						
		}		
		
	}

}
