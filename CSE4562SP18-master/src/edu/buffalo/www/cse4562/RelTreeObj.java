package edu.buffalo.www.cse4562;
 
import java.util.ArrayList;
import java.util.List;
 
public class RelTreeObj {
	
	private RelTreeObj parent = null;
	public RelationalAlgebra operator = null;
	
	private List<RelTreeObj> child = new ArrayList<>();
 
	
	public RelationalAlgebra getOperator() 
	{
		return operator;
	}
	
	public RelTreeObj(RelationalAlgebra operator) 
	{
		this.operator = operator;
	}
 
	public RelTreeObj attachChild(RelTreeObj childop) 
	{
		childop.parent=this;
		this.child.add(childop);
		return childop;
	}
 
	public RelTreeObj retParent() 
	{
		return parent;
	}
	
	
}