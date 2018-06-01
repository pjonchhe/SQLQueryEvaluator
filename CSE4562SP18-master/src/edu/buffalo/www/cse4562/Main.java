package edu.buffalo.www.cse4562; 
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
 public class Main {
	
	//static CreateTable create;
	static HashMap<String, CreateTable> map = new HashMap<>();
    static int l=-1;
	static int c=0;
	static int f=0;
	
	private static void ParseTree(RelationalAlgebra2 root) throws IOException, SQLException
	{		
		Tuple tupleobj;
		//System.out.println("R:"+root.getClass());
		root.open();
		OrderBy2 od;//= new OrderBy2();
		if(!(root instanceof OrderBy2))
		{
			tupleobj = root.retNext();
			//System.out.println("Type:"+tupleobj.record);
			while(tupleobj != null)
			{
				
				if(c==l)
				{
					break;
				}
				for(int i = 0; i < tupleobj.tuple.size() - 1; i++)
				{
					System.out.print(tupleobj.tuple.get(i) + "|");
				}
				System.out.println(tupleobj.tuple.get(tupleobj.tuple.size() - 1));
				c++;
				tupleobj = root.retNext();
			}
			
		}
		else
		{
			tupleobj = root.retNext();
			//System.out.println("Type:"+tupleobj.record);
			while(tupleobj != null)
			{
				
				//if(c==l)
				//{
					//break;
				//}
				//for(int i = 0; i < tupleobj.tuple.size() - 1; i++)
				//{
					//System.out.print(tupleobj.tuple.get(i) + "|");
				//}
				//System.out.println(tupleobj.tuple.get(tupleobj.tuple.size() - 1));
				//c++;
				//System.out.println("Completed");
				tupleobj = root.retNext();
			}
			od=(OrderBy2)root;
			od.sortAndPrint(l);
			//System.out.println("c:"+root.getClass());
		}
	}
	
	
	public static void selectionOptimize(RelationalAlgebra2 root)
	{
		RelationalAlgebra2 iterate = root;
		while(iterate != null)
		{
			if(iterate instanceof Selection2)
			{
				Selection2 selNode = (Selection2)iterate;
				if(selNode.expression instanceof AndExpression)
				{
					List<Expression> expList = new ArrayList<Expression>();
					createCondList((AndExpression)selNode.expression, expList);
					for(int i = 0; i < expList.size(); i++)
					{
						RelationalAlgebra2 op = new Selection2();
						Selection2 op1 = (Selection2)op;
						op1.expression = expList.get(i);
						op = (RelationalAlgebra2)op1;
						op.parent = null;
						op.leftChild = null;
						op.rightChild = null;
						
						selOptParseTree(root, op);
					}
				}				
			}
			iterate = iterate.leftChild;
		}
	}
	
	public static void selOptParseTree(RelationalAlgebra2 root, RelationalAlgebra2 op)
	{
		Expression exp = ((Selection2)op).expression;
		if(exp instanceof BinaryExpression)
		{
			BinaryExpression binExp = (BinaryExpression)exp;
			if(binExp.getLeftExpression() instanceof Column && binExp.getRightExpression() instanceof Column)
			{
				traverseTreeJoin(root, op, null, true);
			}
			else
			{
				traverseTreeScan(root, op, null, true);
			}
		}
	}
	
	public static boolean traverseTreeScan(RelationalAlgebra2 root, RelationalAlgebra2 op, RelationalAlgebra2 parent, boolean left)
	{
		boolean found = false;
		if(root == null)
			return false;
		if(root instanceof Scan2)
		{
			Scan2 temp = (Scan2)root;
			Selection2 selObj = (Selection2)op;
			FromItem table = temp.fromitem;
			String tableName = ((Table)table).getName();
			String tableAlias = ((Table)table).getAlias();
			Expression exp = selObj.expression;
			while(exp instanceof Column == false)
			{
				exp = ((BinaryExpression)exp).getLeftExpression();
			}
			Column leftExp = (Column)exp;
			String expTableName = leftExp.getTable().getName();
			if((expTableName != null && tableName.equals(expTableName)) || (tableAlias != null && tableAlias.equals(expTableName)))
			{
				found = true;
				op.parent = root.parent;
				op.leftChild = root;
				if(left)
					parent.leftChild = op;
				else
					parent.rightChild = op;
				root.parent = op;
				return true;
			}
		}
		
		if(!found && root.leftChild != null)
			found = traverseTreeScan(root.leftChild,op, root, true);
		
		if(!found && root.rightChild != null)
			found = traverseTreeScan(root.rightChild,op, root, false);
		
		return found;
	}
	
	public static boolean traverseTreeJoin(RelationalAlgebra2 root, RelationalAlgebra2 op, RelationalAlgebra2 parent, boolean left)
	{				
		boolean found = false;
		if(root == null)
			return false;
		

		if(!found && root.leftChild != null)
			found = traverseTreeJoin(root.leftChild,op, root, true);
		
		if(!found && root.rightChild != null)
			found = traverseTreeJoin(root.rightChild,op, root, false);
		
		if(!found && root instanceof Join2)
		{
			Selection2 selObj = (Selection2)op;
			Join2 joinObj = (Join2)root;
			
			Expression exp = selObj.expression;
			Column leftExp = (Column)((BinaryExpression)exp).getLeftExpression();
			Column rightExp = (Column)((BinaryExpression)exp).getRightExpression();
			String leftTableName = leftExp.getTable().getName();
			String rightTableName = rightExp.getTable().getName();
			
			if((joinObj.childTables.contains(leftTableName) || ((joinObj.childTableAliases != null) && (joinObj.childTableAliases.contains(leftTableName)))) &&
				(joinObj.childTables.contains(rightTableName) || ((joinObj.childTableAliases != null) && (joinObj.childTableAliases.contains(rightTableName)))))
			{
				found = true;
				op.parent = root.parent;
				op.leftChild = root;
				if(left)
					parent.leftChild = op;
				else
					parent.rightChild = op;
				root.parent = op;
				return true;
			}
		}
		
		return found;
	}
	
	/*public static boolean traverseTreeJoin(RelationalAlgebra2 root, RelationalAlgebra2 op, RelationalAlgebra2 parent, boolean left)
	{				
		boolean found = false;
		if(root == null)
			return false;
		if(root instanceof Join2)
		{
			Selection2 selObj = (Selection2)op;
			
			Expression exp = selObj.expression;
			Column leftExp = (Column)((BinaryExpression)exp).getLeftExpression();
			Column rightExp = (Column)((BinaryExpression)exp).getRightExpression();
			String leftTableName = leftExp.getTable().getName();
			String rightTableName = rightExp.getTable().getName();
			String leftChildTableName = "";
			String rightChildTableName = "";
			String leftChildTableAlias = "";
			String rightChildTableAlias = "";
			
			if(root.leftChild instanceof Scan2)
			{
				Scan2 leftChild = (Scan2)(root.leftChild);
				FromItem table = leftChild.fromitem;
				leftChildTableName = ((Table)table).getName();
				leftChildTableAlias = ((Table)table).getAlias();
				
			}
			else if(root.leftChild instanceof Selection2)
			{
				Selection2 leftChild = (Selection2)(root.leftChild);
				Expression expression = leftChild.expression;
				Column colName = (Column)((BinaryExpression)expression).getLeftExpression();
				leftChildTableName = colName.getTable().getName();
				leftChildTableAlias = colName.getTable().getAlias();
			}
			
			if(root.rightChild instanceof Scan2)
			{
				Scan2 rightChild = (Scan2)(root.rightChild);
				FromItem table = rightChild.fromitem;
				rightChildTableName = ((Table)table).getName();
				rightChildTableAlias = ((Table)table).getAlias();
			}
			else if(root.rightChild instanceof Selection2)
			{
				Selection2 rightChild = (Selection2)(root.rightChild);
				Expression expression = rightChild.expression;
				while(expression instanceof Column == false)
				{
					expression = ((BinaryExpression)expression).getLeftExpression();
				}
				Column colName = (Column)expression;
				rightChildTableName = colName.getTable().getName();
				rightChildTableAlias = colName.getTable().getAlias();
			}
			
			if((((leftChildTableName.equals(leftTableName)) || (leftChildTableName.equals(rightTableName))) || 
					((leftChildTableAlias != null) && ((leftChildTableAlias.equals(leftTableName)) || (leftChildTableAlias.equals(rightTableName))))) &&
					(((rightChildTableName.equals(leftTableName)) || (rightChildTableName.equals(rightTableName))) || 
					((rightChildTableAlias != null) && ((rightChildTableAlias.equals(leftTableName)) || (rightChildTableAlias.equals(rightTableName))))))
			{
				found = true;
				op.parent = root.parent;
				op.leftChild = root;
				if(left)
					parent.leftChild = op;
				else
					parent.rightChild = op;
				root.parent = op;
				return true;
			}
		}
		
		if(!found && root.leftChild != null)
			found = traverseTreeJoin(root.leftChild,op, root, true);
		
		if(!found && root.rightChild != null)
			found = traverseTreeJoin(root.rightChild,op, root, false);
		
		return found;
	}*/
	
	
	public static void createCondList(AndExpression exp, List<Expression> expList)
	{
		Expression leftExp = exp.getLeftExpression();
		Expression rightExp = exp.getRightExpression();
		if(leftExp instanceof AndExpression == false)
		{
			expList.add(leftExp);
		}
		else
		{
			createCondList((AndExpression)leftExp, expList);
		}
		
		if(rightExp instanceof AndExpression == false)
		{
			expList.add(rightExp);
		}
		else
		{
			createCondList((AndExpression)rightExp, expList);
		}
		
	}
	
	
	public static RelationalAlgebra2 createTree(PlainSelect query, String alias) {
		int rootCreated = 0;
		RelationalAlgebra2 root=null;
		RelationalAlgebra2 parent=null;
		
		List<OrderByElement> orderbyEl = query.getOrderByElements();
		if(orderbyEl != null & query.getOrderByElements().size() !=1) {
			RelationalAlgebra2 op = new OrderBy2();
			OrderBy2 op1= (OrderBy2)op;
			op1.element = orderbyEl;
			op= (RelationalAlgebra2)op1 ;
			op.parent = null;
			op.leftChild = null;
			op.rightChild = null;
			root = op;
			parent = op;
			rootCreated = 1;
		}
		
		int aggr = 0; 
		List<Function> functionList = new ArrayList<Function>();  //list of aggr functions in order
		List<Integer> functionIndex = new ArrayList<Integer>(); 
		List<SelectItem> aggrProjection = new ArrayList<SelectItem>(); //SelectItem stmtm without functions to be passed to proj below aggr 
//		String projSelectStmt = "";
		List<Column> parentProj = new ArrayList<Column>(); //list of columns with aliases after applying aggr to be passed above
		
		Iterator<SelectItem> columnName =  query.getSelectItems().iterator();
		int index=0;
		while(columnName.hasNext())
		{
			
			SelectItem currSelectItem = columnName.next();
			
			if(currSelectItem instanceof AllColumns || currSelectItem instanceof AllTableColumns)
			{
				break;
			}
			else
			{
				SelectExpressionItem currSelectExpression = ((SelectExpressionItem) currSelectItem);
				Expression currItem = currSelectExpression.getExpression();
				if(currItem instanceof Function)
				{
					
					functionList.add(((Function) currItem));
					functionIndex.add(index);
					/*
					if(projSelectStmt.equals(""))
					{
						projSelectStmt = projSelectStmt + (((Function) currItem).getParameters().getExpressions().get(0).toString());
					}
					else
					{
						projSelectStmt = projSelectStmt + ","+((Function) currItem).getParameters().getExpressions().get(0).toString();
					}
					*/
					
					SelectItem selectItem = new SelectExpressionItem();
					SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
					selectExpressionItem.setExpression(((Function) currItem).getParameters().getExpressions().get(0));
					SelectItem selectItem2 = (SelectItem)selectExpressionItem;
					aggrProjection.add(selectItem2);
					
					aggr=1;
				}
				else
				{
					/*
					if(projSelectStmt.equals(""))
					{
						projSelectStmt = projSelectStmt + currSelectItem.toString();
					}
					else
					{
						projSelectStmt = projSelectStmt + ","+currSelectItem.toString();
					}
					*/

					aggrProjection.add(currSelectItem);
				}
				
				
				Column col = new Column();
				String als = currSelectExpression.getAlias();
				Table tab_temp = new Table();
				if(als!=null)
				{
					col.setColumnName(als);
				}
				else
				{
					col.setColumnName(currSelectExpression.toString());
				}
				
				col.setTable(tab_temp);
				parentProj.add((Column)col);
			}
			
			index++;
			
		}
		
		
		if(aggr==1)
		{
			/*
			Reader input = new StringReader("select "+projSelectStmt+" from xyz");
			CCJSqlParser parser = new CCJSqlParser(input);
			Statement statement = null;
			try {
				statement = parser.Statement();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			aggrProjection =  ((PlainSelect) ((Select) statement).getSelectBody()).getSelectItems();
			*/
			RelationalAlgebra2 op = new Aggregate2();
			Aggregate2 op1 = (Aggregate2)op;
			op1.groupByColumns = query.getGroupByColumnReferences();
			op1.aggrFunctions.addAll(functionList);
			op1.colNamesParent.addAll(parentProj);
			op1.functionIndex.addAll(functionIndex);
			op= (RelationalAlgebra2)op1 ;
			if(rootCreated == 0)
			{
				op.parent = null;
				root = op;
			}
			else
			{
				parent.leftChild = op;
				op.parent = parent;
			}
			
			op.leftChild= null;
			op.rightChild = null;
			parent = op;
			
			RelationalAlgebra2 opproj = new Projection2();
			Projection2 opproj1 = (Projection2)opproj;
			
			opproj1.projection = aggrProjection;
			opproj= (RelationalAlgebra2)opproj1;
			opproj.parent = parent;
			opproj.leftChild= null;
			opproj.rightChild = null;
			parent.leftChild = opproj;
			parent = opproj;
			
		}
		else
		{	
			List<SelectItem> selItem = query.getSelectItems();
			if(!selItem.isEmpty()) {
				RelationalAlgebra2 op = new Projection2();
				Projection2 op1= (Projection2)op;
				op1.projection = selItem;
				
				if(rootCreated == 0)
				{
					op1.subQuery_alias = alias;
					op= (RelationalAlgebra2)op1;
					op.parent = null;
					op.leftChild= null;
					op.rightChild = null;
					root = op;
				}
				else
				{
					op= (RelationalAlgebra2)op1;
					op.leftChild= null;
					op.rightChild = null;
					op.parent = parent;
					parent.leftChild = op;
					
				}
				
				
				parent = op;
			}
		}
		Expression exp = query.getWhere();
		if(exp != null) {
			RelationalAlgebra2 op = new Selection2();
			Selection2 op1 = (Selection2)op;
			op1.expression = exp;
			op = (RelationalAlgebra2)op1;
			op.parent = parent;
			op.leftChild = null;
			op.rightChild = null;
			parent.leftChild = op;
			parent = op;
			
		}
		FromItem from = query.getFromItem();
		if(from != null) {
			
			if(query.getJoins()!=null)
			{

				int joinCnt=query.getJoins().size();
				
				RelationalAlgebra2 op = new Join2();
				
				Scan2 leftChild = new Scan2();
				Scan2 rightChild = new Scan2();
				
				leftChild.fromitem = from;
				rightChild.fromitem = (FromItem) query.getJoins().get(0).getRightItem();
				
				op.leftChild = leftChild;
				op.rightChild = rightChild;
				
				op.childTables.add(from.toString());
				op.childTables.add(query.getJoins().get(0).getRightItem().toString());
				
				op.childTableAliases.add(from.getAlias());
				op.childTableAliases.add(query.getJoins().get(0).getRightItem().getAlias());
				
				
				leftChild.parent = op;
				rightChild.parent = op;
				
				for(int i=1;i<joinCnt;i++) //parent not getting set
				{
					
					RelationalAlgebra2 opp = new Join2();
					
					Scan2 right = new Scan2();	
					right.fromitem = (FromItem) query.getJoins().get(i).getRightItem();
					
					opp.leftChild = op;
					opp.rightChild = right;
					
					
					opp.childTables.addAll(op.childTables);
					opp.childTables.add(query.getJoins().get(i).getRightItem().toString());
					
					opp.childTableAliases.addAll(op.childTableAliases);
					opp.childTableAliases.add(query.getJoins().get(i).getRightItem().getAlias());
					
					
					right.parent = opp;					
					op.parent = opp;
					op = opp;
					
				}
				
				parent.leftChild = op;
				op.parent = parent;
				parent = op;
				
			}
			else
			{
				RelationalAlgebra2 op = new Scan2();
				Scan2 op1 = (Scan2)op;
				
				
				if(from instanceof SubSelect)
				{
					
					RelationalAlgebra2 subTreeRoot = null;;
					subTreeRoot = createTree((PlainSelect) ((SubSelect) from).getSelectBody(), ((SubSelect) from).getAlias());
					parent.leftChild = subTreeRoot;	
				}			
				else
				{			
						
					op1.fromitem = from;
					op = (RelationalAlgebra2)op1;
					op.parent = parent;
					op.leftChild = null;
					op.rightChild = null;
					parent.leftChild = op;
					parent = op;
				}
			}
		}
		
		return root;
	}

	public static void main(String[] args) throws ParseException, SQLException {
		
		RelationalAlgebra2 treeRoot = null;
		String prompt = "$> ";		
		System.out.println(prompt);
        System.out.flush();

        Reader input = new InputStreamReader(System.in);

		
		CCJSqlParser parser = new CCJSqlParser(input);
		Statement statement = parser.Statement();
		while(statement != null) {
			
			//System.out.println(statement);
			
			
			if(statement instanceof Select) {
				Select select = (Select) statement;
				SelectBody body = select.getSelectBody();
				if(body instanceof PlainSelect)
				{
					l=-1;
					c=0;
					PlainSelect plain = (PlainSelect)body;
					if (plain.getLimit()!=null)
					{	l=(int)plain.getLimit().getRowCount();
					}
					treeRoot = createTree(plain,"");
					
					selectionOptimize(treeRoot);
					
					try 
					{
						ParseTree(treeRoot);
					} 
					catch (IOException e) 
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					
				}

			}
			else if(statement instanceof CreateTable) {
				CreateTable create1 = (CreateTable) statement;

				String tableName = create1.getTable().getName().toLowerCase() ;
				map.put(tableName, create1);
			}
			System.out.println(prompt);
            System.out.flush();
			statement = parser.Statement();
		}
	}
}
