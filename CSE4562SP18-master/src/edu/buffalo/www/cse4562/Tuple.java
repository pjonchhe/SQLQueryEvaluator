package edu.buffalo.www.cse4562;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVRecord;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
//import net.sf.jsqlparser.stable.*;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class Tuple 
{
  public CreateTable table;
  public CSVRecord record;
  public List<PrimitiveValue> tuple;
  //public List<String> columnNames;
  public List<Column> colNames;
  
  Tuple(){
	  tuple = new ArrayList<PrimitiveValue>();
	  //columnNames = new ArrayList<String>();
	  colNames = new ArrayList<Column>();
  }

}
