package edu.buffalo.www.cse4562;

import java.sql.SQLException;

public interface RelationalAlgebra
{
   boolean api(Tuple tupleobj) throws SQLException;
}
