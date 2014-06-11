package org.hsqldb.jdbc;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;

import org.hsqldb.ColumnBase;
import org.hsqldb.SessionInterface;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.navigator.RowSetNavigatorClient;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.types.Type;

import rasj.RasDimensionMismatchException;
import rasj.RasGMArray;
import rasj.RasIndexOutOfBoundsException;
import rasj.RasMArrayByte;
import rasj.RasMArrayDouble;
import rasj.RasPoint;

public class JDBCMArray implements Array {

	volatile boolean closed;
	Type             arrayType;
	Type             elementType;
	RasGMArray       data;
	JDBCConnection   connection;
	SessionInterface sessionProxy;

	public JDBCMArray(Object data, Type type, Type arrayType,
			SessionInterface session) {

		this(data, type, arrayType, session.getJDBCConnection());

		this.sessionProxy = session;
	}

	/**
	 * Constructor should reject unsupported types.
	 */
	JDBCMArray(Object data, Type type,
			JDBCConnection connection) throws SQLException {
		this(data, type, null, connection);
	}

	JDBCMArray(Object data, Type type, Type arrayType,
			JDBCConnection connection) {

		this.data         = (RasGMArray)data;
		this.elementType  = type;
		this.arrayType    = arrayType;
		this.connection   = connection;

		if (connection != null) {
			this.sessionProxy = connection.sessionProxy;
		}
	}

	public Object getArrayInternal() {
		return data;
	}

	private Result newColumnResult(long position,
			int count) throws SQLException {
		
		int mArraySize = 0;
		String mArrayStruct = data.getTypeStructure();
		ArrayList<Integer> dimSize = getDimSize(mArrayStruct);
		ArrayList<Integer> iterIndex = new ArrayList<>();	//go through all the array values
		
		for (int i = 0; i < dimSize.size(); i++) {
			mArraySize += dimSize.get(i);
		}
		
		// tests if the required elements exist. The limits are determined
		// by computing the product of each dimension's size
		if (!JDBCClobClient.isInLimits(mArraySize, position, count)) {
			throw JDBCUtil.outOfRangeArgument();
		}

		Type[] types = new Type[2];

		types[0] = Type.SQL_VARCHAR;
		types[1] = elementType;

		ResultMetaData meta = ResultMetaData.newSimpleResultMetaData(types);

		meta.columnLabels = new String[] {
				"C1", "C2"
		};
		meta.colIndexes   = new int[] {
				-1, -1
		};
		meta.columns      = new ColumnBase[2];

		ColumnBase column = new ColumnBase("", "", "", "");

		column.setType(types[0]);

		meta.columns[0] = column;
		column          = new ColumnBase("", "", "", "");

		column.setType(types[1]);

		meta.columns[1] = column;

		RowSetNavigatorClient navigator = new RowSetNavigatorClient();

		
		for (int i = (int) position; i < position + count; i++) {
			Object[] rowData = new Object[2];

			rowData[0] = Integer.valueOf(i + 1);
			try {
				rowData[1] = data.getCell(new RasPoint(1,1));
			} catch (RasDimensionMismatchException
					| RasIndexOutOfBoundsException e) {
				System.err.println("Can't get the cell at point (1, 1)");
				e.printStackTrace();
			}

			navigator.add(rowData);
		}

		Result result = Result.newDataResult(meta);

		result.setNavigator(navigator);

		return result;
	}
	
	/**
	 * Updates the indexes array, going to the next element
	 * @param dimSize: The size of all dimensions of the array
	 * @param indexes: The current indexes while going through the 
	 * 				   values of the multidimensional array
	 * @param currDim: the dimension on which we are currently iterating
	 * @return
	 * 		The dimension on which we are iterating after updating the indexes
	 */
	private int increaseIndex(ArrayList<Integer> dimSize, ArrayList<Integer> indexes, int currDim) {
		int dimIndex = indexes.get(currDim);
		int maxDimIndex = dimSize.get(currDim);
		if (dimIndex < maxDimIndex - 1) {
			dimIndex++;
			indexes.set(currDim, dimIndex);
		} else {
			//TODO increase indexes
		}
		
		return 0;
	}
	
	/**
	 * Returns a list with a number of elements equal to the number
	 * of dimensions of the multidimensional array. Each element 
	 * represents the number of elements of the corresponding dimension
	 * @param structure: The structure of the array taken from the metadata
	 * @return
	 */
	// TODO implement unit test
	private ArrayList<Integer> getDimSize(String mArrayStruct) {
		ArrayList<Integer> dimSize = new ArrayList<>();
		int startIndex;
		int endIndex;
		
		startIndex = mArrayStruct.indexOf("[");
		endIndex = mArrayStruct.indexOf("]");
		
		String structure = mArrayStruct.substring(startIndex + 1, endIndex);
		
		String[] intervals = structure.split(",");
		
		for (int i = 0; i < intervals.length; i++) {
			// TODO check the case for select *
			// or the case when only one value is specified and
			// not an interval 1 not 1:200
			String[] interval = intervals[i].split(":");
			int startPos = Integer.parseInt(interval[0]);
			int endPos = Integer.parseInt(interval[1]);
			dimSize.add(endPos - startPos);
		}
		
		return dimSize;
	}

	private void checkClosed() throws SQLException {

		if (closed) {
			throw JDBCUtil.sqlException(ErrorCode.X_07501);
		}
	}

	@Override
	public void free() throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public Object getArray() throws SQLException {
		
		checkClosed();
		
		return data;
	}

	@Override
	public Object getArray(Map<String, Class<?>> map) throws SQLException {
		return getArray();
	}

	@Override
	public Object getArray(long index, int count) throws SQLException {
		//TODO return only a limited number of elements
		String error = "getArray(long index, int count) is not yet implemented";
		throw new UnsupportedOperationException(error);
	}

	@Override
	public Object getArray(long index, int count, Map<String, Class<?>> map)
			throws SQLException {
		return getArray(index, count);
	}

	@Override
	public int getBaseType() throws SQLException {
		
		checkClosed();
		
		return elementType.getJDBCTypeCode();
	}

	@Override
	public String getBaseTypeName() throws SQLException {
		
		checkClosed();
		
		return elementType.getNameString();
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getResultSet(Map<String, Class<?>> map)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getResultSet(long index, int count) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getResultSet(long index, int count,
			Map<String, Class<?>> map) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

}
