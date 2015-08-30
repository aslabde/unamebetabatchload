package tk.uname.batch.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class AbstractDAO {

	private static final String DB_DRV = "oracle.jdbc.driver.OracleDriver";

	protected Properties properties;
	protected Connection connection;
	private PreparedStatement preparedStatement;
	private ResultSet resultSet;

	public AbstractDAO() {
		super();
		
	}

	public void openConnection() throws ClassNotFoundException, SQLException {
			
		closeConnection();
		
		String url = "jdbc:postgresql://localhost/postgres";
		String usr = "postgres";
		String psw = "postgres";
		try {
			connection = DriverManager.getConnection(url, usr, psw);
		} catch (SQLException e) {
			throw new SQLException("Cannot get Connection");
		}
	}

	protected PreparedStatement createPreparedStatement(String sql)
			throws SQLException {
		if (preparedStatement != null) {
			closePreparedStatement();
		}
		try {
			preparedStatement = connection.prepareStatement(sql);
		} catch (SQLException e) {
			throw new SQLException("Cannot create PreparedStatement");
		}
		return preparedStatement;
	}

	protected ResultSet executePreparedStatement() throws SQLException {
		try {
			resultSet = preparedStatement.executeQuery();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new SQLException("Cannot execute Query");
		}
		return resultSet;
	}
	
	protected void executeInsertStatement() throws SQLException {
		try {
			 preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new SQLException("Cannot execute Query");
		}
		
	}

	protected ResultSet getResultSet() {
		return resultSet;
	}
	
	protected void closePreparedStatement() {
		if (preparedStatement != null) {
			try {
				preparedStatement.close();
				preparedStatement = null;
			} catch (SQLException e) {
//				throw new SQLException("Cannot close PreparedStatement");
				e.printStackTrace();
			}
		}
		closeResultSet();
	}	
	
	protected void closeResultSet() {
		if (resultSet != null) {
			try {
				resultSet.close();
				resultSet = null;
			} catch (SQLException e) {
//				throw new SQLException("Cannot close PreparedStatement");
				e.printStackTrace();
			}
		}
	}	
	
	public void closeConnection() {
		closePreparedStatement();
		closeResultSet();
		if (connection != null) {
			try {
				connection.close();
				connection = null;
			} catch (SQLException e) {
//				throw new SQLException("Cannot close Connection");
				e.printStackTrace();
			}
		}
	}

	public Connection getConnection(){
		return this.connection;
		
	}
	
	public void setConnection(Connection connection){
		this.connection=connection;
		
	}


}
