package tk.uname.batch.tde;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import tk.uname.batch.dao.AbstractDAO;
import tk.uname.batch.model.Player;


public class TdeDAO extends AbstractDAO {

	
	public TdeDAO() {
		
	}

	public void loadTable(List<Player> playersList) throws SQLException {
		
		for(Player p: playersList){
			StringBuffer sql = new StringBuffer("INSERT INTO FUTBOLUNAME.PLAYER_LOAD_TMP ")
			.append(" ( P_NAME, ")
			.append(" REAL_TEAM, ")
			.append(" FIELD_POSITION, ")
			.append(" POINTS, ")
			.append(" MARKET_VALUE ) ")
			.append(" VALUES ")
			.append(" (? , ? , ? , ? , ?) ");
					

			PreparedStatement preparedStatement = createPreparedStatement(sql
			.toString());
	
			preparedStatement.setString(1, p.getName());
			preparedStatement.setString(2, p.getTeam());
			preparedStatement.setInt(3, p.getPosition());
			preparedStatement.setInt(4, p.getPoints());
			preparedStatement.setInt(5, p.getValue());
			
			executeInsertStatement();
		}
	
		closePreparedStatement();
		
	}
	
}
