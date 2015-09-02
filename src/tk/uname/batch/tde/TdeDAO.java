package tk.uname.batch.tde;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import tk.uname.batch.dao.AbstractDAO;
import tk.uname.batch.model.Player;


public class TdeDAO extends AbstractDAO {

	
	public TdeDAO() {
		
	}
	
	public void createTables() throws SQLException{
		//futboluname.player_load_tmp
		StringBuffer sql = new StringBuffer(" CREATE TABLE futboluname.player_load_tmp ")
			.append(" ( ")
			.append( " p_name VARCHAR(30), ")
			.append( " real_team VARCHAR(20), ")
			.append( " field_position INTEGER, ")
			.append( " points INTEGER, ")
			.append( " market_value INTEGER, ")
			.append( " CONSTRAINT player_load_tmp_pk PRIMARY KEY (p_name) ")
			.append( " ) ");
		
		PreparedStatement preparedStatement = createPreparedStatement(sql
				.toString());
			
		executeInsertStatement();
		closePreparedStatement();

		
		//Real_Team
		sql = new StringBuffer(" CREATE TABLE futboluname.real_team ")
			.append(" ( ")
			.append( " id INTEGER, ")
			.append( " t_name VARCHAR(20), ")
			.append( " CONSTRAINT real_team_pk PRIMARY KEY (id) ")
			.append( " ) ");
		
		preparedStatement = createPreparedStatement(sql
				.toString());
			
		executeInsertStatement();
		closePreparedStatement();

		//Market
		sql = new StringBuffer(" CREATE TABLE futboluname.market ")
			.append(" ( ")
			.append( " s_day INTEGER, ")
			.append( " player INTEGER, ")
			.append( " sell_prize INTEGER, ")
			.append( " CONSTRAINT market_pk PRIMARY KEY (player) ")
			.append( " ) ");
		
		preparedStatement = createPreparedStatement(sql
				.toString());
			
		executeInsertStatement();
		closePreparedStatement();	
		
		//Player
		sql = new StringBuffer(" CREATE TABLE futboluname.player ")
			.append(" id INTEGER, ")
			.append( " pl_name VARCHAR(30), ")
			.append( " real_team INTEGER, ")
			.append( " user_team INTEGER, ")
			.append( " field_position INTEGER, ")
			.append( " market_value INTEGER, ")
			.append( " points INTEGER, ")
			.append( " updated_market_value INTEGER, ")
			.append( " updated_points INTEGER, ")
			.append( " CONSTRAINT player_pk PRIMARY KEY (id) ")
			;
		
		preparedStatement = createPreparedStatement(sql
				.toString());
			
		executeInsertStatement();
		closePreparedStatement();	
		
	}
	
	public void loadTables(List<Player> playersList) throws SQLException {
		
		//PLAYER_LOAD_TMP
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
		
		//REAL_TEAM
		StringBuffer sql = new StringBuffer(" INSERT INTO futboluname.real_team (id, t_name) ")
		.append(" ( SELECT row_number() OVER (order by real_team) as rownum, real_team ")
		.append(" FROM futboluname.player_load_tmp ")
		.append(" GROUP BY real_team ORDER BY real_team )")
		;
		PreparedStatement preparedStatement = createPreparedStatement(sql
				.toString());
		executeInsertStatement();
		
		closePreparedStatement();
		
		//PLAYER
		 sql = new StringBuffer(" INSERT INTO futboluname.player (id, pl_name, real_team, user_team, field_position, market_value, ")
		.append(" points, updated_market_value, updated_points) ")
		.append(" SELECT row_number() OVER (order by real_team) as id, pl_name, ")
		.append(" real_team, user_team, field_position, ")
		.append(" market_value, points, updated_market_value, ")
		.append(" updated_points ")
		.append(" FROM( ")
		.append(" SELECT player.p_name as pl_name, team.id as real_team, 0 as user_team, player.field_position,  ")
		.append(" player.market_value, player.points, 0 as updated_market_value, ")
		.append(" 0 as updated_points ")
		.append(" FROM ")
		.append(" futboluname.player_load_tmp player JOIN futboluname.real_team team ")
		.append(" ON player.real_team = team.t_name ")
		.append(" ORDER BY real_team ")
		.append(" ) subquery ")
		;
		preparedStatement = createPreparedStatement(sql
				.toString());
		executeInsertStatement();
		
		closePreparedStatement();
		
		//CARGAMOS DATOS EN MARKET SOLO PARA DESARROLLO:
		 sql = new StringBuffer(" INSERT INTO futboluname.market (s_day, player, sell_prize) ")
			.append(" SELECT 20150901 as s_day, id as player, ")
			.append(" market_value as sell_prize ")
			.append(" FROM futboluname.player ")
			.append(" WHERE real_team IN (5,7) ")
			.append(" AND ID BETWEEN 118 AND 157 ")
			;
			 preparedStatement = createPreparedStatement(sql
					.toString());
			executeInsertStatement();
			
			closePreparedStatement();
			
			//PLAYER
			sql = new StringBuffer(" INSERT INTO futboluname.player (id, pl_name, real_team, user_team, field_position, market_value, ")
				.append(" points, updated_market_value, updated_points) ")
				.append(" SELECT row_number() OVER (order by real_team) as id, pl_name, ")
				.append(" real_team, user_team, field_position, ")
				.append(" market_value, points, updated_market_value, ")
				.append(" updated_points ")
				.append(" FROM( ")
				.append(" SELECT player.p_name as pl_name, team.id as real_team, 0 as user_team, player.field_position,  ")
				.append(" player.market_value, player.points, 0 as updated_market_value, ")
				.append(" 0 as updated_points ")
				.append(" FROM ")
				.append(" futboluname.player_load_tmp player JOIN futboluname.real_team team ")
				.append(" ON player.real_team = team.t_name ")
				.append(" ORDER BY real_team ")
				.append(" ) subquery ")
				;
			preparedStatement = createPreparedStatement(sql.toString());
			executeInsertStatement();
			
			closePreparedStatement();
			
			//CARGAMOS DATOS EN MARKET SOLO PARA DESARROLLO:
			 sql = new StringBuffer(" INSERT INTO futboluname.market (s_day, player, sell_prize) ")
				.append(" SELECT 20150901 as s_day, id as player, ")
				.append(" market_value as sell_prize ")
				.append(" FROM futboluname.player ")
				.append(" WHERE real_team IN (5,7) ")
				.append(" AND ID BETWEEN 118 AND 157 ")
				;
			preparedStatement = createPreparedStatement(sql
						.toString());
			executeInsertStatement();
				
			closePreparedStatement();
		
	}
	
	public void cargaPlayer() throws SQLException{

	}
	
}
