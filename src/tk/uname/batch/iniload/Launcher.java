package tk.uname.batch.iniload;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import tk.uname.batch.model.Player;
import tk.uname.batch.reader.Loader;
import tk.uname.batch.tde.TdeDAO;

public class Launcher {

	public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {
		
		Loader loader = new Loader();
		TdeDAO dao    = new TdeDAO();
		
		List<Player> playersList = loader.loadPlayerList();
		dao.openConnection();
		dao.loadTable(playersList);
	}

}
