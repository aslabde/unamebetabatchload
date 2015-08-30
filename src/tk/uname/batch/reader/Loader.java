package tk.uname.batch.reader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import tk.uname.batch.model.Player;

public class Loader {

	public List<Player> loadPlayerList() throws IOException{
		List<Player> playersList = new ArrayList<Player>();
		
		String URL_BASE = "http://www.comuniazo.com/comunio/jugadores";
		URL url = new URL(URL_BASE);
		URLConnection uc = url.openConnection();
		uc.addRequestProperty("User-Agent", 
				"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)");
		InputStreamReader input = new InputStreamReader(uc.getInputStream());
		BufferedReader 		 in = new BufferedReader(input);
		String line;
		
		while ((line = in.readLine()) != null ){
			line=line.substring(line.indexOf("<h2>Jugadores</h2>"),line.length());
			line=line.substring(0,line.indexOf("Mostrar todos"));
			//System.out.println(line);
			
			List<String> playerBrute = Arrays.asList(line.split("<tr>"));
			
			
			for(String s: playerBrute){
				//System.out.println(s);
				
				if(s.startsWith("<td class=")){
					
					List<String> attributesBrute = Arrays.asList(s.split("<td"));
						
					//Position OK
					String positionBrute = attributesBrute.get(1);
					int position = Integer.
								valueOf(positionBrute.substring(positionBrute.indexOf("pos-")+4,positionBrute.indexOf("pos-") + 5));
																			
					//Team OK
					String teamBrute =  attributesBrute.get(2);
					String team = teamBrute.substring(teamBrute.indexOf("spr-")+ 4, teamBrute.lastIndexOf("\""));
			
					//Name OK
					String nameBrute = attributesBrute.get(3);
					String name = nameBrute.substring(nameBrute.lastIndexOf("\"")+2,nameBrute.lastIndexOf("</a"));
					
					//Points OK
					String pointsBrute = attributesBrute.get(4);
					int points = Integer.valueOf(pointsBrute.substring(pointsBrute.lastIndexOf("\"") + 2 , pointsBrute.lastIndexOf("<") ));
					
					//Value OK
					String valueBrute = attributesBrute.get(10);
					valueBrute = valueBrute.substring(valueBrute.lastIndexOf("\"")+2, valueBrute.lastIndexOf("<"));
					valueBrute = valueBrute.replace(".", "");
					int value = Integer.valueOf(valueBrute);
					
					Player player = new Player(name, team, position, value, points);
					playersList.add(player);
						
					//System.out.println(position + " " + team + " " + name + " " + points + " " + value);
				}	
			}
			
					
		}
		
		return playersList;
	}
}
