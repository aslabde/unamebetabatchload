package tk.uname.batch.model;

public class Player {
	
	private String name;
	private String team;
	private int position; 
	private int value;
	private int points;
	
	public Player(String name, String team, int position, int value, int points) {
		super();
		this.name = name;
		this.team = team;
		this.position = position;
		this.value = value;
		this.points = points;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getTeam() {
		return team;
	}

	public void setTeam(String team) {
		this.team = team;
	}

	public int getPosition() {
		return position;
	}

	public void setPosition(int position) {
		this.position = position;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

	public int getPoints() {
		return points;
	}

	public void setPoints(int points) {
		this.points = points;
	}
	
	
	
}
