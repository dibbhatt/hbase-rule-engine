package hbase.rule.data;

public class Result {
	
	private String personId;
	
	private float resultScore;
	
	private int totalDuration;
	
	private int itemsWorked;
	
	int ruleFired = 0;

	public String getPersonId() {
		return personId;
	}

	public void setPersonId(String personId) {
		this.personId = personId;
	}

	public float getResultScore() {
		return resultScore;
	}

	public void setResultScore(float resultScore) {
		this.resultScore = resultScore;
	}

	public int getTotalDuration() {
		return totalDuration;
	}

	public void setTotalDuration(int totalDuration) {
		this.totalDuration = totalDuration;
	}

	public int getItemsWorked() {
		return itemsWorked;
	}

	public void setItemsWorked(int itemsWorked) {
		this.itemsWorked = itemsWorked;
	}

	public int getRuleFired() {
		return ruleFired;
	}

	public void setRuleFired(int ruleFired) {
		this.ruleFired = ruleFired;
	}

}
