package models;

public class RSIData {
	private double gain =0.0;
	private double loss = 0.0;
	private long aggregatedStockCounter = 1;
	private double averageGain = 0;
	private double averageLoss = 0;
	
	public double getGain() {
		return gain;
	}
	public void setGain(double gain) {
		this.gain = gain;
	}
	public double getLoss() {
		return loss;
	}
	public void setLoss(double loss) {
		this.loss = loss;
	}
	public long getAggregatedStockCounter() {
		return aggregatedStockCounter;
	}
	public void setAggregatedStockCounter(long aggregatedStockCounter) {
		this.aggregatedStockCounter = aggregatedStockCounter;
	}
	public double getAverageGain() {
		return averageGain;
	}
	public void setAverageGain(double averageGain) {
		this.averageGain = averageGain;
	}
	public double getAverageLoss() {
		return averageLoss;
	}
	public void setAverageLoss(double averageLoss) {
		this.averageLoss = averageLoss;
	}

}
