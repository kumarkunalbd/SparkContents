package models;

public class RSIDataAverage {
	
	private static final long serialVersionUID = 1L;
	private double avregaeGain = 0.0;
	private double averageLoss = 0.0;
	private double finalRSI = 0.0;
	public double getFinalRSI() {
		return finalRSI;
	}
	public void setFinalRSI(double finalRSI) {
		this.finalRSI = finalRSI;
	}
	public double getAvregaeGain() {
		return avregaeGain;
	}
	public void setAvregaeGain(double avregaeGain) {
		this.avregaeGain = avregaeGain;
	}
	public double getAverageLoss() {
		return averageLoss;
	}
	public void setAverageLoss(double averageLoss) {
		this.averageLoss = averageLoss;
	}
	

}
