package models;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.hadoop.io.DoubleWritable;

public class PriceData {
	
	private static final long serialVersionUID = 1L;
	
	private double open;
	private double high;
	private double low;
	private double close;
	private double volume;
	public double getOpen() {
		return open;
	}
	public void setOpen(double open) {
		this.open = open;
	}
	public double getHigh() {
		return high;
	}
	public void setHigh(double high) {
		this.high = high;
	}
	public double getLow() {
		return low;
	}
	public void setLow(double low) {
		this.low = low;
	}
	public double getClose() {
		return close;
	}
	public void setClose(double close) {
		this.close = close;
	}
	public double getVolume() {
		return volume;
	}
	public void setVolume(double volume) {
		this.volume = volume;
	}
	
	/*private void writeObject(ObjectOutputStream o) throws IOException {  
		    
		o.writeDouble(open);
		o.writeDouble(high);
		o.writeDouble(close);
		o.writeDouble(volume);
		o.writeDouble(low);		
	}
	
	private void readObject(ObjectInputStream o) throws IOException, ClassNotFoundException {  
		    this.open = o.readDouble();
		    this.high = o.readDouble();
		    this.close = o.readDouble();
		    this.volume = o.readDouble();
		    this.low = o.readDouble();
		   
	}*/
		
}
