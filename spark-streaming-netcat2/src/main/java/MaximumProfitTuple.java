import java.io.Serializable;

public class MaximumProfitTuple implements Serializable{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private int count;
    private float close_average;
    private float open_average;
    public MaximumProfitTuple(int count, float close_average, float open_average) {
    super();
    this.count = count;
    this.close_average = close_average;
    this.open_average = open_average;
    }
    public int getCount() {
        return count;
    }
    public void setCount(int count) {
        this.count = count;
    }
    public float getClose_average() {
        return close_average;
    }
    public void setClose_average(float close_average) {
        this.close_average = close_average;
    }
    public float getOpen_average() {
        return open_average;
    }
    public void setOpen_average(float open_average) {
        this.open_average = open_average;
    }
    @Override
    public String toString() {
        return String.valueOf(close_average/count - open_average/count);
    }
}
