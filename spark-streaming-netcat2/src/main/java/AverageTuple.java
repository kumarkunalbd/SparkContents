import java.io.Serializable;

public class AverageTuple implements Serializable{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private int count;
    private float average;
    public AverageTuple(int count, float average) {
    super();
    this.count = count;
    this.average = average;
    }
    public int getCount() {
    return count;
    }
    public void setCount(int count) {
    this.count = count;
    }
    public float getAverage() {
    return average;
    }
    public void setAverage(float average) {
    this.average = average;
    }
    @Override
    public String toString() {
    return String.valueOf(average/count);
    }
}
