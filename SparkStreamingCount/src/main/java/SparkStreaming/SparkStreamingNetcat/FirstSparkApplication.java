package SparkStreaming.SparkStreamingNetcat;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.log4j.*;

public class FirstSparkApplication {

	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("FirstSparkApplication");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));
		Logger.getRootLogger().setLevel(Level.ERROR);
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
		lines.print();
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
		
		
	}

}
