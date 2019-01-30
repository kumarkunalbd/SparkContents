package SparkStreaming.SparkStreamingNetcat;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.log4j.*;

public class FirstSparkApplication {

	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("FirstSparkApplication");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));
		jssc.checkpoint("checkpoint_dir");
		Logger.getRootLogger().setLevel(Level.ERROR);
		//JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
		/*JavaDStream<String> words = lines.flatMap(x->Arrays.asList(x.split(" ")).iterator());
		JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
		JavaPairDStream<String, Integer> updatedPairs = pairs.updateStateByKey((values,currentState)->{
			int sum = currentState.or(0);
			for (int i: values) {
				sum += i;
			}
			return Optional.of(sum);
			
		});
		updatedPairs.print();*/
		//lines.window(Durations.seconds(6),Durations.seconds(4)).print();
		/* Implementing window operations*/
		Function2<Integer,Integer, Integer> reduceFunct = new Function2<Integer, Integer, Integer>() {
			@Override 
			public Integer call(Integer valuePresent, Integer valueIncoming) throws Exception {
				System.out.println("Summary function is running");
				System.out.println(valuePresent+"+"+valueIncoming);
				return valuePresent+valueIncoming;
				
			}
		};
		
		Function2<Integer,Integer, Integer> invReduceFunct = new Function2<Integer,Integer, Integer>() {
			@Override 
			public Integer call(Integer valuePresent, Integer valueOutgoing) throws Exception {
				System.out.println("Inverse function is running");
				System.out.println(valuePresent+"-"+valueOutgoing);
				return valuePresent-valueOutgoing;
			}
		};
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
		//lines.flatMap(line -> Arrays.asList(line.split(​" "​)).iterator()).reduceByWindow(reduceFunc, invReduceFunc, Durations.seconds(​10​), Durations.seconds(​6​)).print();
		lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
					.mapToPair(line -> new Tuple2<>(line, 1))
					.reduceByKeyAndWindow(reduceFunct, invReduceFunct, Durations.seconds(10), Durations.seconds(6))
					.print();
		
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
		
		
	}

}
