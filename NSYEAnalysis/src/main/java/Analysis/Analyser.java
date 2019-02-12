package Analysis;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import models.StockPrice;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.*;

public class Analyser {

	public static void main(String[] args) throws InterruptedException{
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("StockAnalyser");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(60));
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
			public Integer call(Integer valuePresent, Integer valueIncoming) throws Exception {
				System.out.println("Summary function is running");
				System.out.println(valuePresent+"+"+valueIncoming);
				return valuePresent+valueIncoming;
				
			}
		};
		
		Function2<Integer,Integer, Integer> invReduceFunct = new Function2<Integer,Integer, Integer>() {
			public Integer call(Integer valuePresent, Integer valueOutgoing) throws Exception {
				System.out.println("Inverse function is running");
				System.out.println(valuePresent+"-"+valueOutgoing);
				return valuePresent-valueOutgoing;
			}
		};
		JavaDStream<String> lines = jssc.textFileStream("/Users/kumarkunal/Upgrad_materials/Course5-Mod7-SparkStream/NSYE_Data-12Feb2");
		lines.print();
		JavaDStream<Map<String, StockPrice>> stockStream = Analyser.convertIntoDStream(lines);
		stockStream.print();
		jssc.start();
		jssc.awaitTermination();
		jssc.close();

	}
	
	private static JavaDStream<Map<String, StockPrice>>convertIntoDStream(JavaDStream<String> json){
		
		return json.map(x -> {
			ObjectMapper mapper = new ObjectMapper();
			TypeReference<List<StockPrice>> mapType = new
			TypeReference<List<StockPrice>>() {
			};
			List<StockPrice> list = mapper.readValue(x, mapType);
			Map<String, StockPrice> map = new HashMap<>();
			for (StockPrice sp : list) {
			map.put(sp.getSymbol(), sp);
			}
			return map;
			});
	}
	
	/*private static JavaPairDStream<String, PriceData> getWindowDStream(
			JavaDStream<Map<String, StockPrice>> stockStream){
		
	}*/

}