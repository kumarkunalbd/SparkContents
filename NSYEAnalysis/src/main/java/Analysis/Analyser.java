package Analysis;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import scala.collection.mutable.WrappedArray.ofRef;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import models.PriceData;
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
		SparkConf confInitial = new SparkConf().setMaster("local[*]").setAppName("StockAnalyser");
		confInitial.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		confInitial.set("spark.kryo.registrator","models.StockKryoRegistrator");
		// force kryo to throw an exception when it tries to serialize 
		// an unregistered class
		confInitial.set("spark.kryo.registrationRequired","true");
		
		
		JavaStreamingContext jssc = new JavaStreamingContext(confInitial, Durations.seconds(60));
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
		JavaDStream<String> lines = jssc.textFileStream("/Users/kumarkunal/Upgrad_materials/Course5-Mod7-SparkStream/Scripts_Shell/destination");
		lines.print();
		JavaDStream<Map<String, StockPrice>> stockStream = Analyser.convertIntoDStream(lines);
		stockStream.print();
		JavaPairDStream<String, PriceData> windowStockDStream = getWindowDStream(stockStream);
		windowStockDStream.print();
		jssc.start();
		jssc.awaitTermination();
		jssc.close();

	}
	
	private static JavaDStream<Map<String, StockPrice>>convertIntoDStream(JavaDStream<String> json){
		
		return json.map(x -> {
			System.out.println(x);
			ObjectMapper mapper = new ObjectMapper();
			TypeReference<List<StockPrice>> mapType = new
			TypeReference<List<StockPrice>>() {
			};
			//System.out.println("maptype:"+mapType);
			List<StockPrice> list = mapper.readValue(x, mapType);
			System.out.println(list);
			Map<String, StockPrice> map = new HashMap<>();
			for (StockPrice sp : list) {
			map.put(sp.getSymbol(), sp);
			}
			return map;
			});
	}
	
	private static JavaPairDStream<String, PriceData> getWindowDStream(JavaDStream<Map<String, StockPrice>> stockStream){
		//JavaPairDStream< String, PriceData> stockPriceStream 
		JavaPairDStream<String, PriceData> stockPriceMSFTStream = getPriceDStream(stockStream, "MSFT");
		JavaPairDStream<String, PriceData> stockPriceGoogleStream= getPriceDStream(stockStream, "GOOGL");
		JavaPairDStream<String, PriceData> stockPriceADBEStream = getPriceDStream(stockStream, "ADBE");
		JavaPairDStream<String, PriceData> stockPriceFBStream = getPriceDStream(stockStream, "FB");
		
		JavaPairDStream<String, PriceData> windowMSFTDStream = stockPriceMSFTStream.reduceByKeyAndWindow(
				SUM_REDUCER_PRICE_DATA,
				DIFF_REDUCER_PRICE_DATA, Durations.minutes(10),
				Durations.minutes(5));
		JavaPairDStream<String, PriceData> windowGoogDStream =
				stockPriceGoogleStream.reduceByKeyAndWindow(
				SUM_REDUCER_PRICE_DATA,
				DIFF_REDUCER_PRICE_DATA, Durations.minutes(10),
				Durations.minutes(5));
		
		JavaPairDStream<String, PriceData> windowAdbDStream =
				stockPriceADBEStream.reduceByKeyAndWindow(
				SUM_REDUCER_PRICE_DATA,
				DIFF_REDUCER_PRICE_DATA, Durations.minutes(10),
				Durations.minutes(5));
		
		JavaPairDStream<String, PriceData> windowFBDStream =
				stockPriceFBStream.reduceByKeyAndWindow(
				SUM_REDUCER_PRICE_DATA,
				DIFF_REDUCER_PRICE_DATA, Durations.minutes(10),
				Durations.minutes(5));
		windowMSFTDStream =
				windowMSFTDStream.union(windowGoogDStream).union(windowAdbDStream).union(windowFBDStream);
		
		
		return windowMSFTDStream;
	}
	
	private static JavaPairDStream<String, PriceData> getPriceDStream(JavaDStream<Map<String, StockPrice>> stockStream,String symbol){

		JavaPairDStream<String, PriceData> stockPriceStream = stockStream.mapToPair(new PairFunction<Map<String, StockPrice>,String, PriceData>() {
			public Tuple2<String, PriceData> call(Map<String, StockPrice> map) throws Exception {
				if (map.containsKey(symbol)) {
					return new Tuple2<String,PriceData>(symbol, map.get(symbol).getPriceData());
				} else {
					return new Tuple2<String,PriceData>(symbol, new PriceData());
				}
			}
		});

		return stockPriceStream;

	}
	
	private static Function2<PriceData, PriceData, PriceData>
	SUM_REDUCER_PRICE_DATA = (a, b) -> {
	PriceData pd = new PriceData();
	pd.setOpen(a.getOpen() + b.getOpen());
	pd.setClose(a.getClose() + b.getClose());
	return pd;
	};
	
	private static Function2<PriceData, PriceData, PriceData>
	DIFF_REDUCER_PRICE_DATA = (a, b) -> {
	PriceData pd = new PriceData();
	pd.setOpen(a.getOpen() - b.getOpen());
	pd.setClose(a.getClose() - b.getClose());
	return pd;
	};

}
