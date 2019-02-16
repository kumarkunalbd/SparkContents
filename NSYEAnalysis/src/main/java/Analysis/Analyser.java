package Analysis;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
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

import models.AveragePrice;
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
		String inputPathFiles = "/Users/kumarkunal/Upgrad_materials/Course5-Mod7-SparkStream/Scripts_Shell/destination";
        String outputPathFiles = "/Users/kumarkunal/Upgrad_materials/Course5-Mod7-SparkStream/SparkProjects/OutputFiles5";
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
		JavaDStream<String> lines = jssc.textFileStream("/Users/kumarkunal/Upgrad_materials/Course5-Mod7-SparkStream/Scripts_Shell/destination");
		lines.print();
		JavaDStream<Map<String, StockPrice>> stockStream = Analyser.convertIntoDStream(lines);
		//stockStream.print();
		//JavaPairDStream<String, PriceData> windowStockDStream = getWindowDStream(stockStream);
		//JavaPairDStream<String, PriceData> windowStockDStream = StreamTransformer.getPDWindowDStream(stockStream);
		JavaPairDStream<String, Tuple2<PriceData, Long>> windowStockDStream = StreamTransformer.getStockPDandCountWindowDStream(stockStream);
		
		windowStockDStream.foreachRDD(rdd ->{
			if(!rdd.isEmpty()) {
				//rdd.saveAsTextFile("/Users/kumarkunal/Upgrad_materials/Course5-Mod7-SparkStream/SparkProjects/OutputFiles");
				//JavaPairRDD<String, Double> closingPriceRdd = Analyser.getActualPriceRdd(rdd, "close");
				JavaPairRDD<String,Tuple2<Double,Long>> closingPriceAndCountRdd = StreamTransformer.getActualPriceAndCounntRdd(rdd, "close");
				closingPriceAndCountRdd.coalesce(1).saveAsTextFile(outputPathFiles + java.io.File.separator + "windowUnionStocks_Closinng_Count_" + System.currentTimeMillis());
				JavaPairRDD<String, Double> stockAverageRdd = StreamTransformer.getAveragePriceRdd(closingPriceAndCountRdd);
				stockAverageRdd.coalesce(1).saveAsTextFile(outputPathFiles + java.io.File.separator + "windowUnionStocks_AveragePrice_Closing" + System.currentTimeMillis());
			}
		});
		/*JavaPairDStream<Tuple2<String, PriceData>,Long> windowStockDStreamCount = Analyser.getWindowDStream(stockStream);
		windowStockDStreamCount.foreachRDD(rdd ->{
			if(!rdd.isEmpty()) {
				rdd.saveAsTextFile("/Users/kumarkunal/Upgrad_materials/Course5-Mod7-SparkStream/SparkProjects/OutputFiles3");
				JavaPairRDD<String, Double> closingPriceRdd = Analyser.getAveragePriceRdd(rdd, "close");
				closingPriceRdd.coalesce(1).saveAsTextFile(outputPathFiles + java.io.File.separator + "windowUnionStocks_Average_" + System.currentTimeMillis());
				
			}
		});*/
		//windowStockDStream.saveAsHadoopFiles(prefix, suffix);
		jssc.start();
		jssc.awaitTermination();
		jssc.close();

	}
	
	private static JavaDStream<Map<String, StockPrice>>convertIntoDStream(JavaDStream<String> json){
		
		return json.map(x -> {
			//System.out.println(x);
			ObjectMapper mapper = new ObjectMapper();
			TypeReference<List<StockPrice>> mapType = new
			TypeReference<List<StockPrice>>() {
			};
			//System.out.println("maptype:"+mapType);
			List<StockPrice> list = mapper.readValue(x, mapType);
			//System.out.println(list);
			Map<String, StockPrice> map = new HashMap<>();
			for (StockPrice sp : list) {
			map.put(sp.getSymbol(), sp);
			}
			return map;
			});
	}
	
	private static JavaPairDStream<Tuple2<String, PriceData>,Long> getWindowDStream(JavaDStream<Map<String, StockPrice>> stockStream){
		//JavaPairDStream< String, PriceData> stockPriceStream 
		JavaPairDStream<String, PriceData> stockPriceMSFTStream = getPriceDStream(stockStream, "MSFT");
		//stockPriceMSFTStream.print();
		JavaPairDStream<String, PriceData> stockPriceGoogleStream= getPriceDStream(stockStream, "GOOGL");
		//stockPriceGoogleStream.print();
		JavaPairDStream<String, PriceData> stockPriceADBEStream = getPriceDStream(stockStream, "ADBE");
		//stockPriceADBEStream.print();
		JavaPairDStream<String, PriceData> stockPriceFBStream = getPriceDStream(stockStream, "FB");
		//stockPriceFBStream.print();
		
		JavaPairDStream<String, PriceData> windowMSFTDStream = stockPriceMSFTStream.reduceByKeyAndWindow(
				SUM_REDUCER_PRICE_DATA,
				DIFF_REDUCER_PRICE_DATA, Durations.minutes(10),
				Durations.minutes(5));
		
		JavaPairDStream<Tuple2<String, PriceData>, Long> windowMSFTDtreamCount = windowMSFTDStream.countByValueAndWindow(Durations.minutes(10), Durations.minutes(5));
		JavaPairDStream<String, PriceData> windowGoogDStream =
				stockPriceGoogleStream.reduceByKeyAndWindow(
				SUM_REDUCER_PRICE_DATA,
				DIFF_REDUCER_PRICE_DATA, Durations.minutes(10),
				Durations.minutes(5));
		
		JavaPairDStream<Tuple2<String, PriceData>, Long> windowGoogDtreamCount = windowGoogDStream.countByValueAndWindow(Durations.minutes(10), Durations.minutes(5));
		
		JavaPairDStream<String, PriceData> windowAdbDStream =
				stockPriceADBEStream.reduceByKeyAndWindow(
				SUM_REDUCER_PRICE_DATA,
				DIFF_REDUCER_PRICE_DATA, Durations.minutes(10),
				Durations.minutes(5));
		
		JavaPairDStream<Tuple2<String, PriceData>, Long> windowAdbDtreamCount = windowAdbDStream.countByValueAndWindow(Durations.minutes(10), Durations.minutes(5));
		
		JavaPairDStream<String, PriceData> windowFBDStream =
				stockPriceFBStream.reduceByKeyAndWindow(
				SUM_REDUCER_PRICE_DATA,
				DIFF_REDUCER_PRICE_DATA, Durations.minutes(10),
				Durations.minutes(5));
		JavaPairDStream<Tuple2<String, PriceData>, Long> windowFBDtreamCount = windowFBDStream.countByValueAndWindow(Durations.minutes(10), Durations.minutes(5));
		windowMSFTDStream =
				windowMSFTDStream.union(windowGoogDStream).union(windowAdbDStream).union(windowFBDStream);
		windowFBDtreamCount = windowFBDtreamCount.union(windowGoogDtreamCount).union(windowAdbDtreamCount).union(windowFBDtreamCount);
		
		
		return windowFBDtreamCount;
	}
	
	public static JavaPairDStream<String, PriceData> getPriceDStream(JavaDStream<Map<String, StockPrice>> stockStream,String symbol){

		JavaPairDStream<String, PriceData> stockPriceStream = stockStream.mapToPair(new PairFunction<Map<String, StockPrice>,String, PriceData>() {
			/**
			 * Adding serialization
			 */
			private static final long serialVersionUID = 1L;

			public Tuple2<String, PriceData> call(Map<String, StockPrice> map) throws Exception {
				if (map.containsKey(symbol)) {
					Tuple2<String, PriceData> strPriceTuple = new Tuple2<String,PriceData>(symbol, map.get(symbol).getPriceData());
					return strPriceTuple;
					//return new Tuple2<String,PriceData>(symbol, map.get(symbol).getPriceData());
				} else {
					Tuple2<String, PriceData> strPriceTuple = new Tuple2<String,PriceData>(symbol, new PriceData());
					return strPriceTuple;
					//return new Tuple2<String,PriceData>(symbol, new PriceData());
				}
			}
		});

		return stockPriceStream;

	}
	
	public static JavaPairRDD<String, Double> getActualPriceRdd(JavaPairRDD<String, PriceData> stockPriceRdd, String priceType){
		JavaPairRDD<String, Double> actualPriceRdd = stockPriceRdd.mapToPair(new PairFunction<Tuple2<String,PriceData>, String, Double>() {
			private static final long serialVersionUID = 1L;
			
			public Tuple2<String, Double> call(Tuple2<String,PriceData> aStockPD) throws Exception{
				Double actualPrice=(double) 0;
				
				String symbol = aStockPD._1;
				PriceData stockPDonly = aStockPD._2;
				if(priceType.equalsIgnoreCase("close")) {
					actualPrice = stockPDonly.getClose();
				}else if(priceType.equalsIgnoreCase("open")) {
					actualPrice = stockPDonly.getOpen();
				}
				return new Tuple2<String, Double>(symbol, actualPrice);
			}
			
		});
		return actualPriceRdd;
	}
	
	private static JavaPairRDD<String, Double> getAveragePriceRdd(JavaPairRDD<Tuple2<String, PriceData>, Long> stockPriceCountRdd, String priceType){
		
		JavaPairRDD<String, Double> averagePriceRdd = stockPriceCountRdd.mapToPair(new PairFunction<Tuple2<Tuple2<String,PriceData>,Long>, String, Double>() {
			private static final long serialVersionUID = 1L;
			
			public Tuple2<String, Double> call(Tuple2<Tuple2<String,PriceData>,Long> aStockCount) throws Exception{
				Double average = (double) 0;
				Double actualPriceAggeragted = (double) 0;
				String symbol = aStockCount._1._1;
				Long count = aStockCount._2;
				PriceData stockPDonly = aStockCount._1._2;
				if(priceType.equalsIgnoreCase("close")) {
					actualPriceAggeragted = stockPDonly.getClose();
					average = stockPDonly.getClose()/count;
				}else if(priceType.equalsIgnoreCase("open")) {
					actualPriceAggeragted = stockPDonly.getOpen();
					average = stockPDonly.getOpen()/count;
				}
				System.out.println("Stock :::" + symbol + " : TotalCountWindow:::"+count+" : AggregatePrice:::"+actualPriceAggeragted+" : Average:::"+average);
				return new Tuple2<String, Double>(symbol, average);
			}
		});
		
		return averagePriceRdd;
	}
	
	public static Function2<PriceData, PriceData, PriceData>
	SUM_REDUCER_PRICE_DATA = (a, b) -> {
	PriceData pd = new PriceData();
	pd.setOpen(a.getOpen() + b.getOpen());
	pd.setClose(a.getClose() + b.getClose());
	return pd;
	};
	
	public static Function2<PriceData, PriceData, PriceData>
	DIFF_REDUCER_PRICE_DATA = (a, b) -> {
	PriceData pd = new PriceData();
	pd.setOpen(a.getOpen() - b.getOpen());
	pd.setClose(a.getClose() - b.getClose());
	return pd;
	};

}
