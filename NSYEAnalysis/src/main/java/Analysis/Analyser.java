package Analysis;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import scala.collection.mutable.WrappedArray.ofRef;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import models.TupleSorter;

import models.AveragePrice;
import models.PriceData;
import models.RSIData;
import models.RSIDataAverage;
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
        String outputPathFiles = "/Users/kumarkunal/Upgrad_materials/Course5-Mod7-SparkStream/SparkProjects/OutputFiles14";
        String lastRSIDataAvergaeRddKeyname = "lastRSIDataAvergaeRdd";
		SparkConf confInitial = new SparkConf().setMaster("local[*]").setAppName("StockAnalyser");
		
		/**
		 * Kryo Serializer being used
		 */
		confInitial.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		confInitial.set("spark.kryo.registrator","models.StockKryoRegistrator");
		// force kryo to throw an exception when it tries to serialize 
		// an unregistered class
		confInitial.set("spark.kryo.registrationRequired","true");
		
		
		JavaStreamingContext jssc = new JavaStreamingContext(confInitial, Durations.seconds(60));
		jssc.checkpoint("checkpoint_dir");
		Logger.getRootLogger().setLevel(Level.ERROR);
		//JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
		/* Implementing window operations*/
		JavaDStream<String> lines = jssc.textFileStream("/Users/kumarkunal/Upgrad_materials/Course5-Mod7-SparkStream/Scripts_Shell/destination");
		lines.print();
		JavaDStream<Map<String, StockPrice>> stockStream = Analyser.convertIntoDStream(lines);
		//stockStream.print();
		//JavaPairDStream<String, PriceData> windowStockDStream = getWindowDStream(stockStream);
		//JavaPairDStream<String, PriceData> windowStockDStream = StreamTransformer.getPDWindowDStream(stockStream);
		
		/**
		 * Get Average Closing Price for Each Stock. 
		 * Get Average Opening Price for each stock and then select the stock with maximum profit
		 * @Param stockStream obtained by converting String into DSteramn of JavaDStream<Map<String, StockPrice>> 
		 * @Return <String, Tuple2<PriceData, Long>> windowStockDStream 
		 * @Operation windowStockDStream will be used to calculate JavaPairRDD<String, Double> stockAverageRdd in window.
		 * 
		 */
		JavaPairDStream<String, Tuple2<PriceData, Long>> windowStockDStream = StreamTransformer.getStockPDandCountWindowDStream(stockStream);
		
		windowStockDStream.foreachRDD(rdd ->{
			if(!rdd.isEmpty()) {
				/* Average closing price of each stock */
				JavaPairRDD<String,Tuple2<Double,Long>> closingPriceAndCountRdd = StreamTransformer.getActualPriceAndCounntRdd(rdd, "close");
				closingPriceAndCountRdd.coalesce(1).saveAsTextFile(outputPathFiles + java.io.File.separator + "windowUnionStocks_Closinng_Count_" + System.currentTimeMillis());
				JavaPairRDD<String, Double> stockAverageRdd = StreamTransformer.getAveragePriceRdd(closingPriceAndCountRdd);
				stockAverageRdd.coalesce(1).saveAsTextFile(outputPathFiles + java.io.File.separator + "windowUnionStocks_AveragePrice_Closing" + System.currentTimeMillis());
				
				/*Calculating average opening price of each stock and then deciding with maximum profit*/
				JavaPairRDD<String,Tuple2<Double,Long>> openingPriceAndCountRdd = StreamTransformer.getActualPriceAndCounntRdd(rdd, "open");
				JavaPairRDD<String, Double> stockOpenPriceAverageRdd = StreamTransformer.getAveragePriceRdd(openingPriceAndCountRdd);
				stockOpenPriceAverageRdd.coalesce(1).saveAsTextFile(outputPathFiles + java.io.File.separator + "windowUnionStocks_AveragePrice_Opening" + System.currentTimeMillis());
				
				/* Joining the average Closing Price and Opening price and calculating difference*/
				JavaPairRDD<String,Tuple2<Double,Double>> stockClosingAvgAndOpeningAvgRdd = stockAverageRdd.join(stockOpenPriceAverageRdd);
				JavaPairRDD<String,Double> stockAveragePriceDifference = StreamTransformer.getAveragePriceDifferenceRdd(stockClosingAvgAndOpeningAvgRdd);
				stockAveragePriceDifference.coalesce(1).saveAsTextFile(outputPathFiles + java.io.File.separator + "windowUnionStocks_AveragePrice_Difference_" + System.currentTimeMillis());
				
				/* First Finding the Maximum Stock Tuple. Then Filterinng out that Rdd */
				Tuple2<String, Double> stockAveragePriceDifferenceTuple = stockAveragePriceDifference.max(new TupleSorter());		
				JavaPairRDD<String,Double> stockAveragePriceDifferenceMax = stockAveragePriceDifference.filter(new Function<Tuple2<String,Double>, Boolean>() {
					
					private static final long serialVersionUID = 1L;
					
					public Boolean call(Tuple2<String, Double> aStockAveragePriceDifference) throws Exception{
						String symbol = aStockAveragePriceDifference._1;
						String matchinngSignal = stockAveragePriceDifferenceTuple._1;
						if(symbol.equalsIgnoreCase(matchinngSignal)) {
							return true;
						}else {
							return false;
						}
					}
				}
				);
				stockAveragePriceDifferenceMax.coalesce(1).saveAsTextFile(outputPathFiles + java.io.File.separator + "windowUnionStocks_AveragePrice_Difference_Maximum" + System.currentTimeMillis());
				
			}
		});

		/**
		 * Get Volume of Stock.
		 * @Operation windowStockDStream will be used to calculate JavaPairRDD<String, Long> stockAggregated Volume in window(Size=10, sliding=10).
		 * 
		 */
		JavaPairDStream<String, Double> windowStockVolumeDStream = StreamTransformer.getStockVolumeWindowDStream(stockStream);
		windowStockVolumeDStream.foreachRDD(rdd -> {
			if(!rdd.isEmpty()) {
				rdd.coalesce(1).saveAsTextFile(outputPathFiles + java.io.File.separator + "windowUnionStocks_Volume_Aggregated_" + System.currentTimeMillis());
				/* Finding stock with maximu  volume and then filetring to print its value.*/
				Tuple2<String, Double> stockVolumeMaxTuple = rdd.max(new TupleSorter());
				JavaPairRDD<String,Double> stockVolumeMaxRdd = rdd.filter(new Function<Tuple2<String,Double>, Boolean>() {
					
					private static final long serialVersionUID = 1L;
					
					public Boolean call(Tuple2<String, Double> aStockVolume) throws Exception{
						String symbol = aStockVolume._1;
						String matchinngSignal = stockVolumeMaxTuple._1;
						if(symbol.equalsIgnoreCase(matchinngSignal)) {
							return true;
						}else {
							return false;
						}
					}
				}
				);
				stockVolumeMaxRdd.coalesce(1).saveAsTextFile(outputPathFiles + java.io.File.separator + "windowUnionStocks_Volume_Maximum" + System.currentTimeMillis());
			}
		}
		);
		
		
		/**
		 * Get RSI of Stock in window(size =10minute, sliding=1minute)
		 * @Operation windowStockDStream will be used to calculate JavaPairRDD<String, Tuple<Double,Double>> stockAggregated gains and losses in window(Size=10, sliding=1).
		 * In the Tuple considered, the first double value is sum of all gains in window. The second Double value is of sum of all losses in window.
		 * Losses would be positive.
		 * 
		 */
		
		JavaPairDStream<String, Tuple2<RSIData, Long>> windowStockRSIDStream = StreamTransformer.getRSIDataWindowDStream(stockStream);
		//JavaPairRDD<String, RSIDataAverage> previousWindowRSIDataAverageRdd = null;
		//RSIDataAverage previousRSIDataAvergae = null;
		HashMap<String, JavaPairRDD<String, RSIDataAverage>> anHashRSIDataAverageMap = new HashMap<String, JavaPairRDD<String, RSIDataAverage>>();
		windowStockRSIDStream.foreachRDD(rdd ->{
			if(!rdd.isEmpty()) {
				
				/*Map<String, Tuple2<RSIData,Long>> aMap = rdd.collectAsMap();
				long totalSlideCounter = aMap.get("MSFT")._2;
 				if(totalSlideCounter == 10 && anHashRSIDataAverageMap.get(lastRSIDataAvergaeRddKeyname) == null) {
					JavaPairRDD<String, RSIDataAverage> windowRSIDataAverageRdd = StreamTransformer.getRSIDataAverageRdd(null, rdd);
					anHashRSIDataAverageMap.put(lastRSIDataAvergaeRddKeyname, windowRSIDataAverageRdd);
					windowRSIDataAverageRdd.coalesce(1).saveAsTextFile(outputPathFiles + java.io.File.separator + "windowUnionStocks_RSIDATA_Average_First_" + System.currentTimeMillis());
				}else if(totalSlideCounter == 10 && anHashRSIDataAverageMap.get(lastRSIDataAvergaeRddKeyname) != null){
					JavaPairRDD<String, RSIDataAverage> previousWindowRSIDataAverageRdd = anHashRSIDataAverageMap.get(lastRSIDataAvergaeRddKeyname);
					JavaPairRDD<String, RSIDataAverage> windowRSIDataAverageRdd = StreamTransformer.getRSIDataAverageRdd(previousWindowRSIDataAverageRdd, rdd);
					anHashRSIDataAverageMap.put(lastRSIDataAvergaeRddKeyname, windowRSIDataAverageRdd);
					windowRSIDataAverageRdd.coalesce(1).saveAsTextFile(outputPathFiles + java.io.File.separator + "windowUnionStocks_RSIDATA_Average_Socond_Onwards_" + System.currentTimeMillis());
				}else if(totalSlideCounter < 10) {
					rdd.coalesce(1).saveAsTextFile(outputPathFiles + java.io.File.separator + "windowUnionStocks_RSIDATA_Before_WindowSize_" + System.currentTimeMillis());
				}*/
				
				JavaPairRDD<String, Double> windowRSIValueRdd = StreamTransformer.getRSIValueRdd(rdd);
				windowRSIValueRdd.coalesce(1).saveAsTextFile(outputPathFiles + java.io.File.separator + "windowUnionStocks_RSI_Values_" + System.currentTimeMillis());
				
			}
			
		});
		
		
		
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
