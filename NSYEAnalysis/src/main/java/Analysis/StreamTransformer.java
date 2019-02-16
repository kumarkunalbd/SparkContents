package Analysis;

import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import models.PriceData;
import models.StockPrice;
import scala.Tuple2;

public class StreamTransformer {
	
	
	public static JavaPairDStream<String, PriceData> getPDWindowDStream(JavaDStream<Map<String, StockPrice>> stockStream){
		//JavaPairDStream< String, PriceData> stockPriceStream 
		JavaPairDStream<String, PriceData> stockPriceMSFTStream = Analyser.getPriceDStream(stockStream, "MSFT");
		//stockPriceMSFTStream.print();
		JavaPairDStream<String, PriceData> stockPriceGoogleStream= Analyser.getPriceDStream(stockStream, "GOOGL");
		//stockPriceGoogleStream.print();
		JavaPairDStream<String, PriceData> stockPriceADBEStream = Analyser.getPriceDStream(stockStream, "ADBE");
		//stockPriceADBEStream.print();
		JavaPairDStream<String, PriceData> stockPriceFBStream = Analyser.getPriceDStream(stockStream, "FB");
		//stockPriceFBStream.print();
		
		JavaPairDStream<String, PriceData> windowMSFTDStream = stockPriceMSFTStream.reduceByKeyAndWindow(
				Analyser.SUM_REDUCER_PRICE_DATA,
				Analyser.DIFF_REDUCER_PRICE_DATA, Durations.minutes(10),
				Durations.minutes(5));
		
		
		JavaPairDStream<String, PriceData> windowGoogDStream =
				stockPriceGoogleStream.reduceByKeyAndWindow(
				Analyser.SUM_REDUCER_PRICE_DATA,
				Analyser.DIFF_REDUCER_PRICE_DATA, Durations.minutes(10),
				Durations.minutes(5));
		
		
		
		JavaPairDStream<String, PriceData> windowAdbDStream =
				stockPriceADBEStream.reduceByKeyAndWindow(
				Analyser.SUM_REDUCER_PRICE_DATA,
				Analyser.DIFF_REDUCER_PRICE_DATA, Durations.minutes(10),
				Durations.minutes(5));
		
		
		
		JavaPairDStream<String, PriceData> windowFBDStream =
				stockPriceFBStream.reduceByKeyAndWindow(
				Analyser.SUM_REDUCER_PRICE_DATA,
				Analyser.DIFF_REDUCER_PRICE_DATA, Durations.minutes(10),
				Durations.minutes(5));
		
		windowMSFTDStream =
				windowMSFTDStream.union(windowGoogDStream).union(windowAdbDStream).union(windowFBDStream);
		
		
		
		return windowMSFTDStream;
	}
	
	public static JavaPairDStream<String,Tuple2<PriceData, Long>> getStockPDandCountWindowDStream(JavaDStream<Map<String, StockPrice>> stockStream){
				
				JavaPairDStream<String, PriceData> stockPriceMSFTStream = Analyser.getPriceDStream(stockStream, "MSFT");
				JavaPairDStream<String, PriceData> stockPriceGoogleStream= Analyser.getPriceDStream(stockStream, "GOOGL");
				JavaPairDStream<String, PriceData> stockPriceADBEStream = Analyser.getPriceDStream(stockStream, "ADBE");
				JavaPairDStream<String, PriceData> stockPriceFBStream = Analyser.getPriceDStream(stockStream, "FB");
				
				JavaPairDStream<String, Long> stockPriceMSFTCounntStream = StreamTransformer.getStockCountDStream(stockStream, "MSFT");
				JavaPairDStream<String, Long> stockPriceGoogleCounntStream = StreamTransformer.getStockCountDStream(stockStream, "GOOGL");
				JavaPairDStream<String, Long> stockPriceADBECounntStream = StreamTransformer.getStockCountDStream(stockStream, "ADBE");
				JavaPairDStream<String, Long> stockPriceFBCounntStream = StreamTransformer.getStockCountDStream(stockStream, "FB");
				
				JavaPairDStream<String, PriceData> windowMSFTDStream = stockPriceMSFTStream.reduceByKeyAndWindow(
						Analyser.SUM_REDUCER_PRICE_DATA,
						Analyser.DIFF_REDUCER_PRICE_DATA, Durations.minutes(10),
						Durations.minutes(5));
				JavaPairDStream<String, Long> windowMSFTDCountStream = stockPriceMSFTCounntStream.reduceByKeyAndWindow(SUM_REDUCER_COUNT, DIFF_REDUCER_COUNT, Durations.minutes(10), Durations.minutes(5));
				JavaPairDStream<String, Tuple2<PriceData, Long>> windowMSFTJoinedDStream= windowMSFTDStream.join(windowMSFTDCountStream);
				/*JavaPairDStream<Tuple2<String, PriceData>, Long> windowMSFTCountDStream = stockPriceMSFTStream.countByValueAndWindow(Durations.minutes(10), Durations.minutes(5));
				System.out.println("Print of windowMSFTCountDStream");
				windowMSFTCountDStream.print();*/
				
				JavaPairDStream<String, PriceData> windowGoogDStream =
						stockPriceGoogleStream.reduceByKeyAndWindow(
						Analyser.SUM_REDUCER_PRICE_DATA,
						Analyser.DIFF_REDUCER_PRICE_DATA, Durations.minutes(10),
						Durations.minutes(5));
				JavaPairDStream<String, Long> windowGoogCountDStream = stockPriceGoogleCounntStream.reduceByKeyAndWindow(SUM_REDUCER_COUNT, DIFF_REDUCER_COUNT, Durations.minutes(10), Durations.minutes(5));
				JavaPairDStream<String, Tuple2<PriceData, Long>> windowGoogleJoinedDStream= windowGoogDStream.join(windowGoogCountDStream);
				/*JavaPairDStream<Tuple2<String, PriceData>, Long> windowGoogleCountDStream = stockPriceGoogleStream.countByValueAndWindow(Durations.minutes(10), Durations.minutes(5));
				System.out.println("Print of windowMSFTCountDStream");*/
				
				JavaPairDStream<String, PriceData> windowAdbDStream =
						stockPriceADBEStream.reduceByKeyAndWindow(
						Analyser.SUM_REDUCER_PRICE_DATA,
						Analyser.DIFF_REDUCER_PRICE_DATA, Durations.minutes(10),
						Durations.minutes(5));
				JavaPairDStream<String, Long> windowAdbCountDStream = stockPriceADBECounntStream.reduceByKeyAndWindow(SUM_REDUCER_COUNT, DIFF_REDUCER_COUNT, Durations.minutes(10), Durations.minutes(5));
				JavaPairDStream<String, Tuple2<PriceData, Long>> windowAdbJoinedDStream= windowAdbDStream.join(windowAdbCountDStream);
				
				JavaPairDStream<String, PriceData> windowFBDStream =
						stockPriceFBStream.reduceByKeyAndWindow(
						Analyser.SUM_REDUCER_PRICE_DATA,
						Analyser.DIFF_REDUCER_PRICE_DATA, Durations.minutes(10),
						Durations.minutes(5));
				JavaPairDStream<String, Long> windowFBCounntDStream = stockPriceFBCounntStream.reduceByKeyAndWindow(SUM_REDUCER_COUNT, DIFF_REDUCER_COUNT, Durations.minutes(10), Durations.minutes(5));
				JavaPairDStream<String, Tuple2<PriceData, Long>> windowFBJoinedDStream= windowFBDStream.join(windowFBCounntDStream);
				
				windowMSFTJoinedDStream = windowMSFTJoinedDStream.union(windowGoogleJoinedDStream).union(windowAdbJoinedDStream).union(windowFBJoinedDStream);
				
				
		return windowMSFTJoinedDStream;
	}
	
	public static JavaPairDStream<String, Long> getStockCountDStream(JavaDStream<Map<String, StockPrice>> stockStream,String symbol){

		JavaPairDStream<String, Long> stockPriceStream = stockStream.mapToPair(new PairFunction<Map<String, StockPrice>,String, Long>() {
			/**
			 * Adding serialization
			 */
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Long> call(Map<String, StockPrice> map) throws Exception {
				if (map.containsKey(symbol)) {
					Tuple2<String, Long> strPriceTuple = new Tuple2<String,Long>(symbol, (long) 1);
					return strPriceTuple;
					//return new Tuple2<String,PriceData>(symbol, map.get(symbol).getPriceData());
				} else {
					Tuple2<String, Long> strPriceTuple = new Tuple2<String,Long>(symbol, (long) 1);
					return strPriceTuple;
					//return new Tuple2<String,PriceData>(symbol, new PriceData());
				}
			}
		});

		return stockPriceStream;

	}
	
	public static JavaPairRDD<String, Tuple2<Double, Long>> getActualPriceAndCounntRdd(JavaPairRDD<String, Tuple2<PriceData, Long>> stockPriceAndCountRdd, String priceType){
		JavaPairRDD<String, Tuple2<Double, Long>> actualProceAndCountRdd = stockPriceAndCountRdd.mapToPair(new PairFunction<Tuple2<String,Tuple2<PriceData,Long>>, String, Tuple2<Double,Long>>() {
			
			private static final long serialVersionUID = 1L;
			public Tuple2<String, Tuple2<Double, Long>> call(Tuple2<String, Tuple2<PriceData, Long>> aStockPDandCount) throws Exception{
				Double average = (double) 0;
				Double actualPriceAggeragted = (double) 0;
				Long countAggregated = aStockPDandCount._2._2;
				String symbol = aStockPDandCount._1;
				PriceData aStockPD = aStockPDandCount._2._1;
				if(priceType.equalsIgnoreCase("close")) {
					actualPriceAggeragted = aStockPD.getClose();
				}else if(priceType.equalsIgnoreCase("open")) {
					actualPriceAggeragted = aStockPD.getOpen();
				}
				Tuple2<Double, Long> agrregatedPriceAdnCount = new Tuple2<Double, Long>(actualPriceAggeragted, countAggregated);
				return new Tuple2<String, Tuple2<Double,Long>>(symbol,agrregatedPriceAdnCount);
				
				
			}
		}
		);
		return actualProceAndCountRdd;
	}
	
	public static Function2<Long, Long, Long>
	SUM_REDUCER_COUNT = (a, b) -> {
	return a+b;
	};
	
	public static Function2<Long, Long, Long>
	DIFF_REDUCER_COUNT = (a, b) -> {
	return a-b;
	};

}
