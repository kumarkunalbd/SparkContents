package Analysis;

import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import models.PriceData;
import models.RSIData;
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
	
	public static JavaPairRDD<String, Double> getAveragePriceRdd(JavaPairRDD<String, Tuple2<Double, Long>> stockActualProceAndCountRdd){
		
		JavaPairRDD<String, Double> averagePriceRdd = stockActualProceAndCountRdd.mapToPair(new PairFunction<Tuple2<String,Tuple2<Double,Long>>, String, Double>() {
			private static final long serialVersionUID = 1L;
			
			public Tuple2<String, Double> call(Tuple2<String, Tuple2<Double, Long>> aStockActualPriceAndCount) throws Exception{
				String symbol = aStockActualPriceAndCount._1;
				Double stockAggreagtedPrice = aStockActualPriceAndCount._2._1;
				Long stockAggeragtedCounnt = aStockActualPriceAndCount._2._2;
				Double stockAverage = stockAggreagtedPrice/stockAggeragtedCounnt;
				
				return new Tuple2<String, Double>(symbol,stockAverage);
			}
		}
		);
		
		return averagePriceRdd;
	}
	
	public static JavaPairRDD<String, Double> getAveragePriceDifferenceRdd(JavaPairRDD<String, Tuple2<Double, Double>> aStockClosingAvgAndOpeningAvgRdd){
		JavaPairRDD<String, Double> priceDifferenceRdd = aStockClosingAvgAndOpeningAvgRdd.mapToPair(new PairFunction<Tuple2<String,Tuple2<Double,Double>>, String, Double>() {
			private static final long serialVersionUID = 1L;
			
			public Tuple2<String, Double> call(Tuple2<String, Tuple2<Double, Double>> aStockClosingAvgAndOpeningAvg) throws Exception{
				String symbol = aStockClosingAvgAndOpeningAvg._1;
				Double closingAvg = aStockClosingAvgAndOpeningAvg._2._1;
				Double openingAvg = aStockClosingAvgAndOpeningAvg._2._2;
				Double profitLoss = closingAvg-openingAvg;
				
				return new Tuple2<String, Double>(symbol, profitLoss);
			}
		}
		
		);
		return priceDifferenceRdd;
		
	}
	
	public static JavaPairDStream<String,Double> getStockVolumeWindowDStream(JavaDStream<Map<String, StockPrice>> stockStream){
		JavaPairDStream<String, Double> stockVolumeMSFTStream = StreamTransformer.getVolumeDStream(stockStream, "MSFT");
		JavaPairDStream<String, Double> stockVolumeGoogleStream= StreamTransformer.getVolumeDStream(stockStream, "GOOGL");
		JavaPairDStream<String, Double> stockVolumeADBEStream = StreamTransformer.getVolumeDStream(stockStream, "ADBE");
		JavaPairDStream<String, Double> stockVolumeFBStream = StreamTransformer.getVolumeDStream(stockStream, "FB");
		
		JavaPairDStream<String, Double> windowMSFTVolumeDStream = stockVolumeMSFTStream.reduceByKeyAndWindow(
				StreamTransformer.SUM_REDUCER_VOLUME,
				StreamTransformer.DIFF_REDUCER_VOLUME, Durations.minutes(10),
				Durations.minutes(10));
		JavaPairDStream<String, Double> windowGoogleVolumeDStream = stockVolumeGoogleStream.reduceByKeyAndWindow(
				StreamTransformer.SUM_REDUCER_VOLUME,
				StreamTransformer.DIFF_REDUCER_VOLUME, Durations.minutes(10),
				Durations.minutes(10));
		JavaPairDStream<String, Double> windowADBEVolumeDStream = stockVolumeADBEStream.reduceByKeyAndWindow(
				StreamTransformer.SUM_REDUCER_VOLUME,
				StreamTransformer.DIFF_REDUCER_VOLUME, Durations.minutes(10),
				Durations.minutes(10));
		JavaPairDStream<String, Double> windowFBVolumeDStream = stockVolumeFBStream.reduceByKeyAndWindow(
				StreamTransformer.SUM_REDUCER_VOLUME,
				StreamTransformer.DIFF_REDUCER_VOLUME, Durations.minutes(10),
				Durations.minutes(10));
		
		windowMSFTVolumeDStream =  windowMSFTVolumeDStream.union(windowGoogleVolumeDStream).union(windowADBEVolumeDStream).union(windowFBVolumeDStream);
		return windowMSFTVolumeDStream;
	}
	
	public static JavaPairDStream<String, Double> getVolumeDStream(JavaDStream<Map<String, StockPrice>> stockStream,String symbol){
		
		JavaPairDStream<String, Double> stockVolumeStream = stockStream.mapToPair(new PairFunction<Map<String,StockPrice>, String, Double>() {
			
			private static final long serialVersionUID = 1L;
			
			public Tuple2<String, Double> call(Map<String,StockPrice> map) throws Exception{
				if(map.containsKey(symbol)) {
					Tuple2<String, Double> stockVolume = new Tuple2<String, Double>(symbol, map.get(symbol).getPriceData().getVolume());
					return stockVolume;
				}else {
					Tuple2<String, Double> stockVolume = new Tuple2<String, Double>(symbol, 0.0);
					return stockVolume;
				}
			}
		}
		);
		return stockVolumeStream;
	}
	
	
	/*public static JavaPairDStream<String,Tuple2<PriceData, Long>> getRSIDataWindowDStream(JavaDStream<Map<String, StockPrice>> stockStream){
		
		JavaPairDStream<String, Double> stockRSIDataMSFTStream = StreamTransformer.getVolumeDStream(stockStream, "MSFT");
		JavaPairDStream<String, Double> stockRSIDataGoogleStream= StreamTransformer.getVolumeDStream(stockStream, "GOOGL");
		JavaPairDStream<String, Double> stockRSIDataADBEStream = StreamTransformer.getVolumeDStream(stockStream, "ADBE");
		JavaPairDStream<String, Double> stockRSIDataFBStream = StreamTransformer.getVolumeDStream(stockStream, "FB");
		
	}*/
	
	public static JavaPairDStream<String, RSIData> getRSIDataDStream(JavaDStream<Map<String, StockPrice>> stockStream,String symbol){
		JavaPairDStream<String, RSIData> stockRSIDataDStream = stockStream.mapToPair(new PairFunction<Map<String, StockPrice>,String, RSIData>() {
			/**
			 * Adding serialization
			 */
			private static final long serialVersionUID = 1L;

			public Tuple2<String, RSIData> call(Map<String, StockPrice> map) throws Exception {
				if (map.containsKey(symbol)) {
					RSIData anRsiData = new RSIData();
					Double stockOpenPrice = map.get(symbol).getPriceData().getOpen();
					Double stockClosePrice = map.get(symbol).getPriceData().getClose();
					if(stockClosePrice >= stockOpenPrice) {
						anRsiData.setGain(stockClosePrice-stockOpenPrice);
					}else {
						anRsiData.setLoss(stockOpenPrice-stockClosePrice);
					}
					Tuple2<String, RSIData> strRSIDataTuple = new Tuple2<String,RSIData>(symbol, anRsiData);
					return strRSIDataTuple;
					//return new Tuple2<String,PriceData>(symbol, map.get(symbol).getPriceData());
				} else {
					RSIData anRsiData = new RSIData();
					Tuple2<String, RSIData> strRSIDataTuple = new Tuple2<String,RSIData>(symbol, anRsiData);
					return strRSIDataTuple;
					//return new Tuple2<String,PriceData>(symbol, new PriceData());
				}
			}
		});

		return stockRSIDataDStream;
	}
	
	
	public static Function2<Long, Long, Long>
	SUM_REDUCER_COUNT = (a, b) -> {
	return a+b;
	};
	
	public static Function2<Long, Long, Long>
	DIFF_REDUCER_COUNT = (a, b) -> {
	return a-b;
	};
	
	public static Function2<Double, Double, Double>
	SUM_REDUCER_VOLUME = (a, b) -> {
	return a+b;
	};
	
	public static Function2<Double, Double, Double>
	DIFF_REDUCER_VOLUME = (a, b) -> {
	return a-b;
	};
	
	public static Function2<RSIData, RSIData, RSIData>
	SUM_REDUCER_RSI_DATA = (a, b) -> {
	RSIData rsiData = new RSIData();
	rsiData.setGain(a.getGain() + b.getGain());
	rsiData.setLoss(a.getLoss() + b.getLoss());
	rsiData.setAggregatedStockCounter(a.getAggregatedStockCounter() + b.getAggregatedStockCounter());
	
	if(rsiData.getAggregatedStockCounter() <= 9) {
		rsiData.setAverageGain(rsiData.getGain()/rsiData.getAggregatedStockCounter());
		rsiData.setAverageLoss(rsiData.getLoss()/rsiData.getAggregatedStockCounter());
	}else {
		System.out.println("Aggregated Stock Counter for presnent value is: "+ a.getAggregatedStockCounter());
		System.out.println("Aggregated Stock Counter for incoming value is: "+ a.getAggregatedStockCounter());
		Double incomingGain = b.getGain();
		Double incomingLoss = b.getLoss();
		Double previousAvgGain = a.getAverageGain();
		Double previousAvgLoss = a.getAverageLoss();
		Double newAvgGain = ((previousAvgGain*(rsiData.getAggregatedStockCounter()-1))+incomingGain)/(rsiData.getAggregatedStockCounter()-1);
		Double newAvgLoss = ((previousAvgLoss*9)+incomingLoss)/10;
		
		rsiData.setAverageGain(newAvgGain);
		rsiData.setAverageLoss(newAvgLoss);
	}
	
	return rsiData;
	};
	
	public static Function2<RSIData, RSIData, RSIData>
	DIFF_REDUCER_RSI_DATA = (a, b) -> {
		RSIData rsiData = new RSIData();
		rsiData.setGain(a.getGain() + b.getGain());
		rsiData.setLoss(a.getLoss() + b.getLoss());
		rsiData.setAggregatedStockCounter(a.getAggregatedStockCounter() + b.getAggregatedStockCounter());
		
		return rsiData;
	
	};
	
	
	
	
}
