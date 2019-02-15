import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.gson.Gson;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.*;

public class FirstSparkApplication {

	

    public static void main(String[] args) throws InterruptedException {
        final long window = 30;
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("FirstSparkApplication");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		Logger.getRootLogger().setLevel(Level.ERROR);
		JavaReceiverInputDStream<String> dstrInput = jssc.socketTextStream("localhost", 9999);
		JavaDStream<String> windowedInput = dstrInput.window(Durations.seconds(window), Durations.seconds(10));
		
		Function<String, StockData[]> f = new Function<String, StockData[]>() {
            public StockData[] call(String s) throws Exception {
		        Gson gson = new Gson();
		        StockData[] data = gson.fromJson(s, StockData[].class);
                return data;
            }};
        // Read input data & convert into DStream
        JavaDStream<StockData[]> DtoInut = windowedInput.map(f);

        // Working logic to calculate SMA without reduce
        /*VoidFunction<JavaRDD<StockData[]>> SMAFunction = new VoidFunction<JavaRDD<StockData[]>>() {
            public void call(JavaRDD<StockData[]> t) throws Exception {
                List<StockData[]> list = t.collect();
                if(list != null && list.size() > 0) {
                    Map<String, Float> map = new HashMap<String, Float>();
                    for(StockData[] s_arr : list) {
                        for(StockData s : s_arr) {
                            Float closedPrice = Float.parseFloat(s.getPriceData().getClose());
                            if(map.get(s.getSymbol()) == null)
                                map.put(s.getSymbol(), closedPrice);
                            else
                                map.put(s.getSymbol(), (map.get(s.getSymbol()) + closedPrice));
                        }
                    }
                    for(Map.Entry<String, Float> m : map.entrySet()) {
                        System.out.println("SMA for " + window/60F + " minute window for stock symbol " + m.getKey() + " is : " + m.getValue() / list.size());
                    }
                }
            }
        };
        DtoInut.foreachRDD(SMAFunction);*/
        
        // 1. Calculate Closed SMA
        PairFlatMapFunction<StockData[], String, AverageTuple> closed_avg = new PairFlatMapFunction<StockData[], String, AverageTuple>() {
            public Iterator<Tuple2<String, AverageTuple>> call(StockData[] list) throws Exception {
                if(list.length > 0) {
                    Gson gson = new Gson();
                    //Iterator<Tuple2<String, Float>> itr = new Iterator<Tuple2<String, Float>>();
                    List<Tuple2<String, AverageTuple>> retList = new ArrayList<Tuple2<String,AverageTuple>>();
                    for(StockData s : list) {
                        retList.add(new Tuple2<String, AverageTuple>(s.getSymbol(), new AverageTuple(1, Float.parseFloat(s.getPriceData().getClose()))));
                    }
                    return retList.iterator();
                }
                return new ArrayList<Tuple2<String, AverageTuple>>().iterator();
            }
        };
        
        JavaPairDStream<String, AverageTuple> sma_closed_RDD = DtoInut.flatMapToPair(closed_avg);
        
        Function2<AverageTuple, AverageTuple, AverageTuple> smaRDD_ReducebyKey = new Function2<AverageTuple, AverageTuple, AverageTuple>(){

            public AverageTuple call(AverageTuple v1, AverageTuple v2) throws Exception {
                return new AverageTuple(v1.getCount() + v2.getCount(), v1.getAverage() + v2.getAverage());
            }
            
        };
        JavaPairDStream<String, AverageTuple> sma_closed_RDD_reduced = sma_closed_RDD.reduceByKey(smaRDD_ReducebyKey);
        
        // Print SMA based on closed value
        sma_closed_RDD_reduced.print();
        
        // 2. Find the stock out of the four stocks giving maximum profit (average closing price - average opening price) in a 5-minute sliding window for the last 10 minutes
        PairFlatMapFunction<StockData[], String, MaximumProfitTuple> maximum_avg = new PairFlatMapFunction<StockData[], String, MaximumProfitTuple>() {
            public Iterator<Tuple2<String, MaximumProfitTuple>> call(StockData[] list) throws Exception {
                if(list.length > 0) {
                    Gson gson = new Gson();
                    //Iterator<Tuple2<String, Float>> itr = new Iterator<Tuple2<String, Float>>();
                    List<Tuple2<String, MaximumProfitTuple>> retList = new ArrayList<Tuple2<String,MaximumProfitTuple>>();
                    for(StockData s : list) {
                        retList.add(new Tuple2<String, MaximumProfitTuple>(s.getSymbol(), new MaximumProfitTuple(1, Float.parseFloat(s.getPriceData().getClose()), Float.parseFloat(s.getPriceData().getOpen()))));
                    }
                    return retList.iterator();
                }
                return new ArrayList<Tuple2<String, MaximumProfitTuple>>().iterator();
            }
        };
        
        JavaPairDStream<String, MaximumProfitTuple> max_sma_RDD = DtoInut.flatMapToPair(maximum_avg);
        
        Function2<MaximumProfitTuple, MaximumProfitTuple, MaximumProfitTuple> max_smaRDD_ReducebyKey = new Function2<MaximumProfitTuple, MaximumProfitTuple, MaximumProfitTuple>(){

            public MaximumProfitTuple call(MaximumProfitTuple v1, MaximumProfitTuple v2) throws Exception {
                return new MaximumProfitTuple(v1.getCount() + v2.getCount(), v1.getClose_average() + v2.getClose_average(), v1.getOpen_average() + v2.getOpen_average());
            }
            
        };
        JavaPairDStream<String, MaximumProfitTuple> max_sma_RDD_reduced = max_sma_RDD.reduceByKey(max_smaRDD_ReducebyKey);
        max_sma_RDD_reduced.print();

        // Print highest profitable stock
        VoidFunction<JavaPairRDD<String, MaximumProfitTuple>> printMaxAvg = new VoidFunction<JavaPairRDD<String, MaximumProfitTuple>>() {

            public void call(JavaPairRDD<String, MaximumProfitTuple> t) throws Exception {
                List<Tuple2<String,MaximumProfitTuple>> list = t.collect();
                if(list != null && list.size() > 0) {
                    Map<Float, String> high_profit = new TreeMap<Float, String>(Collections.reverseOrder());
                    for(Tuple2<String,MaximumProfitTuple> t2 : list) {
                        high_profit.put(Float.parseFloat(t2._2.toString()), t2._1);
                    }
                    Map.Entry<Float, String> entry = high_profit.entrySet().iterator().next();
                    System.out.println("Highest profit earning stock is : " + entry.getValue());
                }
            }
        };
        max_sma_RDD_reduced.foreachRDD(printMaxAvg);
        
        
        
        
        //smaRDD.count().print();
        //smaRDD.countByValue().print();
        
        //smaRDD.count
        Function2<Float, Float, Float> funReduceByKeySMA = new Function2<Float, Float, Float>(){

            public Float call(Float v1, Float v2) throws Exception {
                if(v1 != null && v2 != null)
                    return (v1 + v2);
                return 0F;
            }
        };
        //JavaPairDStream<String, Float> finalSMA = smaRDD.reduceByKey(funReduceByKeySMA);
        //finalSMA.print();
        

        // Not working properly code to calculate maximum profit
        /*VoidFunction<JavaRDD<StockData[]>> MaxProfitFunction = new VoidFunction<JavaRDD<StockData[]>>() {
            public void call(JavaRDD<StockData[]> t) throws Exception {
                List<StockData[]> list = t.collect();
                if(list != null && list.size() > 0) {
                    Map<String, Float> avg_opening = new HashMap<String, Float>();
                    Map<String, Float> avg_closing = new HashMap<String, Float>();
                    for(StockData[] s_arr : list) {
                        for(StockData s : s_arr) {
                            Float openedPrice = Float.parseFloat(s.getPriceData().getOpen());
                            Float closedPrice = Float.parseFloat(s.getPriceData().getClose());
                            // Calculate avg open price
                            if(avg_opening.get(s.getSymbol()) == null)
                                avg_opening.put(s.getSymbol(), openedPrice);
                            else
                                avg_opening.put(s.getSymbol(), (avg_opening.get(s.getSymbol()) + openedPrice) / list.size());

                            // Calculate avg close price
                            if(avg_closing.get(s.getSymbol()) == null)
                                avg_closing.put(s.getSymbol(), closedPrice);
                            else
                                avg_closing.put(s.getSymbol(), (avg_closing.get(s.getSymbol()) + closedPrice) / list.size());
                        }
                    }
                    Map<Float, String> high_profit = new TreeMap<Float, String>(Collections.reverseOrder());
                    for(Map.Entry<String, Float> m : avg_closing.entrySet()) {
                        String symbol = m.getKey();
                        System.out.println("Average profit for " + window/60F + " minute window is, for " + symbol + " is : " + (avg_closing.get(symbol) - avg_opening.get(symbol)));
                        high_profit.put((avg_closing.get(symbol) - avg_opening.get(symbol)), symbol);
                    }
                    Map.Entry<Float, String> entry = high_profit.entrySet().iterator().next();
                    System.out.println("Highest profit earning stock is : " + entry.getValue());
                }
            }
        };
        DtoInut.foreachRDD(MaxProfitFunction);*/
        
        
        // 4. Calculate highest volume stock
        PairFlatMapFunction<StockData[], String, Float> volumeFunction = new PairFlatMapFunction<StockData[], String, Float>() {
            public Iterator<Tuple2<String, Float>> call(StockData[] list) throws Exception {
                if(list.length > 0) {
                    Gson gson = new Gson();
                    //Iterator<Tuple2<String, Float>> itr = new Iterator<Tuple2<String, Float>>();
                    List<Tuple2<String, Float>> retList = new ArrayList<Tuple2<String,Float>>();
                    for(StockData s : list) {
                        retList.add(new Tuple2<String, Float>(s.getSymbol(), Float.parseFloat(s.getPriceData().getVolume())));
                    }
                    return retList.iterator();
                }
                return new ArrayList<Tuple2<String,Float>>().iterator();
            }
        };
        
        JavaPairDStream<String, Float> volumeRDD = DtoInut.flatMapToPair(volumeFunction);
        
        
        
        
        Function2<Float, Float, Float> funReduceByKey = new Function2<Float, Float, Float>(){

            public Float call(Float v1, Float v2) throws Exception {
                if(v1 != null && v2 != null)
                    return v1 + v2;
                return 0F;
            }
        };
        JavaPairDStream<String, Float> finalVolumeCnt = volumeRDD.reduceByKey(funReduceByKey);
        
        
        JavaPairDStream<Float, String> finalVolumeCnt2 = finalVolumeCnt.mapToPair(new PairFunction<Tuple2<String,Float>,Float, String>() {
            public Tuple2<Float, String> call(Tuple2<String, Float> t) throws Exception {
                return t.swap();
            }
        });
        
        JavaPairDStream<Float, String> sorted = finalVolumeCnt2.transformToPair(new Function<JavaPairRDD<Float,String>, JavaPairRDD<Float,String>>() {
            public JavaPairRDD<Float, String> call(JavaPairRDD<Float, String> v1) throws Exception {
                return v1.sortByKey(false);
            }
        });
		
        //DtoInut.print();
        //System.out.println("(symbol, volumeCount) : ");
        //finalVolumeCnt.print();
        
        
        
        JavaPairDStream<String, Float> sorted2 = sorted.mapToPair(new PairFunction<Tuple2<Float, String>, String, Float>() {
            public Tuple2<String, Float> call(Tuple2<Float, String> t) throws Exception {
                return t.swap();
            }
        });
        sorted2.print();
        
        VoidFunction<JavaPairRDD<String, Float>> foreachFunc = new VoidFunction<JavaPairRDD<String, Float>>() {

            public void call(JavaPairRDD<String, Float> t) throws Exception {
                // TODO Auto-generated method stub
                List<Tuple2<String, Float>> tuple = t.collect();
                if(tuple.size() > 0)
                    System.out.println("Stock with highest volume : " + tuple.get(0));
            }
            
        };
        
        sorted2.foreachRDD(foreachFunc);
		
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}

}
