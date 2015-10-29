package com.notes4geeks.learn.spark.basic;

import java.util.Arrays;
import java.util.function.Consumer;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class SparkPrintRDD {

	public static void main(String[] args) {
String master = args.length > 0 ? args[0] : "local[4]";
		
		JavaSparkContext sc = new JavaSparkContext(master, SparkSumByFold.class.getSimpleName());
		
		JavaRDD<Integer> oddNos = sc.parallelize(Arrays.asList(1, 3, 5, 3, 7, 7, 9, 1));
	    
		/*
		 * when we do rdd.foreach(), the foreach is processed at the worker nodes, 
		 * and the results are not transferred to the driver,
		 * so, the below will print the results in the console of the worker nodes. 
		 */
	    oddNos.foreach(new VoidFunction<Integer>() {
			
			@Override
			public void call(Integer t) throws Exception {
				System.out.println(t);
			}
		});
	    
	    /*
		 * rdd.collect() causes the results to be transferred to the driver.
		 * so when we do rdd.collect().foreach() and print the results,
		 * it is done in the console of the driver program. 
		 */
	    oddNos.collect().forEach(new Consumer<Integer>() {
			@Override
			public void accept(Integer t) {
				System.out.println(t);
			}
		});
	    
	    sc.close();

	}

}
