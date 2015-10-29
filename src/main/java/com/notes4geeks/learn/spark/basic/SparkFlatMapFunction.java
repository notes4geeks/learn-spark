package com.notes4geeks.learn.spark.basic;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

public class SparkFlatMapFunction {

	public static void main(String[] args) {
		String master = args.length > 0 ? args[0] : "local[4]";
		
		JavaSparkContext sc = new JavaSparkContext(master, SparkSumByFold.class.getSimpleName());
		
		JavaRDD<String> namesRDD = sc.parallelize(Arrays.asList("Simon Mignolet", "Clyne", "Sakho", "Moreno", "Skrtel", "Emre Can", "James Milner", "Coutinho", "Lallana", "Lucas", "Origi", "Jurgen Klopp"));
		 
		// returns the first name of every player.
		JavaRDD<String> firstNamesRDD = namesRDD.map(new Function<String, String>() {
		    @Override
		    public String call(String v1) throws Exception {
		        return v1.split(" ")[0];
		    }
		});
		
		
		// print all first names.
		firstNamesRDD.foreach(new VoidFunction<String>() {
			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});
		
		sc.close();

	}

}
