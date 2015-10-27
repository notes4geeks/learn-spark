package com.notes4geeks.learn.spark;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

public class SparkMapFunction {

	public static void main(String[] args) {
		String master = args.length > 0 ? args[0] : "local[4]";
		
		JavaSparkContext sc = new JavaSparkContext(master, SparkSumByFold.class.getSimpleName());
		
		JavaRDD<String> namesRDD = sc.parallelize(Arrays.asList("Simon Mignolet", "Clyne", "Sakho", "Moreno", "Skrtel", "Emre Can", "James Milner", "Coutinho", "Lallana", "Lucas", "Origi", "Jurgen Klopp"));
		 
		// returns the first name of every player.
		JavaRDD<String> allNamesRDD = namesRDD.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterable<String> call(String t) throws Exception {
				return Arrays.asList(t.split(" "));
			}
		});
		
		
		// print all names.
		allNamesRDD.foreach(new VoidFunction<String>() {
			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});
		
		sc.close();

	}

}
