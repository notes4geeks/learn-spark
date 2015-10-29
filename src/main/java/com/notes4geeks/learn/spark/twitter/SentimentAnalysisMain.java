package com.notes4geeks.learn.spark.twitter;

import java.util.List;
import java.util.Set;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple5;
import twitter4j.Status;

public class SentimentAnalysisMain {

	private static final Logger LOGGER = Logger.getLogger(SentimentAnalysisMain.class);

	public static void main(String[] args) {
		BasicConfigurator.configure();
		
		SparkConf conf = new SparkConf().setAppName("Twitter Sentiment Analysis");
		
		if(args.length > 0){
			conf.setMaster(args[0]);
		} else {
			conf.setMaster("local[4]");
		}
		
		JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(2000));
		
		String[] filters = new String[] {"modi"};
		
		// Set the system properties so that Twitter4j library used by twitter stream
	    // can use them to generate OAuth credentials
		//the below credentials should be replaced with your own credentials.
	    System.setProperty("twitter4j.oauth.consumerKey", "lsHwPvBGzayZX5vIrFppA");
	    System.setProperty("twitter4j.oauth.consumerSecret", "ifRPx9TrSylrsEmMQEibwyswDRZBwJim306enVJMw");
	    System.setProperty("twitter4j.oauth.accessToken", "56664287-VMUwWdlYTK2IfT2trQUMtAB5GtrS40Age8OUGkQ");
	    System.setProperty("twitter4j.oauth.accessTokenSecret", "aoEj5CUV1EweJV03msbSa9KS9ruDc9LE0wRUuC68");
	    
		JavaReceiverInputDStream<Status> messages = TwitterUtils.createStream(ssc, filters);
		
		JavaPairDStream<Long, String> tweets = messages.mapToPair(new PairFunction<Status, Long, String>() {

			@Override
			public Tuple2<Long, String> call(Status status) throws Exception {
				LOGGER.error(status.getText());
				return new Tuple2<Long, String>(status.getId(), status.getText());
			}
		});
		
		JavaPairDStream<Long, String> filtered = tweets.filter(new Function<Tuple2<Long,String>, Boolean>() {

			@Override
			public Boolean call(Tuple2<Long, String> v1) throws Exception {
				return v1 != null;
			}
		});
		
		JavaDStream<Tuple2<Long, String>> filteredTweets = filtered.map(new Function<Tuple2<Long,String>, Tuple2<Long, String>>() {

			@Override
			public Tuple2<Long, String> call(Tuple2<Long, String> v1) throws Exception {
				String text = v1._2();
				text = text.replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase();
		        return new Tuple2<Long, String>(v1._1(), text);
			}
		});
		
		filteredTweets = filteredTweets.map(new Function<Tuple2<Long,String>, Tuple2<Long, String>>() {

			@Override
			public Tuple2<Long, String> call(Tuple2<Long, String> v1) throws Exception {
				String text = v1._2();
				List<String> stopWords = StopWords.getWords();
				
				for (String word : stopWords)
		        {
		            text = text.replaceAll("\\b" + word + "\\b", "");
		        }
		        return new Tuple2<Long, String>(v1._1(), text);
			}
		});
		
		JavaPairDStream<Tuple2<Long, String>, Float> positiveTweets = filteredTweets.mapToPair(new PairFunction<Tuple2<Long,String>, Tuple2<Long, String>, Float>() {

			@Override
			public Tuple2<Tuple2<Long, String>, Float> call(Tuple2<Long, String> t) throws Exception {
				String text = t._2();
				Set<String> posWords = PositiveWords.getWords();
				String[] words = text.split(" ");
				int numWords = words.length;
				int numPosWords = 0;
				
				for (String word : words)
		        {
		            if (posWords.contains(word))
		                numPosWords++;
		        }
				
				return new Tuple2<Tuple2<Long,String>, Float>(new Tuple2<Long, String>(t._1(), t._2()), (float)numPosWords/numWords);
			}
		});
		
		JavaPairDStream<Tuple2<Long, String>, Float> negativeTweets = filteredTweets.mapToPair(new PairFunction<Tuple2<Long,String>, Tuple2<Long, String>, Float>() {

			@Override
			public Tuple2<Tuple2<Long, String>, Float> call(Tuple2<Long, String> t) throws Exception {
				String text = t._2();
				Set<String> negWords = NegativeWords.getWords();
				String[] words = text.split(" ");
				int numWords = words.length;
				int numPosWords = 0;
				
				for (String word : words)
		        {
		            if (negWords.contains(word))
		                numPosWords++;
		        }
				
				return new Tuple2<Tuple2<Long,String>, Float>(new Tuple2<Long, String>(t._1(), t._2()), (float)numPosWords/numWords);
			}
		});
		
		JavaPairDStream<Tuple2<Long, String>, Tuple2<Float, Float>> joined = positiveTweets.join(negativeTweets);
		
		JavaDStream<Tuple4<Long, String, Float, Float>> scoredTweets = joined.map(new Function<Tuple2<Tuple2<Long,String>,Tuple2<Float,Float>>, Tuple4<Long, String, Float, Float>>() {

			@Override
			public Tuple4<Long, String, Float, Float> call(Tuple2<Tuple2<Long, String>, Tuple2<Float, Float>> v1)
					throws Exception {
				return new Tuple4<Long, String, Float, Float>(v1._1()._1(), v1._1()._2(), v1._2()._1(), v1._2()._2());
			}
		});
		
		JavaDStream<Tuple5<Long, String, Float, Float, String>> result = scoredTweets.map(new Function<Tuple4<Long,String,Float,Float>, Tuple5<Long, String, Float, Float, String>>() {

			@Override
			public Tuple5<Long, String, Float, Float, String> call(Tuple4<Long, String, Float, Float> v1)
					throws Exception {
				String sentiment = v1._3()  >= v1._4() ? "positive" : "negative";
				
				return new Tuple5<Long, String, Float, Float, String>(v1._1(), v1._2(), v1._3(), v1._4(), sentiment);
			}
		});
		
		result.foreachRDD(new Function2<JavaRDD<Tuple5<Long,String,Float,Float,String>>, Time, Void>() {

			@Override
			public Void call(JavaRDD<Tuple5<Long, String, Float, Float, String>> v1, Time v2) throws Exception {
				if(v1.count() < 0){
					return null;
				}
				
				LOGGER.error(v1);
				return null;
			}

			
		});
		
		ssc.start();
	    ssc.awaitTermination();
	}

}
