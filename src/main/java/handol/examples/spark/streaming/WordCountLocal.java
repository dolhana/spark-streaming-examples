package handol.examples.spark.streaming;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;

import scala.Tuple2;


public class WordCountLocal {
	
	public static JavaPairDStream<String, Integer>
	appendWordCountStream(JavaDStream<String> lines) {
        // Split each line into words
        @SuppressWarnings("serial")
		JavaDStream<String> words = lines
            .flatMap(new FlatMapFunction<String, String>() {
            	@Override public Iterable<String> call(String x) {
            		return Arrays.asList(x.split(" "));
            	}
            })
            .filter(new Function<String, Boolean>() {
				@Override
				public Boolean call(String arg0) throws Exception {
					return !Strings.isNullOrEmpty(arg0);
				}
            });

        // Count each word in each batch
        @SuppressWarnings("serial")
		JavaPairDStream<String, Integer> pairs =
            words.mapToPair(new PairFunction<String, String, Integer>() {
                    @Override public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                });

        @SuppressWarnings("serial")
		JavaPairDStream<String, Integer> wordCounts =
            pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

        return wordCounts;
	}
	
	public static class WordCountRow {
		String word = "";
		Integer count = 0;
		
		WordCountRow() {}
		
		public WordCountRow(String word, Integer count) {
			this.word = word;
			this.count = count;
		}
		
		public String getWord() { return word; }
		public void setWord(String value) { word = value; }
		
		public Integer getCount() { return count; }
		public void setCount(Integer value) { count = value; }
	}
	
	public static void outputToCassandra(JavaPairDStream<String, Integer> wordCountPairs) {
		@SuppressWarnings("serial")
		JavaDStream<WordCountRow> wordCountRows = wordCountPairs.map(new Function<Tuple2<String, Integer>, WordCountRow>() {
			@Override public WordCountRow call(Tuple2<String, Integer> tuple) {
				return new WordCountRow(tuple._1, tuple._2);
			}
		});
		outputToCassandra(wordCountRows);
	}
	
	public static void outputToCassandra(JavaDStream<WordCountRow> stream) {
		// This example demonstrates two ways to persist the result in Cassandra.
		// The first uses saveToCassandra() method.
		// The second uses foreachRDD and CQL in the worker.
		// Both gives you the same result.
		
		// saveToCassandra()
//		CassandraStreamingJavaUtil.javaFunctions(stream)
//		.writerBuilder("test_keyspace", "word_count", CassandraJavaUtil.mapToRow(
//				WordCountRow.class))
//		.saveToCassandra();
		
		// foreachRDD and CQL
		foreachRDDtoCassandra(stream);
	}
	
	@SuppressWarnings("serial")
	public static void foreachRDDtoCassandra(JavaDStream<WordCountRow> wordCountRows) {
		wordCountRows.foreachRDD(new Function2<JavaRDD<WordCountRow>, Time, Void>() {
			@Override public Void call(JavaRDD<WordCountRow> rdd, Time time) throws Exception {
				final SparkConf sc = rdd.context().getConf();
				final CassandraConnector cc = CassandraConnector.apply(sc);
				rdd.foreach(new VoidFunction<WordCountRow>() {
					@Override public void call(WordCountRow wordCount) throws Exception {
						// Cassandra Connector maintains a connection pool internally, so
						// you don't need to worry about the overhead that may have been caused
						// by re-establishing connections for each iteration.
						// A Session instance is simply a wrapper around a physical connection.
						try (Session session = cc.openSession()) {
							String query = String.format(Joiner.on(" ").join(
									"UPDATE test_keyspace.word_count",
									"SET count = count + %s",
									"WHERE word = '%s'"),
									wordCount.count, wordCount.word);
							session.execute(query);
						}
					}
				});
				return null;
			}
		});;
	}

	public static void main(String[] args) {
        // Create a local StreamingContext with two working thread and batch
        // of 1 second
        SparkConf conf =
            new SparkConf()
            .setMaster("local[2]")
            .setAppName("NetworkWordCount")
            .set("spark.cassandra.connection.host", "192.168.59.103")
            .set("spark.cassandra.connection.port", "9042")
            .set("spark.cassandra.connection.keep_alive_ms", "10000");
        
        CassandraConnector cassandra = CassandraConnector.apply(conf);
        
        try (Session session = cassandra.openSession()) {
        	String[] statements = {
        			"DROP KEYSPACE IF EXISTS test_keyspace",
        			"CREATE KEYSPACE test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        			"CREATE TABLE test_keyspace.word_count (word TEXT PRIMARY KEY, count COUNTER)",
        	};
        	
        	for (String statement : statements) {
        		session.execute(statement);
        	}
        }

        {
        	JavaStreamingContext jssc =
                    new JavaStreamingContext(conf, Durations.seconds(2));

            // Create a DStream that will connect to hostname:port, like
            // localhost:9999
            JavaReceiverInputDStream<String> lines =
                jssc.socketTextStream("localhost", 9999,
                                      StorageLevels.MEMORY_AND_DISK_SER);

            JavaPairDStream<String, Integer> wordCounts = appendWordCountStream(lines);

            // Print the first ten elements of each RDD generated in this DStream
            // to the console.
            outputToCassandra(wordCounts);
            wordCounts.print();

            // Start the computation.
            jssc.start();

            // Wait for the computation to terminate
            jssc.awaitTermination();
            
            jssc.close();
        }
	}
	
}
