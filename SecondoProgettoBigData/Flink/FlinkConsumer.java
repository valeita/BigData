package flinkConsumer;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class FlinkConsumer {
	private static final Set<String> VALUES = new HashSet<String>(Arrays.asList(
		     new String[] {"asshole","bastard","dickhead","dumbass","dullard",
				      "fuck","block-head","jackass","muthafucka","shitface","slut",
				      "moron","bitch","shit"}
		));
	@SuppressWarnings({ "deprecation", "rawtypes" })
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		// only required for Kafka 0.8
		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "test");
		@SuppressWarnings("unchecked")
		DataStream<String> stream = env
			.addSource(new FlinkKafkaConsumer08("tweet-output-for-streaming", new SimpleStringSchema(), properties));
		
		@SuppressWarnings({ "serial", "unchecked" })
		DataStream<Map<String,Object>> result = stream.flatMap(new FlatMapFunction<String, Map<String,Object>>() {
			public void flatMap(String line, Collector<Map<String, Object>> out) throws Exception {
				Map<String, Object> jsonMap = new Gson().fromJson(
					    line, new TypeToken<HashMap<String, Object>>() {}.getType()
				);
				out.collect(jsonMap);
				
			}
		})
		.map(tweet->check(tweet))
		.returns(new TupleTypeInfo(TypeInformation.of(Map.class), TypeInformation.of(Boolean.class)))
		.filter(new FilterFunction<Tuple2<Map<String,Object>,Boolean>>() {
			@Override
			public boolean filter(Tuple2<Map<String, Object>, Boolean> checkedTweet) throws Exception {
				return checkedTweet.f1;
			}
		})
		.map(new MapFunction<Tuple2<Map<String, Object>, Boolean>,Map<String, Object>>() {
			@Override
			public Map<String, Object> map(Tuple2<Map<String, Object>,Boolean> checkedTweet) throws Exception {
				return checkedTweet.f0;
			}
		});
		result.print();
		// write the result on MongoDB
		result.map(new MapFunction<Map<String,Object>, Boolean>() {

			@Override
			public Boolean map(Map<String, Object> tweet) throws Exception {
				write(tweet);
				return true;
			}
		});
		env.execute();
	}
	
	public static void write( Map<String, Object> tweet) {
		Document doc = mapToDocument(tweet);
		MongoClient client = new MongoClient();  
        MongoDatabase db = client.getDatabase("clusterdb");
        MongoCollection<Document> tweetsCollection = db.getCollection("flinkStreamingResult");
        tweetsCollection.insertOne(doc);
        client.close();
	}
	
	private static Tuple2<Map<String,Object>,Boolean> check(Map<String, Object> tweet) {
		Boolean check = false;
		String[] text;
		for (Entry<String, Object> entryElem : tweet.entrySet()) {
			if(entryElem.getKey().equals("text")) {
				text=entryElem.getValue().toString().split(" ");
				for(String t : text) {if(VALUES.contains(t)) check=true;}
			}
		}
		return new Tuple2<>(tweet,check);
	}
	
	private static Document mapToDocument( Map<String,Object> tweet) {
		Document doc;
		StringBuilder build = new StringBuilder();
		build.append("{");
		for (Entry<String, Object> entryElem : tweet.entrySet()) {
			build.append(entryElem.getKey().toString());
			build.append(":'");
			build.append(entryElem.getValue().toString());
			build.append("',");
		}
		build.deleteCharAt(build.length()-1);  // remove last ','
		build.append("}");
		System.out.println(build.toString());
		doc=Document.parse(build.toString());
		return doc;
	}

}
