package stormStreaming;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
* This is a basic example of a Storm topology.
*/
public class StormTweet {
	
	private static List<String> badWordsList = new ArrayList<String>(Arrays.asList("asshole","bastard","dickhead","dumbass","dullard"
			,"fuck","block-head","jackass","muthafucka","shitface","slut","moron","bitch","shit"));
	
	public static class CheckBolt extends BaseRichBolt {
	 
	   OutputCollector _collector;
	
	   @Override
	   public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
	     _collector = collector;
	   }
	
	   @Override
	   public void execute(Tuple tuple) {
		 
		   if(containBadWord(tuple.getString(0))) {
			   
			   forwardToMongoDB(tuple.getString(0));
		   }
		   _collector.emit(new Values(tuple.getString(0)));
		   _collector.ack(tuple);
	   }
	
	   @Override
	   public void declareOutputFields(OutputFieldsDeclarer declarer) {
	     declarer.declare(new Fields("tweet-box"));
	   }
	   
	   public static Boolean containBadWord(String tweetJSON) {
		   
		   Map<String, Object> retMap = new Gson().fromJson(
	    		    tweetJSON, new TypeToken<HashMap<String, Object>>() {}.getType()
			);
		   String textField = retMap.get("text").toString();
		   
		   for(String elem: badWordsList) {
			 if(tweetJSON.contains(elem)) return true;
		   }
		   return false;
	   }
	   
		public static String forwardToMongoDB(String jsonLine) {
			
			Document doc = Document.parse(jsonLine);	
			MongoClient client = new MongoClient();  
	        MongoDatabase db = client.getDatabase("clusterdb");
	        MongoCollection<Document> streamingCollection = db.getCollection("stormStreamingResult");
	        streamingCollection.insertOne(doc);
	        client.close();
	         
			return jsonLine;
		}
	}

	
	
	
	
	 public static void main(String[] args) throws Exception {

	    TopologyBuilder builder = new TopologyBuilder();
	
	    BrokerHosts hosts = new ZkHosts("localhost:2181");
		SpoutConfig spoutConfig = new SpoutConfig(hosts, "tweet-output-for-streaming", "/" + "tweet-output-for-streaming", UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		
	    builder.setSpout("tweet", kafkaSpout, 10);
	    builder.setBolt("print", new CheckBolt(), 3).shuffleGrouping("tweet");
	
	    Config conf = new Config();
	    conf.setDebug(true);
	
	    if (args != null && args.length > 0) {
	      conf.setNumWorkers(3);
	
	      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
	    }
	    else {
	    		LocalCluster cluster = new LocalCluster();
	    		cluster.submitTopology("test", conf, builder.createTopology());
	    		//Utils.sleep(10000);
	    		//cluster.killTopology("test");
	    		//cluster.shutdown();
	    }
	}
}
