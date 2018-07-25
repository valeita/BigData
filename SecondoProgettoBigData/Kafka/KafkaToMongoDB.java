package DRData.progetto2bigdata;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
 
public class KafkaToMongoDB {
 
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe-2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
 
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("tweet-intermediate");
        source.flatMapValues(value -> Arrays.asList(forwardToMongoDB(value)));
        
        final Topology topology = builder.build();
 
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);
 
        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook-2") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });
 
        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
    
	public static String forwardToMongoDB(String jsonLine) {
		
		Document doc = Document.parse(jsonLine);	
		MongoClient client = new MongoClient();  
        MongoDatabase db = client.getDatabase("clusterdb");
        MongoCollection<Document> tweetsCollection = db.getCollection("tweets");
        tweetsCollection.insertOne(doc);
        client.close();
         
		return jsonLine;
	}
}