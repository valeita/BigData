package DRData.progetto2bigdata;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
 
public class KafkaToStreaming {
 
    public static void main(String[] args) throws Exception {
    	
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe-3");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
 
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("tweet-intermediate");
        source.flatMapValues(value -> Arrays.asList(filterJSON(value))).to("tweet-output-for-streaming");;
        
        
        final Topology topology = builder.build();
 
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);
 
        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook-3") {
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
    
	
	public static String filterJSON(String jsonLine) {
		
		Map<String, Object> retMap = new Gson().fromJson(
    		    jsonLine, new TypeToken<HashMap<String, Object>>() {}.getType()
		);
		Map<String, Object> filteredMap = new HashMap<String,Object>();
		
		for (Entry<String, Object> entryElem : retMap.entrySet()){
			if(entryElem.getKey().equals("_id")) filteredMap.put(entryElem.getKey(), entryElem.getValue());
			if(entryElem.getKey().equals("id")) filteredMap.put(entryElem.getKey(), entryElem.getValue());
			if(entryElem.getKey().equals("user")) {
				Map<String, Object> mapTmp = (Map<String, Object>) entryElem.getValue();
				for (Entry<String, Object> entryElem2 : mapTmp.entrySet()) 
					if(entryElem2.getKey().equals("name")) filteredMap.put(entryElem2.getKey(), entryElem2.getValue());
			}
			if(entryElem.getKey().equals("text")) filteredMap.put(entryElem.getKey(), entryElem.getValue());
			if(entryElem.getKey().equals("created_at")) filteredMap.put(entryElem.getKey(), entryElem.getValue());	
		}
		String jsonFromMapFiltered = new Gson().toJson(filteredMap);
		
    		return jsonFromMapFiltered;
    }
}