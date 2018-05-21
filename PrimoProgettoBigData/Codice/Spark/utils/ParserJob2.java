package utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import com.fasterxml.jackson.databind.ObjectMapper;
import scala.Tuple2;

public class ParserJob2 {
	private static String tokens = "[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";
	public static Tuple2<Integer,Tuple2<String, Integer>> parseReview(String line) {
		String[] lines = line.split(",");
		int year;
		String prodID= lines[1];
		String scoreString= lines[6];
	
		
	    
	    try {
			long timestamp = Long.parseLong(lines[7]);
			Date date = new Date(timestamp*1000L);
			SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss z");
			String formattedDate = sdf.format(date);
			year = Integer.parseInt(formattedDate.substring(6, 10));
			ObjectMapper objectMapper = new ObjectMapper();
		}catch (Exception e) {
			year = 9999;
		}
	    Integer score;
		try {
			score = Integer.parseInt(scoreString);
		}catch (Exception e) {
			score = 0;
		}
		Tuple2<String, Integer> prod = new Tuple2<>(prodID,year);
		Tuple2<Integer,Tuple2<String, Integer>> pair = new Tuple2<Integer,Tuple2<String, Integer>>(score, prod);

		return pair;
	}

}