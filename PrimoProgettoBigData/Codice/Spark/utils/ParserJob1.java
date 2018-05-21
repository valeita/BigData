package utils;


import java.text.SimpleDateFormat;
import java.util.Date;

import scala.Tuple2;

public class ParserJob1 {
	
	private static String tokens = "[%_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";
	
	public static Tuple2<Integer,String> parseLineToPairYearSummary(String line) {
		
		Tuple2<Integer,String> pair = null;
	    
	    String[] parts = line.split(",");
	    Integer year;
	    String summary;
	    
	    summary = parts[8].toLowerCase().replaceAll(tokens, "").replaceAll("  ", " ");

	    try {
	    	Integer timestamp = Integer.parseInt(parts[7]);
	    	Date date = new Date(timestamp*1000L);
			SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss z");
			String formattedDate = sdf.format(date);
			
			year = Integer.parseInt(formattedDate.substring(6, 10));
			
	    } catch(Exception e) {
	    	year = (Integer) 9999;
	    }
		
		pair = new Tuple2<>(year,summary);
	    return pair;
	  }

}
