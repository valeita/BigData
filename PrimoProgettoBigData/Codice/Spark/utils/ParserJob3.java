package utils;

import scala.Tuple2;

public class ParserJob3 {
	
	public static Tuple2<String, String> parseReview(String line) {
		
		String[] lines = line.split(",");
		String prodID= lines[1];
		String userID= lines[2];
		
		Tuple2<String, String> userProd = new Tuple2<String,String>(userID,prodID);

		return userProd;
	}

}
