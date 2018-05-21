package com.example.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class FormatSummary extends UDF{

	private String tokens = "[%_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";
	
	public Text evaluate(Text text) {
		String formatted = text.toString().toLowerCase().replaceAll(tokens, "").replaceAll("  ", " ");;
		return new Text(formatted); 
	}

}
