package com.example.hive.udf;
import java.util.Date;
import java.text.SimpleDateFormat;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class Unix2Date extends UDF{
	public Text evaluate(Text text) {
		Text returned;
		try {
			if(text == null) return null;
			 long timestamp = Long.parseLong(text.toString());
			 Date date = new Date(timestamp*1000L);
			 SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss z");
			 String formattedDate = sdf.format(date);
			 String year = formattedDate.substring(6, 10);
			 returned = new Text(year);
		} catch (Exception e) {
			returned = new Text("9999");
		}
		return returned; 
	}
}
