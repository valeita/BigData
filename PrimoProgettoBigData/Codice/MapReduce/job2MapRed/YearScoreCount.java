package PrimoProgetto.job2MapRed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class YearScoreCount implements WritableComparable<YearScoreCount>{

	private IntWritable year;
	private IntWritable score;
	private IntWritable count;
	
	public YearScoreCount() {}
	
	public YearScoreCount(IntWritable year, IntWritable score, IntWritable count) {
	    this.year = year;
	    this.score = score;
	    this.count = count;
	}
	
	public void readFields(DataInput in) throws IOException {
	    year = new IntWritable(Integer.parseInt(in.readUTF()));
	    score = new IntWritable(Integer.parseInt(in.readUTF()));
	    count = new IntWritable(Integer.parseInt(in.readUTF()));
	}
	
	public void write(DataOutput out) throws IOException {
	    out.writeUTF(year.toString());
	    out.writeUTF(score.toString());
	    out.writeUTF(count.toString());
	}
	
	public void set(IntWritable year, IntWritable score, IntWritable count) {
	    this.year = year;
	    this.score = score;
	    this.count = count;
	}
	
	@Override
	public String toString() {
		return year.toString() + " " + score.toString() + " " + count.toString();
	}
	
	@Override
	public int hashCode() {
	    return year.hashCode() + score.hashCode() + count.hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
	if (o instanceof YearScoreCount) {
		YearScoreCount couple = (YearScoreCount) o;
	    return year.equals(couple.year) && score.equals(couple.score) && count.equals(couple.count);
	}
	    return false;
	}
	
	public int compareTo(YearScoreCount o) {
		
		return this.year.compareTo(o.year);
		
	}

	public IntWritable getYear() {
		return year;
	}

	public void setYear(IntWritable year) {
		this.year = year;
	}

	public IntWritable getScore() {
		return score;
	}

	public void setScore(IntWritable score) {
		this.score = score;
	}

	public IntWritable getCount() {
		return count;
	}

	public void setCount(IntWritable count) {
		this.count = count;
	}

	public void incScore(IntWritable score) {
		this.score.set(this.score.get()+score.get());
	}

	public void incCount(IntWritable count) {
		this.count.set(this.count.get()+count.get());
	}
}
