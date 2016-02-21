package com.braintaker.transformation;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.braintaker.util.KeyPair;

public class JoinOperation {
	public static class MapperDepartment extends
			Mapper<LongWritable, Text, KeyPair, Text> {

		@Override
		protected void setup(Context context) {
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String linevalue[] = line.split(",");

			context.write(new KeyPair(linevalue[0], "0"), new Text(
					linevalue[1]));
		}

		@Override
		public void cleanup(Context context) {
		}

	}

	public static class MapperSalary extends
			Mapper<LongWritable, Text, KeyPair, Text> {

		@Override
		protected void setup(Context context) {
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String linevalue[] = line.split(",");
			context.write(new KeyPair(linevalue[0], "1"), new Text(line));
		}

		@Override
		public void cleanup(Context context) {
		}

	}

	public static class ReducerOperation extends
			Reducer<KeyPair, Text, Text, Text> {
		@Override
		protected void setup(Context context) {
		}

		@Override
		protected void reduce(KeyPair key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			Iterator<Text> iterator = values.iterator();
			Text stationName = new Text(iterator.next());
			while (iterator.hasNext()) {
				Text record = iterator.next();
				Text outValue = new Text(stationName.toString() + "\t"
						+ record.toString());
				context.write(key.getFirst(), outValue);
			}
		}

		@Override
		public void cleanup(Context context) {
		}
	}
}
