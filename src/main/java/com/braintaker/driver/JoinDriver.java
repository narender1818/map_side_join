package com.braintaker.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static com.braintaker.transformation.JoinOperation.MapperDepartment;
import static com.braintaker.transformation.JoinOperation.MapperSalary;
import static com.braintaker.transformation.JoinOperation.ReducerOperation;

import com.braintaker.util.KeyPair;

public class JoinDriver extends Configured implements Tool {
	public static class KeyPartitioner extends Partitioner<KeyPair, Text> {
		@Override
		public int getPartition(KeyPair key, Text value, int numPartitions) {
			return (key.getFirst().hashCode() & Integer.MAX_VALUE)
					% numPartitions;
		}
	}

	public static class GroupComparator extends WritableComparator {
		protected GroupComparator() {
			super(KeyPair.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			KeyPair ip1 = (KeyPair) w1;
			KeyPair ip2 = (KeyPair) w2;
			return KeyPair.compare(ip1.getFirst(), ip2.getFirst());
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration configuration= new Configuration();
		Job job = new Job(configuration, "Join weather records with station names");
		job.setJarByClass(getClass());
		Path ncdcInputPath = new Path(args[0]);
		Path stationInputPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		MultipleInputs.addInputPath(job, ncdcInputPath, TextInputFormat.class,
				MapperDepartment.class);
		MultipleInputs.addInputPath(job, stationInputPath,
				TextInputFormat.class, MapperSalary.class);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setPartitionerClass(KeyPartitioner.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setMapOutputKeyClass(KeyPair.class);
		job.setReducerClass(ReducerOperation.class);
		job.setOutputKeyClass(Text.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run( new JoinDriver(), args);
		System.exit(exitCode);
	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setConf(Configuration arg0) {
		// TODO Auto-generated method stub

	}
}
