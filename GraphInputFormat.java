package sogara.hadoop.subgraph;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;

public class GraphInputFormat extends FileInputFormat<IntWritable, IntWritable> {
	public RecordReader<IntWritable, IntWritable> createRecordReader(InputSplit split, TaskAttemptContext ctx) {
		return new GraphRecordReader();
	}
}