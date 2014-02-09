package sogara.hadoop.subgraph;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;

public class DensityInputFormat extends FileInputFormat<FloatWritable, Text> {
	public RecordReader<FloatWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext ctx) {
		return new DensityRecordReader();
	}
}