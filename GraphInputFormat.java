import org.apache.hadoop.*;

public static class GraphInputFormat extends TextInputFormat {
	@Override
	public RecordReader<IntWritable, IntWritable> createRecordReader(InputSplit split, TaskAttemptContext ctx) {
		return new GraphRecordReader(split, ctx);
	}
}