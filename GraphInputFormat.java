package sogara.hadoop.subgraph;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;

public class GraphInputFormat extends FileInputFormat<Text, Text> {
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext ctx) {
		return new GraphRecordReader();
	}
}