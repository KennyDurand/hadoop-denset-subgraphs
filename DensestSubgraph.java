package sogara.hadoop.subgraph;

import java.io.IOException;
import java.lang.InterruptedException;
import java.lang.ClassNotFoundException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class DensestSubgraph {

  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    Job densityJob = CalculateGraphDensityJob.createJob();

    FileInputFormat.setInputPaths(densityJob, new Path(args[0]));
    FileOutputFormat.setOutputPath(densityJob, new Path(args[1] + "/density"));

    densityJob.waitForCompletion(true);

    Job degreeJob = CalculateNodeDegreeJob.createJob();

    FileInputFormat.setInputPaths(degreeJob, new Path(args[1] + "/density"));
    FileOutputFormat.setOutputPath(degreeJob, new Path(args[1] + "/degree"));

    degreeJob.waitForCompletion(true);

    Job originNodeJob = DeleteOriginNodeJob.createJob();

    FileInputFormat.addInputPath(originNodeJob, new Path(args[0]));
    FileInputFormat.addInputPath(originNodeJob, new Path(args[1] + "/degree"));
    FileOutputFormat.setOutputPath(originNodeJob, new Path(args[1] + "/origin"));

    originNodeJob.waitForCompletion(true);

    Job extremityNodeJob = DeleteExtremityNodeJob.createJob();

    FileInputFormat.addInputPath(extremityNodeJob, new Path(args[1] + "/origin"));
    FileInputFormat.addInputPath(extremityNodeJob, new Path(args[1] + "/degree"));
    FileOutputFormat.setOutputPath(extremityNodeJob, new Path(args[1] + "/extremity"));

    extremityNodeJob.waitForCompletion(true);
  }
}