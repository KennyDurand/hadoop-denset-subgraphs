package sogara.hadoop.subgraph;

import java.io.IOException;
import java.lang.InterruptedException;
import java.lang.ClassNotFoundException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;
import java.nio.file.Files;

public class DensestSubgraph {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.newInstance(conf);
        long time = System.currentTimeMillis();
        String outputDirectory = "/DensestSubgraph" + time;
        try {
            fs.copyFromLocalFile(false, false, new Path(System.getProperty("user.dir") + "/" + args[0]), new Path(outputDirectory + "/graph/result"));
            fs.copyFromLocalFile(false, false, new Path(System.getProperty("user.dir") + "/" + args[0]), new Path(outputDirectory + "/graph/subgraph"));
        } catch (IOException e) {
            throw e;
        }

        Job densityJob = CalculateGraphDensityJob.createJob();

        FileInputFormat.setInputPaths(densityJob, new Path(outputDirectory + "/graph/result"));
        FileOutputFormat.setOutputPath(densityJob, new Path(outputDirectory + "/density"));

        FSDataInputStream filein;
        LineReader in;
        Text reader;
        float currentDensity = 0;

        densityJob.waitForCompletion(true);

        if (fs.getFileLinkStatus(new Path(outputDirectory + "/density/part-r-00000")).getLen() > 0) {
            filein = fs.open(new Path(outputDirectory + "/density/part-r-00000"));
            in = new LineReader(filein, conf);
            reader = new Text();
            in.readLine(reader);
            currentDensity = Float.parseFloat(reader.toString().split("\t")[0]);
        }
        float newDensity;

        while (true) {
            Job degreeJob = CalculateNodeDegreeJob.createJob();

            FileInputFormat.addInputPath(degreeJob, new Path(outputDirectory + "/density"));
            FileOutputFormat.setOutputPath(degreeJob, new Path(outputDirectory + "/degree"));

            degreeJob.waitForCompletion(true);

            Job originNodeJob = DeleteOriginNodeJob.createJob();

            FileInputFormat.addInputPath(originNodeJob, new Path(outputDirectory + "/graph/subgraph"));
            FileInputFormat.addInputPath(originNodeJob, new Path(outputDirectory + "/degree"));
            FileOutputFormat.setOutputPath(originNodeJob, new Path(outputDirectory + "/origin"));

            originNodeJob.waitForCompletion(true);

            fs.delete(new Path(outputDirectory + "/graph/subgraph"), true);

            Job extremityNodeJob = DeleteExtremityNodeJob.createJob();

            FileInputFormat.addInputPath(extremityNodeJob, new Path(outputDirectory + "/origin"));
            FileInputFormat.addInputPath(extremityNodeJob, new Path(outputDirectory + "/degree"));
            FileOutputFormat.setOutputPath(extremityNodeJob, new Path(outputDirectory + "/graph/subgraph"));

            extremityNodeJob.setNumReduceTasks(3);
            extremityNodeJob.waitForCompletion(true);

            fs.delete(new Path(outputDirectory + "/degree"), true);
            fs.delete(new Path(outputDirectory + "/density"), true);
            fs.delete(new Path(outputDirectory + "/origin"), true);

            densityJob = CalculateGraphDensityJob.createJob();

            FileInputFormat.addInputPath(densityJob, new Path(outputDirectory + "/graph/subgraph"));
            FileOutputFormat.setOutputPath(densityJob, new Path(outputDirectory + "/density"));

            densityJob.waitForCompletion(true);

            if (fs.getFileLinkStatus(new Path(outputDirectory + "/density/part-r-00000")).getLen() > 0) {
                filein = fs.open(new Path(outputDirectory + "/density/part-r-00000"));
                in = new LineReader(filein, conf);
                reader = new Text();
                in.readLine(reader);
                newDensity = Float.parseFloat(reader.toString().split("\t")[0]);
            } else {
                break;
            }

            if (newDensity > currentDensity) {
                currentDensity = newDensity;
                fs.delete(new Path(outputDirectory + "/graph/result"), false);
                fs.rename(new Path(outputDirectory + "/graph/subgraph/part-r-00000"), new Path(outputDirectory + "/graph/result"));
            }
        }

        try {
            fs.copyToLocalFile(false, new Path(outputDirectory + "/graph/result"), new Path(System.getProperty("user.dir") + "/" + args[1] + "/" + time));
        } catch (IOException e) {
            throw e;
        }
    }
}