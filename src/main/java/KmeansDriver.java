import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class KmeansDriver {
    private static Path file = new Path("http://localhost:50070/explorer.html#/centers.csv");
    private static Path output = new Path("http://localhost:50070/explorer.html#/output");
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "KMeans");


        job.setMapperClass(KmeansMapper.class);
        job.setReducerClass(KmeansReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Load initial centroids into DistributedCache
        // Make sure to adjust the path based on your Hadoop environment
        job.addCacheFile(new Path("http://localhost:50070/explorer.html#/centers.csv").toUri());

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
