import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        double sumX = 0.0;
        double sumY = 0.0;
        int count = 0;

        // Calculate the average of the points in the cluster
        for (Text value : values) {
            String[] tokens = value.toString().split(",");
            sumX += Double.parseDouble(tokens[0]);
            sumY += Double.parseDouble(tokens[1]);
            count++;
        }

        // Calculate the new centroid value
        double newCentroidX = sumX / count;
        double newCentroidY = sumY / count;

        // Emit the new centroid
        context.write(key, new Text(newCentroidX + "," + newCentroidY));
    }
}
