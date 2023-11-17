import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    private List<double[]> centroids = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Load centroids from the DistributedCache
        try (BufferedReader reader = new BufferedReader(new FileReader("http://localhost:50070/explorer.html#/centers.csv"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split(",");
                double x = Double.parseDouble(tokens[0]);
                double y = Double.parseDouble(tokens[1]);
                centroids.add(new double[]{x, y});
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Read x and y values from the line
        String[] tokens = value.toString().split(",");
        double x = Double.parseDouble(tokens[0]);
        double y = Double.parseDouble(tokens[1]);

        // Find the nearest centroid for each point
        int nearestCentroid = findNearestCentroid(new double[]{x, y});

        // Emit the centroid as key and the point's coordinates as value
        context.write(new IntWritable(nearestCentroid), value);
    }

    private int findNearestCentroid(double[] point) {
        int nearestCentroid = -1;
        double minDistance = Double.MAX_VALUE;

        for (int i = 0; i < centroids.size(); i++) {
            double[] centroid = centroids.get(i);
            double distance = Math.sqrt(Math.pow(point[0] - centroid[0], 2) + Math.pow(point[1] - centroid[1], 2));

            if (distance < minDistance) {
                minDistance = distance;
                nearestCentroid = i;
            }
        }

        return nearestCentroid;
    }
}
