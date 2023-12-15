import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SpeedStatisticsReducer
        extends Reducer<NullWritable, Text, NullWritable, Text> {
    @Override
    public void reduce(NullWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        double max = Double.MIN_VALUE;
        double min = Double.MAX_VALUE;
        double count = 0;
        double mean = 0;
        double M2 = 0;

        for (Text val : values) {
            double value = Double.parseDouble(val.toString());
            max = Math.max(max, value);
            min = Math.min(min, value);
            count++;
            double delta = value - mean;
            mean += delta / count;
            double delta2 = value - mean;
            M2 += delta * delta2;
        }

        double variance = count > 1 ? M2 / count : 0;
        double stdev = Math.sqrt(variance);

        String output = "Max: " + max + ", Min: " + min + ", Count: " + count + ", Mean: " + mean + ", Stdev: " + stdev;
        context.write(NullWritable.get(), new Text(output));
    }
}
