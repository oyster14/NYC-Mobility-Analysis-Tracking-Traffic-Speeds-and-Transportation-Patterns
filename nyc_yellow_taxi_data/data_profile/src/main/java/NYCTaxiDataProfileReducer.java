import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.parquet.Log;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;

public class NYCTaxiDataProfileReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double count = 0;
        double tripDistanceMax = Double.MIN_VALUE;
        double tripDistanceMin = Double.MAX_VALUE;
        double tripDistanceMean = 0;
        double totalAmountMax = Double.MIN_VALUE;
        double totalAmountMin = Double.MAX_VALUE;
        double totalAmountMean = 0;

        for (Text value : values) {
            count++;
            String[] data = value.toString().split(",");
            double tripDistance = Double.parseDouble(data[0]);
            tripDistanceMax = Math.max(tripDistanceMax, tripDistance);
            tripDistanceMin = Math.min(tripDistanceMin, tripDistance);
            tripDistanceMean += ((tripDistance - tripDistanceMean) / count);

            double totalAmount = Double.parseDouble(data[1]);
            totalAmountMax = Math.max(totalAmountMax, totalAmount);
            totalAmountMin = Math.min(totalAmountMin, totalAmount);
            totalAmountMean += ((totalAmount - totalAmountMean) / count);
        }

        String output = "For month: " + key + "\n"
                + "Trip Distance: Max: " + tripDistanceMax + ", Min: " + tripDistanceMin + ", Mean: " + tripDistanceMean + "\n"
                + "Total Amount: Max: " + totalAmountMax + ", Min: " + totalAmountMin + ", Mean: " + totalAmountMean + "\n";
        context.write(NullWritable.get(), new Text(output));
    }
}