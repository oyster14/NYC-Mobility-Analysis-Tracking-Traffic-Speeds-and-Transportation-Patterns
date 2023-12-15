import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.parquet.Log;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;

public class NYCTaxiDataProfileMapper extends Mapper<LongWritable, Group, IntWritable, Text> {
    private static final Log LOG = new Log(NYCTaxiDataProfileMapper.class);

    @Override
    public void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
        long pickupDatetimeMicros = value.getLong("pickup_datetime", 0);
        LOG.info("pickupDatetimeMicros: " + pickupDatetimeMicros);
        Instant pickupInstant = Instant.ofEpochMilli(pickupDatetimeMicros / 1000); // Convert to milliseconds
        LocalDateTime pickupDateTime = LocalDateTime.ofInstant(pickupInstant, ZoneId.systemDefault());

        int pickupMonth = pickupDateTime.getMonthValue();
        int pickupYear = pickupDateTime.getYear();
        LOG.info("PickUp Month: " + pickupMonth);

        double tripDistance = value.getDouble("trip_distance", 0);
        double totalAmount = value.getDouble("total_amount", 0);
        String distanceAndAmount = tripDistance + "," + totalAmount;

        if (pickupYear == 2021) {
            context.write(new IntWritable(pickupMonth), new Text(distanceAndAmount));
        }
    }
}