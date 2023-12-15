package org.rbda;
import org.apache.parquet.example.data.Group;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.schema.GroupType;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class mapper extends Mapper<LongWritable, Group, Text, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    @Override
    public void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
        GroupType schema = value.getType();

        if (schema.containsField("pickup_datetime") && !value.getValueToString(1, 0).isEmpty()) {
            long epochTime = value.getLong("pickup_datetime", 0);
            LocalDateTime date = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochTime), ZoneId.systemDefault());
            int year = date.getYear();
            int month = date.getMonthValue();
            int pickupLocation = getOptionalIntField(value, "PUlocationID");
            int dropoffLocation = getOptionalIntField(value, "DOlocationID");

            if (year==2021 && pickupLocation != 0 && dropoffLocation != 0) {
//                context.write(new Text("pickup "+pickupLocation), one);
//                context.write(new Text("dropoff "+dropoffLocation), one);
//                context.write(new Text(pickupLocation+","+dropoffLocation), one);
                context.write(new Text(date.getMonth()+","+pickupLocation), one);
            }
        }
    }
    private int getOptionalIntField(Group group, String fieldName) {
        if (group.getType().containsField(fieldName) && group.getFieldRepetitionCount(fieldName) > 0) {
            try {
                return (int) group.getDouble(fieldName, 0);
            } catch (RuntimeException ignored) {
                ;
            }
        }
        return 0;
    }
}