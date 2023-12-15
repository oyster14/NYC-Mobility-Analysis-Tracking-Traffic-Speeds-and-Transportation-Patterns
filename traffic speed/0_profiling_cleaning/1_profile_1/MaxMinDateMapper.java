import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxMinDateMapper
        extends Mapper<LongWritable, Text, NullWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if (line.startsWith("ID")) {
            return;
        }
        String[] parts = line.split(",");
        context.write(NullWritable.get(), new Text(parts[4]));
    }
}
