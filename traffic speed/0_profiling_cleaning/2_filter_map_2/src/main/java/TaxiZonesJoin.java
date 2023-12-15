import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaxiZonesJoin {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.addCacheFile(new Path("rbda_proj/taxi_zones.txt").toUri());
        job.setJobName("TaxiZonesJoin");
        job.setJarByClass(TaxiZonesJoin.class);
        FileInputFormat.addInputPath(job,
                new Path("rbda_proj/filter1"));
        FileOutputFormat.setOutputPath(job, new Path("rbda_proj/filter2"));
        job.setMapperClass(TaxiZonesJoinMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
