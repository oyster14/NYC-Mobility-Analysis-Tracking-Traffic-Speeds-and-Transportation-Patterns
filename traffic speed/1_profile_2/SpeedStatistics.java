import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SpeedStatistics {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(SpeedStatistics.class);
        job.setJobName("Speed Statistics");
        FileInputFormat.addInputPath(job, new Path("rbda_proj/DOT_Traffic_Speeds_NBE.csv"));
        FileOutputFormat.setOutputPath(job, new Path("rbda_proj/speed_statistics"));
        job.setMapperClass(SpeedStatisticsMapper.class);
        job.setReducerClass(SpeedStatisticsReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
