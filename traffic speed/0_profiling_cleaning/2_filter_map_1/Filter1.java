import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Filter1 {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.addCacheFile(new Path("rbda_proj/opencsv-5.9.jar").toUri()); // put dependencies on the distributed cache for each worker node
        job.setJobName("Filter1");
        job.setJarByClass(Filter1.class);
        FileInputFormat.addInputPath(job,
                new Path("rbda_proj/DOT_Traffic_Speeds_NBE.csv"));
        FileOutputFormat.setOutputPath(job, new Path("rbda_proj/filter1"));
        job.setMapperClass(Filter1Mapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
