import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxMinDate {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(MaxMinDate.class);
        job.setJobName("Max Min Date");
        FileInputFormat.addInputPath(job, new Path("rbda_proj/DOT_Traffic_Speeds_NBE.csv"));
        FileOutputFormat.setOutputPath(job, new Path("rbda_proj/maxMinDate"));
        job.setMapperClass(MaxMinDateMapper.class);
        job.setCombinerClass(MaxMinDateReducer.class);
        job.setReducerClass(MaxMinDateReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
