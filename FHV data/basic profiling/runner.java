package org.rbda;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class runner {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: RBDA <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();

        job.setJarByClass(runner.class);
        job.setJobName("RBDA");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(mapper.class);
        job.setReducerClass(reducer.class);

//        job.setCombinerClass(reducer.class);
        job.setNumReduceTasks(1);

        job.setInputFormatClass(ParquetInputFormat.class);

        ParquetInputFormat.setReadSupportClass(job, GroupReadSupport.class);
//        job.setOutputFormatClass(TextOutputFormat.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
