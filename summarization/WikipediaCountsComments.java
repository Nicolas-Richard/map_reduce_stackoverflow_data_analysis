package richard.nicolas.summarization;

// Find most referenced Wikipedia page in Stackoverflow comments

import java.io.*;

import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import richard.nicolas.mappers.CommentMapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Counter;
//import richard.nicolas.reducers.IntSumReducer;



public class WikipediaCountsComments {


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: WikipediaCountsComments <in> <cachefile> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance();
        job.setJobName("StackOverflow Wikipedia URL Count");
        job.setJarByClass(WikipediaIndex.class);
        job.setMapperClass(CommentMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        job.addCacheFile(new Path(otherArgs[1]).toUri());

        int code = job.waitForCompletion(true) ? 0 : 1;
        if(code==0){
            for (Counter counter : job.getCounters().getGroup(
                    CommentMapper.COUNTER_GROUP_NAME)) {
                System.out.println(counter.getDisplayName() + "\t"
                        + counter.getValue());
            }
        }
        System.exit(code);
    }

}
