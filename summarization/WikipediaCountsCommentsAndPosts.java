package richard.nicolas.summarization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import richard.nicolas.mappers.PostMapper;
import richard.nicolas.mappers.CommentMapper;
import richard.nicolas.reducers.IntSumReducer;

// Find most referenced Wikipedia page in Stackoverflow comments

public class WikipediaCountsCommentsAndPosts {


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 4) {
            System.err.println("Usage: WikipediaCountsCommentsAndPosts <post data> <comment data> <cache file> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance();
        job.setJobName("StackOverflow Wikipedia URL Count");
        job.setJarByClass(WikipediaIndex.class);

        // Use multiple inputs to set which input uses what mapper
        // This will keep parsing of each data set separate from a logical
        // standpoint
        MultipleInputs.addInputPath(job, new Path(otherArgs[0]),
                TextInputFormat.class, PostMapper.class);

        MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
                TextInputFormat.class, CommentMapper.class);

        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

        job.addCacheFile(new Path(otherArgs[1]).toUri());

        int code = job.waitForCompletion(true) ? 0 : 1;
        if(code==0){
            for (Counter counter : job.getCounters().getGroup(
                    CommentMapper.COUNTER_GROUP_NAME)) {
                System.out.println(CommentMapper.COUNTER_GROUP_NAME + " " + counter.getDisplayName() + "\t"
                        + counter.getValue());
            }
        }

        if(code==0){
            for (Counter counter : job.getCounters().getGroup(
                    PostMapper.COUNTER_GROUP_NAME)) {
                System.out.println(PostMapper.COUNTER_GROUP_NAME + " " + counter.getDisplayName() + "\t"
                        + counter.getValue());
            }
        }

        System.exit(code);
    }

}
