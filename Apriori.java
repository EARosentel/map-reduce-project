import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;



public class Apriori {

    public static void main(String[] args) throws Exception {
        JobControl jobControl = new JobControl("jobChain");

        //set up for frequent items job
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "occurrence of items");
        job1.setJarByClass(Apriori.class);

        job1.setMapperClass(FrequentItemMapper.class);
        job1.setCombinerClass(FrequentItemReducer.class);
        job1.setReducerClass(FrequentItemReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]+"/order_products"));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]+"/tempProds"));


        //add job1 to jobControl
        ControlledJob controlledJob1 = new ControlledJob(conf1);
        controlledJob1.setJob(job1);
        jobControl.addJob(controlledJob1);


        //-----------------------------------
        //set up for count orders job
        Configuration conf2 = new Configuration();

        Job job2 = Job.getInstance(conf2);
        job2.setJarByClass(Apriori.class);
        job2.setJobName("count orders");

        FileInputFormat.setInputPaths(job2, new Path(args[0]+"/order_products"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/tempOrders"));

        job2.setMapperClass(CountOrdersMapper.class);
        job2.setCombinerClass(CountOrdersReducer.class);
        job2.setReducerClass(CountOrdersReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        //add job2 to job control
        ControlledJob controlledJob2 = new ControlledJob(conf2);
        controlledJob2.setJob(job2);
        jobControl.addJob(controlledJob2);
        


        //make sure first two jobs finish before running third job

        System.out.println(job1.waitForCompletion(true) ? 0 : 1);
        System.out.println(job2.waitForCompletion(true) ? 0 : 1);


        //set up for frequent item identification job
        Configuration conf3 = new Configuration();

        Job job3 = Job.getInstance(conf3);
        job3.setJarByClass(Apriori.class);
        job3.setJobName("frequent items");

        FileInputFormat.setInputPaths(job3, new Path(args[1]+"/tempProds"));
        FileOutputFormat.setOutputPath(job3, new Path(args[1] + "/freqItems"));

        job3.setMapperClass(FrequentItemsMapper2.class);
        job3.setCombinerClass(FrequentItemsReducer2.class);
        job3.setReducerClass(FrequentItemsReducer2.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);


        //add job3 to job control
        ControlledJob controlledJob3 = new ControlledJob(conf3);
        controlledJob3.setJob(job3);

        jobControl.addJob(controlledJob3);
        
        
        // waits for job to finish
        System.out.println(job3.waitForCompletion(true) ? 0 : 1);

        System.exit(0);

    }
}
