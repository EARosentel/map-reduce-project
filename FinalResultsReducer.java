import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FinalResultsReducer extends Reducer<Text,IntWritable,Text,IntWritable> {


    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
    ) throws IOException, InterruptedException {

        for (IntWritable val : values) {
            context.write(key, val);
        }

    }

}