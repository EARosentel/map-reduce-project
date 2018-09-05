import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FrequentItemsReducer2 extends Reducer<Text,IntWritable,Text,IntWritable> {


    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
    ) throws IOException, InterruptedException {

        for (IntWritable val : values) {
            if(val.get() > 200){
                context.write(key, val);
            }
        }

    }
}