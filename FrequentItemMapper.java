import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class FrequentItemMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text productID = new Text();

    public void map(Object key, Text value, Mapper.Context context
    ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        String[] inputNums;
        while (itr.hasMoreTokens()) {
            productID.set(itr.nextToken());
            inputNums = (productID.toString()).split(",");
            productID.set(inputNums[1]);
            context.write(productID, one);

        }
    }
}

