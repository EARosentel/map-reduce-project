import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CountOrdersMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable zero = new IntWritable(0);
    private Text orderID = new Text();

    public void map(Object key, Text value, Mapper.Context context
    ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), ",");

        //counter increments to make sure the first token of each line is stored as the orderID
        int counter = 0;
        while (itr.hasMoreTokens()) {
            if (counter == 0) {
                orderID.set(itr.nextToken());
                context.write(orderID, zero);
            } else if (counter == 3) {
                counter = -1;
            }
            counter++;
        }
    }
}

