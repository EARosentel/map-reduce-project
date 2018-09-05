import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.StringTokenizer;

public class FrequentItemsMapper2 extends Mapper<Object, Text, Text, IntWritable> {

    private IntWritable frequency = new IntWritable();
    private Text productID = new Text();

    public static int countLines(String filename) throws IOException {
        InputStream is = new BufferedInputStream(new FileInputStream(filename));
        try {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1) {
                empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        } finally {
            is.close();
        }
    }

    public void map(Object key, Text value, Mapper.Context context
    ) throws IOException, InterruptedException {


        int totOrders = countLines("/home/cloudera/Desktop/FP-output/tempOrders/part-r-00000");
        StringTokenizer itr = new StringTokenizer(value.toString());

        int counter = 0;
        double occurrence;
        int freq;

        while (itr.hasMoreTokens()) {
            if (counter == 0) {
                productID.set(itr.nextToken());
            } else {
                occurrence = Integer.parseInt(itr.nextToken());
                freq = Math.toIntExact(Math.round((occurrence / totOrders)*10000));
                frequency.set(freq);
                context.write(productID, frequency);
                counter = -1;
            }
            counter++;
        }

    }
}




