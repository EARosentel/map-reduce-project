import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Comparator;
import java.io.File;
import java.util.Scanner;
import java.util.Arrays;

import java.io.IOException;
import java.util.StringTokenizer;

public class FinalResultsMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text thisproduct = new Text();

    public void map(Object key, Text value, Mapper.Context context
    ) throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString(),"\r\n");
        String[] inputText;
        int[][] prodInfo = new int[50][2];
        int index=0;
        //reading through the products file


        //read frequent items file into an array
        File file = new File("/home/cloudera/Desktop/FP-output/freqItems/part-r-00000");
        Scanner scanner = new Scanner(file);
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            String[] temp= line.split("\t");
            prodInfo[index][0] = Integer.parseInt(temp[0]);
            prodInfo[index][1] = Integer.parseInt(temp[1]);
            index++;
        }

        Arrays.sort(prodInfo, new Comparator<int[]>() {
            @Override
            //arguments to this method represent the arrays to be sorted
            public int compare(int[] o1, int[] o2) {
                //get the item ids which are at index 0 of the array
                Integer itemIdOne = o1[0];
                Integer itemIdTwo = o2[0];
                // sort on item id
                return itemIdOne.compareTo(itemIdTwo);
            }
        });

        index = 0;

        while (itr.hasMoreTokens()) {
            thisproduct.set(itr.nextToken());
            inputText = thisproduct.toString().split(",");
            context.write(new Text(inputText[1]), new IntWritable(prodInfo[index][1]));
            index++;

            context.write(new Text(inputText[0]), new IntWritable(prodInfo[index][0]));

        }
    }
}