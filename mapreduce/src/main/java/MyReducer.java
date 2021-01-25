import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer {
    @Override
    protected void reduce(Object key, Iterable values, Context context) throws IOException, InterruptedException {
        int result = 0;
        for (Object value : values) {
            result += ((IntWritable)value).get();
        }
        IntWritable intWritable = new IntWritable(result);
        context.write(key,intWritable);
    }
}
