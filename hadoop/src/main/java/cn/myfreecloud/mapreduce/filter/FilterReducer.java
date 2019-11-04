package cn.myfreecloud.mapreduce.filter;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FilterReducer extends Reducer<Text, NullWritable, Text, NullWritable>{

	Text k = new Text();
	
	@Override
	protected void reduce(Text key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {

        // 在key上加上回车和换行符
		k.set(key.toString() + "\r\n");
		
		context.write(k, NullWritable.get());
	}

}
