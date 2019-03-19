package cn.myfreecloud.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TwoIndexReducer extends Reducer<Text, Text, Text, Text>{


    //value
	Text v = new Text();
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		StringBuilder sb = new StringBuilder();
		// 拼接
		for(Text text :values){
			sb.append(text.toString() + "\t");
		}
		
		// 设置输出value
		v.set(sb.toString());
		
		// 写出
		context.write(key, v);
	}
}
