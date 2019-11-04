package cn.myfreecloud.mapreduce.wholefile;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable>{

	Text k = new Text();

    /**
     * 需要提前完成 的事情就放在setup里面完成
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// 获取切片路径，
		FileSplit inputSplit = (FileSplit) context.getInputSplit();
		// 文件路径
		Path path = inputSplit.getPath();
		
		// 设置k
		k.set(path.toString());
	}
	
	@Override
	protected void map(NullWritable key, BytesWritable value,
			Context context) throws IOException, InterruptedException {
		// 控制输出数据
		context.write(k, value);
	}
	
}
