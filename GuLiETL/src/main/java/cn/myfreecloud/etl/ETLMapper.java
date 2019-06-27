package cn.myfreecloud.etl;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class ETLMapper extends Mapper<LongWritable, Text, Text,NullWritable> {

	private Text k = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    //获取一行数据,清洗,切割
		String etlString = ETLUtil.oriString2ETLString(value.toString());

		//脏数据直接返回
		if(StringUtils.isBlank(etlString)) {
            return;
        }
		//数据写出
		k.set(etlString);
		context.write(k,NullWritable.get());
	}
}
