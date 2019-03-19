package cn.myfreecloud.mapreduce.table;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

	TableBean bean = new TableBean();
	Text k = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
//		1 获取输入文件类型(文件类型的切片)
		FileSplit inputSplit = (FileSplit) context.getInputSplit();
		String name = inputSplit.getPath().getName();
		
//		2 获取输入数据
		String line = value.toString();
		
//		3 不同文件分别处理
		if (name.startsWith("order")) {// 订单相关信息处理
			// 切割
			String[] fields = line.split("\t");
			
			// 封装bean对象 1001	01	1
			bean.setOrder_id(fields[0]);
			bean.setP_id(fields[1]);
			bean.setAmount(Integer.parseInt(fields[2]));
			
			bean.setPname("");
			bean.setFlag("0");
			
			// 设置key值
			k.set(fields[1]);
			
		}else {// 产品表信息处理     01	小米

			// 切割
			String[] fields = line.split("\t");
			
			// 封装bean对象
			bean.setOrder_id("");
			bean.setP_id(fields[0]);
			bean.setAmount(0);
			bean.setPname(fields[1]);
			bean.setFlag("1");
			
			// 设置key值
			k.set(fields[0]);
		}
		
//		4 封装bean对象输出
		context.write(k, bean);
	}

}
