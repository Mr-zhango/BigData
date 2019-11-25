package com.z.transformer.mr.statistics;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.z.transformer.common.KpiType;
import com.z.transformer.dimension.key.stats.StatsUserDimension;
import com.z.transformer.dimension.value.MapWritableValue;

public class NewInstallUserReducer extends Reducer<StatsUserDimension, Text, StatsUserDimension, MapWritableValue> {
	// 保存唯一id的集合，用于计算新增的访客数量
	private Set<String> uniqueSets = new HashSet<String>();
	// 给定输出value
	private MapWritableValue outputValue = new MapWritableValue();

	@Override
	protected void reduce(StatsUserDimension key, 
			Iterable<Text> values, 
			Context context) throws IOException, InterruptedException {
		// 1. 统计uuid出现的次数，去重
		for (Text uuid : values) {
			this.uniqueSets.add(uuid.toString());
		}

		// 2. 输出数据拼装
		MapWritable map = new MapWritable();
		map.put(new IntWritable(-1), new IntWritable(this.uniqueSets.size()));
		this.outputValue.setValue(map);
	
		// 设置kpi
		if (KpiType.BROWSER_NEW_INSTALL_USER.name.equals(key.getStatsCommon().getKpi().getKpiName())) {
			// 表示处理的是browser new install user kpi的计算
			this.outputValue.setKpi(KpiType.BROWSER_NEW_INSTALL_USER);
		} else if (KpiType.NEW_INSTALL_USER.name.equals(key.getStatsCommon().getKpi().getKpiName())) {
			// 表示处理的是new install user kpi的计算
			this.outputValue.setKpi(KpiType.NEW_INSTALL_USER);
		}
		
		// 3. 输出数据
		context.write(key, outputValue);
	}
}
