package com.z.transformer.mr.statistics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.z.transformer.common.EventLogConstants;
import com.z.transformer.common.EventLogConstants.EventEnum;
import com.z.transformer.common.GlobalConstants;
import com.z.transformer.dimension.key.stats.StatsUserDimension;
import com.z.transformer.dimension.value.MapWritableValue;
import com.z.transformer.mr.TransformerMySQLOutputFormat;
import com.z.transformer.util.TimeUtil;

public class NewInstallUserRunner implements Tool {
	// 给定一个参数表示参数上下文
	private Configuration conf = null;

	public static void main(String[] args) {
		try {
			int exitCode = ToolRunner.run(new NewInstallUserRunner(), args);
			if (exitCode == 0) {
				System.out.println("运行成功");
			} else {
				System.out.println("运行失败");
			}
		} catch (Exception e) {
			System.err.println("执行异常:" + e.getMessage());
		}
	}

	@Override
	public void setConf(Configuration conf) {
		// 添加自己开发环境所有需要的其他资源属性文件
		conf.addResource("transformer-env.xml");
		conf.addResource("output-collector.xml");
		conf.addResource("query-mapping.xml");

		// 创建hbase的configuration对象
		this.conf = HBaseConfiguration.create(conf);
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		// 1. 获取参数上下文
		Configuration conf = this.getConf();

		// 2. 处理参数，将参数添加到上下文中
		this.processArgs(conf, args);

		// 3. 创建job
		Job job = Job.getInstance(conf, "new-install-users");

		// 4. 给定job的jar相关信息
		job.setJarByClass(NewInstallUserRunner.class);

		// 5. 给定inputformat相关配置参数
		this.setHBaseInputConfig(job);

		// 6. 给定mapper相关参数
		// 在setHBaseInputConfig已经给定了

		// 7. 给定reducer相关参数
		job.setReducerClass(NewInstallUserReducer.class);
		job.setOutputKeyClass(StatsUserDimension.class);
		job.setOutputValueClass(MapWritableValue.class);

		// 8. 给定outputformat相关参数, 一个自定义的outputformat
		job.setOutputFormatClass(TransformerMySQLOutputFormat.class);

		// 9. 运行
		boolean result = job.waitForCompletion(true);
		// 10. 运行成功返回0，失败返回-1
		return result ? 0 : -1;
	}

	/**
	 * 处理参数，一般处理时间参数
	 * 
	 * @param conf
	 * @param args
	 */
	private void processArgs(Configuration conf, String[] args) {
		String date = null;
		for (int i = 0; i < args.length; i++) {
			if ("-date".equals(args[i])) {
				if (i + 1 < args.length) {
					date = args[++i];
					break;
				}
			}
		}

		// 查看是否需要默认参数
		if (StringUtils.isBlank(date) || !TimeUtil.isValidateRunningDate(date)) {
			date = TimeUtil.getYesterday(); // 默认时间是昨天
		}
		// 保存到上下文中间
		conf.set(GlobalConstants.RUNNING_DATE_PARAMES, date);
	}

	/**
	 * 设置从hbase读取数据的相关配置信息
	 * 
	 * @param job
	 * @throws IOException
	 */
	private void setHBaseInputConfig(Job job) throws IOException {
		Configuration conf = job.getConfiguration();
		// 获取要etl数据的日期是那一天
		String dateStr = conf.get(GlobalConstants.RUNNING_DATE_PARAMES);

		List<Scan> scans = new ArrayList<Scan>();
		// 开始构建scan集合

		// 1. 构建hbase scan filter
		FilterList filterList = new FilterList();
		// 2. 构建只获取launch事件的filter
		filterList.addFilter(new SingleColumnValueFilter(
				EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME,
				Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME), 
				CompareOp.EQUAL,
				Bytes.toBytes(EventEnum.LAUNCH.alias)));
		// 3. 构建部分列的过滤filter
		String[] columns = new String[] { 
				EventLogConstants.LOG_COLUMN_NAME_PLATFORM, // 平台名称
				EventLogConstants.LOG_COLUMN_NAME_VERSION, // 平台版本
				EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME, // 浏览器名称
				EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION, // 浏览器版本
				EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME, // 服务器时间
				EventLogConstants.LOG_COLUMN_NAME_UUID, // 访客唯一标识符
				EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME // 确保根据event名称过滤数据有效，所以需要该列的值
		};
		
		//创建getColumnFilter方法用于得到Filter对象
		filterList.addFilter(this.getColumnFilter(columns));

		// 4. 获取数据来源表所属日期是那些
		long startDate, endDate; // scan的表区间属于[startDate, endDate)
		long date = TimeUtil.parseString2Long(dateStr);
		long firstDayOfWeek = TimeUtil.getFirstDayOfThisWeek(date);
		long firstDayOfMonth = TimeUtil.getFirstDayOfThisMonth(date);
		long lastDayOfWeek = TimeUtil.getFirstDayOfNextWeek(date);
		long lastDayOfMonth = TimeUtil.getFirstDayOfNextMonth(date);
		long endOfDate = TimeUtil.getTodayInMillis() + GlobalConstants.DAY_OF_MILLISECONDS;

		// [date,
		// [firstDayOfWeek
		// [firstDayOfMonth
		// 选择最小的时间戳作为数据输入的起始时间, date一定大于等于其他两个first时间戳值
		startDate = Math.min(firstDayOfMonth, firstDayOfWeek);

		// 获取结束时间
		if (endOfDate > lastDayOfWeek || endOfDate > lastDayOfMonth) {
			endDate = Math.max(lastDayOfMonth, lastDayOfWeek);
		} else {
			endDate = endOfDate;
		}

		// 构建HBaseAdmin, 用于判断表是否存在
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
		} catch (Exception e) {
			throw new RuntimeException("创建hbaseadmin对象失败", e);
		}

		// 5. 构建我们scan集合
		try {
			for (long begin = startDate; begin < endDate;) {
				// 格式化hbase的后缀
				String tableNameSuffix = TimeUtil.parseLong2String(begin, 
						TimeUtil.HBASE_TABLE_NAME_SUFFIX_FORMAT);
				// 构建表名称
				String tableName = EventLogConstants.HBASE_NAME_EVENT_LOGS + tableNameSuffix;

				// 需要先判断表存在，然后当表存在的情况下，再构建scan对象
				if (admin.tableExists(tableName)) {
					// 表存在，进行scan对象创建
					// 构建scan对象
					Scan scan = new Scan();
					// 需要扫描的hbase表名设置到scan对象中
					scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(tableName));
					// 设置过滤对象
					scan.setFilter(filterList);
					// 添加到scans集合中
					scans.add(scan);
				}

				// begin累加
				begin += GlobalConstants.DAY_OF_MILLISECONDS;
			}
		} finally {
			// 关闭HBaseAdmin连接
			try {
				admin.close();
			} catch (Exception e) {
				// nothings
			}
		}

		if (scans.isEmpty()) {
			// 没有表存在，那么job运行失败
			throw new RuntimeException("hbase中没有对应表存在:" + dateStr);
		}

		// windows本地运行，addDependencyJars参数必须设置为 false，默认为 true
		TableMapReduceUtil.initTableMapperJob(
				scans, 
				NewInstallUserMapper.class, 
				StatsUserDimension.class, 
				Text.class,
				job,
				true);
	}

	/**
	 * 创建一个根据列名称过滤数据的filter
	 * 
	 * @param columns
	 * @return
	 */
	private Filter getColumnFilter(String[] columns) {
		byte[][] prefixes = new byte[columns.length][];
		for (int i = 0; i < columns.length; i++) {
			prefixes[i] = Bytes.toBytes(columns[i]);
		}
		return new MultipleColumnPrefixFilter(prefixes);
	}
}
