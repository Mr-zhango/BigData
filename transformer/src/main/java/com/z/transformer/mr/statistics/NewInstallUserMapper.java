package com.z.transformer.mr.statistics;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.z.transformer.common.DateEnum;
import com.z.transformer.common.EventLogConstants;
import com.z.transformer.common.GlobalConstants;
import com.z.transformer.common.KpiType;
import com.z.transformer.dimension.key.base.BrowserDimension;
import com.z.transformer.dimension.key.base.DateDimension;
import com.z.transformer.dimension.key.base.KpiDimension;
import com.z.transformer.dimension.key.base.PlatformDimension;
import com.z.transformer.dimension.key.stats.StatsCommonDimension;
import com.z.transformer.dimension.key.stats.StatsUserDimension;
import com.z.transformer.util.TimeUtil;

public class NewInstallUserMapper extends TableMapper<StatsUserDimension, Text> {
	private static final Logger logger = Logger.getLogger(NewInstallUserMapper.class);
	private byte[] family = EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME;

	// 定义输出key
	private StatsUserDimension outputKey = new StatsUserDimension();
	// 定义输出value
	private Text outputValue = new Text();
	// 映射输出key中的StatsCommonDimension属性
	private StatsCommonDimension statsCommonDimension = this.outputKey.getStatsCommon();

	private long date, endOfDate; // 给定需要运行天的起始时间戳和结束时间戳
	private long firstThisWeekOfDate, endThisWeekOfDate; // 给定运行天所属周的起始时间戳和结束时间戳
	private long firstThisMonthOfDate, firstDayOfNextMonth; // 给定运行天所属月的起始时间戳和结束时间戳

	// 创建kpi维度
	private KpiDimension newInstallUsersKpiDimension = new KpiDimension(KpiType.NEW_INSTALL_USER.name);
	private KpiDimension browserNewInstallUsersKpiDimension = new KpiDimension(KpiType.BROWSER_NEW_INSTALL_USER.name);
	
	// 给定一个特殊占位的browser dimension
	private BrowserDimension defaultBrowserDimension = new BrowserDimension("", "");

	@Override
	protected void setup(Mapper<ImmutableBytesWritable, Result, StatsUserDimension, Text>.Context context)
			throws IOException, InterruptedException {
		// 1. 获取参数配置项的上下文
		Configuration conf = context.getConfiguration();
		// 2. 获取我们给定的运行时间参数，获取运行的是哪一天的数据
		String date = conf.get(GlobalConstants.RUNNING_DATE_PARAMES);
		// 3. 用户传入的分析时间转换成时间戳，当前时间0点0分0秒
		this.date = TimeUtil.parseString2Long(date);
		//传入时间当天结束的时间戳
		this.endOfDate = this.date + GlobalConstants.DAY_OF_MILLISECONDS;
		// 传入时间获取所属周第一天的时间戳
		this.firstThisWeekOfDate = TimeUtil.getFirstDayOfThisWeek(this.date);
		// 获取传入时间下一周的第一天的时间戳
		this.endThisWeekOfDate = TimeUtil.getFirstDayOfNextWeek(this.date);

		// 获取月的前后时间范围
		this.firstThisMonthOfDate = TimeUtil.getFirstDayOfThisMonth(this.date);
		this.firstDayOfNextMonth = TimeUtil.getFirstDayOfNextMonth(this.date);
	}

	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {
		/* 1. 获取属性，参数值 */
		String serverTime = Bytes
				.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)));
		String platformName = Bytes
				.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)));
		String platformVersion = Bytes
				.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_VERSION)));
		String browserName = Bytes
				.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME)));
		String browserVersion = Bytes
				.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION)));
		String uuid = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_UUID)));

		/* 2. 针对数据进行过滤 */
		if (StringUtils.isBlank(platformName) || StringUtils.isBlank(uuid)) {
			logger.debug("数据格式异常，直接过滤数据:" + platformName);
			return; // 过滤数据，无效数据
		}

		/* 属性处理 */
		long longOfServerTime = -1;
		try {
			longOfServerTime = Long.valueOf(serverTime);
		} catch (Exception e) {
			logger.debug("服务器时间格式异常:" + serverTime);
			return; // 服务器时间异常的数据直接过滤掉
		}

		/* 3. 构建维度信息 */
		// 获取当前服务器时间对于的天维度的对象
		DateDimension dayOfDimension = DateDimension.buildDate(longOfServerTime, DateEnum.DAY);
		// 获取当前服务器时间对应的周维度的对象
		DateDimension weekOfDimension = DateDimension.buildDate(longOfServerTime, DateEnum.WEEK);
		// 获取当前服务器时间对应的月维度的对象
		DateDimension monthOfDimension = DateDimension.buildDate(longOfServerTime, DateEnum.MONTH);

		// 构建platform维度
		List<PlatformDimension> platforms = PlatformDimension.buildList(platformName, platformVersion);

		// 构建browser维度
		List<BrowserDimension> browsers = BrowserDimension.buildList(browserName, browserVersion);

		// 4、设置outputValue
		this.outputValue.set(uuid);

		// 5、设置outputKey
		for (PlatformDimension pf : platforms) {
			//设置浏览器维度
			this.outputKey.setBrowser(this.defaultBrowserDimension);
			//设置platform维度
			this.statsCommonDimension.setPlatform(pf);
			//设置kpi
			this.statsCommonDimension.setKpi(this.newInstallUsersKpiDimension);

			/* 下面的代码是处理对应于stats_user表的统计数据的 */
			// 处理不同时间维度的情况
			// 处理天维度数据，要求服务器时间处于指定日期的范围内,[today, endOfDate)
			if (longOfServerTime >= date && longOfServerTime < endOfDate) {
				// 设置时间维度为服务器当天的维度
				this.statsCommonDimension.setDate(dayOfDimension);
				// 输出数据
				context.write(outputKey, outputValue);
			}

			// 处理周, 范围：[firstThisWeekOfDate, endThisWeekOfDate)
			if (longOfServerTime >= firstThisWeekOfDate && longOfServerTime < endThisWeekOfDate) {
				// 设置时间维度为服务器所属周的维度
				this.statsCommonDimension.setDate(weekOfDimension);
				// 输出数据
				context.write(outputKey, outputValue);
			}

			// 处理月, 范围：[firstThisMonthOfDate, firstDayOfNextMonth)
			if (longOfServerTime >= firstThisMonthOfDate && longOfServerTime < firstDayOfNextMonth) {
				// 设置时间维度为服务器所属周的维度
				this.statsCommonDimension.setDate(monthOfDimension);
				// 输出数据
				context.write(outputKey, outputValue);
			}

			// 输出stats_device_browser相关的统计数据
			this.statsCommonDimension.setKpi(this.browserNewInstallUsersKpiDimension);
			for (BrowserDimension br : browsers) {
				// 设置browser dimension相关参数
				this.outputKey.setBrowser(br);

				// 处理不同时间维度的情况
				// 处理天维度数据，要求当前事件的服务器时间处于指定日期的范围内,[今天0点, 明天0点)
				if (longOfServerTime >= date && longOfServerTime < endOfDate) {
					// 设置时间维度为服务器当天的维度
					this.statsCommonDimension.setDate(dayOfDimension);
					// 输出数据
					context.write(outputKey, outputValue);
				}

				// 处理周, 范围：[firstThisWeekOfDate, endThisWeekOfDate)
				if (longOfServerTime >= firstThisWeekOfDate && longOfServerTime < endThisWeekOfDate) {
					// 设置时间维度为服务器所属周的维度
					this.statsCommonDimension.setDate(weekOfDimension);
					// 输出数据
					context.write(outputKey, outputValue);
				}

				// 处理月, 范围：[firstThisMonthOfDate, firstDayOfNextMonth)
				if (longOfServerTime >= firstThisMonthOfDate && longOfServerTime < firstDayOfNextMonth) {
					// 设置时间维度为服务器所属周的维度
					this.statsCommonDimension.setDate(monthOfDimension);
					// 输出数据
					context.write(outputKey, outputValue);
				}
				
			}
		}
	}
}
