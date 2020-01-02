import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

// 输入数据样例类
case class UserBehavior( userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long )
// 输出数据样例类
case class ItemViewCount( itemId: Long, windowEnd: Long, count: Long )

object HotItemsKafkaVersion {
  def main(args: Array[String]): Unit = {


    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    

    // 创建一个env Flink自己根据环境选择是本地还是集群环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 显式地定义Time类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置一个并行度(调试模式使用)
    env.setParallelism(1)

    val stream = env
      //.readTextFile("C:\\develop\\IDEAProjects\\BigData\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
        .addSource(new FlinkKafkaConsumer[String]("hotitems",new SimpleStringSchema(),properties))
      .map(line => {

        val linearray = line.split(",")
        // userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long
        UserBehavior( linearray(0).toLong, linearray(1).toLong, linearray(2).toInt, linearray(3), linearray(4).toLong )

      })

      // 升序时间戳排序
      // 指定时间戳和watermark
      .assignAscendingTimestamps(_.timestamp * 1000)
      // 只要pv数据
      .filter(_.behavior == "pv")
      //  商品id进行分流
      .keyBy("itemId")
      // 添加时间窗口,是一个滑动窗口,每5分钟滑动一个,统计一小时之内的数据
      .timeWindow(Time.hours(1), Time.minutes(5))
      //
      .aggregate( new CountAgg(), new WindowResultFunction() )
      .keyBy("windowEnd")
      .process( new TopNHotItems(3))
      .print()

    // 调用execute执行任务
    env.execute("Hot Items Job")
  }

  // 自定义实现聚合函数
  class CountAgg extends AggregateFunction[UserBehavior, Long, Long]{
    // 累加操作 + 1
    override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

    // 创建的累加器的初始值 0
    override def createAccumulator(): Long = 0L

    // 获取累加器
    override def getResult(accumulator: Long): Long = accumulator

    // 合并
    override def merge(a: Long, b: Long): Long = a + b
  }

  // 自定义实现Window Function，输出ItemViewCount格式
  class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow]{
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
      val count = input.iterator.next()
      out.collect(ItemViewCount(itemId, window.getEnd, count))
    }
  }

  // 自定义实现process function
  class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String]{

    // 定义状态ListState 并且设置初始值 _ 系统自动选择初始值
    private var itemState: ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      // 命名状态变量的名字和类型
      val itemStateDesc = new ListStateDescriptor[ItemViewCount]("itemState", classOf[ItemViewCount])
      itemState = getRuntimeContext.getListState(itemStateDesc)
    }

    override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
      itemState.add(i)
      // 注册定时器，触发时间定为 windowEnd + 1，触发时说明window已经收集完成所有数据
      context.timerService.registerEventTimeTimer( i.windowEnd + 1 )
    }

    // 定时器触发操作，从state里取出所有数据，排序取TopN，输出
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      // 获取所有的商品点击信息
      val allItems: ListBuffer[ItemViewCount] = ListBuffer()

      // scala 更好支持java的转环包
      import  scala.collection.JavaConversions._
      for(item <- itemState.get){
        allItems += item
      }
      // 清除状态中的数据，释放空间
      itemState.clear()

      // 按照点击量从大到小排序，选取TopN  sortBy 默认从小到大排序
      val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

      // 将排名数据格式化，便于打印输出
      val result: StringBuilder = new StringBuilder
      result.append("================start====================\n")
      result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")

      //sortedItems.indices 得到索引 0 --> length的循环方式
      for( i <- sortedItems.indices ){
        val currentItem: ItemViewCount = sortedItems(i)
        // 输出打印的格式 e.g.  No1：  商品ID=12224  浏览量=2413
        result.append("No").append(i+1).append(":")
          .append("  商品ID=").append(currentItem.itemId)
          .append("  浏览量=").append(currentItem.count).append("\n")
      }
      result.append("================start====================\n\n")
      // 控制输出频率
      Thread.sleep(1000)
      out.collect(result.toString)
    }
  }
}
