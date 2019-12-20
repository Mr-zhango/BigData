import java.util.{Date, Random, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object SessionStatisticAgg {

  def main(args: Array[String]): Unit = {

    // 获取查询的限制条件
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)

    // 获取全局独一无二的主键
    val taskUUID = UUID.randomUUID().toString

    // 创建sparkConf
    val sparkConf = new SparkConf().setAppName("session").setMaster("local[*]")

    // 创建sparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // actionRDD: rdd[UserVisitAction]
    val actionRDD = getActionRDD(sparkSession, taskParam)

    // sessionId2ActionRDD: rdd[(sid, UserVisitAction)]
    val sessionId2ActionRDD = actionRDD.map{
      item => (item.session_id, item)
    }

    // sessionId2GroupRDD: rdd[(sid, iterable(UserVisitAction))]
    val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()

    // sparkSession.sparkContext.setCheckpointDir()
    sessionId2GroupRDD.cache()
    // sessionId2GroupRDD.checkpoint()

    // 获取聚合数据里面的聚合信息
    val sessionId2FullInfoRDD = getFullInfoData(sparkSession, sessionId2GroupRDD)

    // 创建自定义累加器对象
    val sessionStatAccumulator = new SessionStatAccumulator
    // 注册自定义累加器
    sparkSession.sparkContext.register(sessionStatAccumulator, "sessionAccumulator")

    // 过滤用户数据
    val sessionId2FilterRDD = getFilteredData(taskParam, sessionStatAccumulator, sessionId2FullInfoRDD)

    sessionId2FilterRDD.count()

    // 获取最终的统计结果
    getFinalData(sparkSession, taskUUID, sessionStatAccumulator.value)

    // 需求二：session随机抽取
    // sessionId2FilterRDD： RDD[(sid, fullInfo)] 一个session对应一条数据，也就是一个fullInfo
    sessionRandomExtract(sparkSession, taskUUID, sessionId2FilterRDD)
  }

  // 生成需要随机抽取的数据的list 索引
  // extractPerDay      一天一共要抽取多少个
  // daySessionCount    一天一共有多少个
  // hourCountMap       每个小时有多少个
  // hourListMap        结果list
  def generateRandomIndexList(extractPerDay:Long,
                              daySessionCount:Long,
                              hourCountMap:mutable.HashMap[String, Long],
                              hourListMap:mutable.HashMap[String, ListBuffer[Int]]): Unit ={
    for((hour, count) <- hourCountMap){
      // 获取一个小时要抽取多少条数据
      var hourExrCount = ((count / daySessionCount.toDouble) * extractPerDay).toInt
      // 避免一个小时要抽取的数量超过这个小时的总数
      if(hourExrCount > count){
        hourExrCount = count.toInt
      }

      val random = new Random()

      hourListMap.get(hour) match{
          // 不存在
        case None => hourListMap(hour) = new ListBuffer[Int]
          for(i <- 0 until hourExrCount){
            var index = random.nextInt(count.toInt)
            // 如果该索引已经存在了
            while(hourListMap(hour).contains(index)){
              index = random.nextInt(count.toInt)
            }
            hourListMap(hour).append(index)
          }
          // 已经存在
        case Some(list) =>
          for(i <- 0 until hourExrCount){
            var index = random.nextInt(count.toInt)
            while(hourListMap(hour).contains(index)){
              index = random.nextInt(count.toInt)
            }
            hourListMap(hour).append(index)
          }
      }
    }
  }

  // 随机均匀的抽取session
  def sessionRandomExtract(sparkSession: SparkSession,
                           taskUUID: String,
                           sessionId2FilterRDD: RDD[(String, String)]): Unit = {
    // 转化key为日期
    // dateHour2FullInfoRDD: RDD[(dateHour, fullInfo)]
    val dateHour2FullInfoRDD = sessionId2FilterRDD.map{
      case (sid, fullInfo) =>
        val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
        // dateHour: yyyy-MM-dd_HH
        val dateHour = DateUtils.getDateHour(startTime)
        (dateHour, fullInfo)
    }

    // hourCountMap: Map[(dateHour, count)]
    val hourCountMap = dateHour2FullInfoRDD.countByKey()

    // dateHourCountMap: Map[(date, Map[(hour, count)])]
    val dateHourCountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()

    for((dateHour, count) <- hourCountMap){
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)

      dateHourCountMap.get(date) match{
        case None => dateHourCountMap(date) = new mutable.HashMap[String, Long]()
          dateHourCountMap(date) += (hour->count)
        case Some(map) => dateHourCountMap(date) += (hour->count)
      }
    }

    // 解决问题一： 一共有多少天： dateHourCountMap.size
    //              一天抽取多少条：100 / dateHourCountMap.size
    val extractPerDay = 100 / dateHourCountMap.size

    // 解决问题二： 一天有多少session：dateHourCountMap(date).values.sum
    // 解决问题三： 一个小时有多少session：dateHourCountMap(date)(hour)

    val dateHourExtractIndexListMap = new mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]()

    // dateHourCountMap: Map[(date, Map[(hour, count)])]
    for((date, hourCountMap) <- dateHourCountMap){
      val dateSessionCount = hourCountMap.values.sum

      dateHourExtractIndexListMap.get(date) match{
          // 还没数据
        case None => dateHourExtractIndexListMap(date) = new mutable.HashMap[String, ListBuffer[Int]]()
          generateRandomIndexList(extractPerDay, dateSessionCount, hourCountMap,  dateHourExtractIndexListMap(date))
        case Some(map) =>
          generateRandomIndexList(extractPerDay, dateSessionCount, hourCountMap,  dateHourExtractIndexListMap(date))
      }

      // 到目前为止，我们获得了每个小时要抽取的session的index

      // 广播大变量，提升任务性能
      val dateHourExtractIndexListMapBd = sparkSession.sparkContext.broadcast(dateHourExtractIndexListMap)

      // dateHour2FullInfoRDD: RDD[(dateHour, fullInfo)]
      // dateHour2GroupRDD: RDD[(dateHour, iterableFullInfo)]
      val dateHour2GroupRDD = dateHour2FullInfoRDD.groupByKey()

      // extractSessionRDD: RDD[SessionRandomExtract]
      val extractSessionRDD = dateHour2GroupRDD.flatMap{
        case (dateHour, iterableFullInfo) =>
          val date = dateHour.split("_")(0)
          val hour = dateHour.split("_")(1)

          val extractList = dateHourExtractIndexListMapBd.value.get(date).get(hour)

          val extractSessionArrayBuffer = new ArrayBuffer[SessionRandomExtract]()

          var index = 0

          for(fullInfo <- iterableFullInfo){
            if(extractList.contains(index)){
              val sessionId = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SESSION_ID)
              val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|",Constants.FIELD_START_TIME)
              val searchKeywords = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
              val clickCategories = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)

              // 封装成我们需要的对象
              val extractSession = SessionRandomExtract(taskUUID, sessionId, startTime, searchKeywords, clickCategories)

              extractSessionArrayBuffer += extractSession
            }
            index += 1
          }

          extractSessionArrayBuffer
      }

      import sparkSession.implicits._
      extractSessionRDD.toDF().write
        .format("jdbc")
        .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
        .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
        .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
        .option("dbtable", "session_extract_0308")
        .mode(SaveMode.Append)
        .save()
    }

  }

  def getFinalData(sparkSession: SparkSession,
                   taskUUID: String,
                   value: mutable.HashMap[String, Int]): Unit = {
    // 获取所有符合过滤条件的session个数
    val session_count = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble

    // 不同范围访问时长的session个数
    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    // 不同访问步长的session个数
    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    val stat = SessionAggrStat(taskUUID, session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    val statRDD = sparkSession.sparkContext.makeRDD(Array(stat))

    import sparkSession.implicits._
    statRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_ration_0308")
      .mode(SaveMode.Append)
      .save()
  }

  def calculateVisitLength(visitLength:Long, sessionStatisticAccumulator: SessionStatAccumulator): Unit ={
    if(visitLength >=1 && visitLength<=3) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    } else if (visitLength >= 4 && visitLength <= 6) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    } else if (visitLength >= 7 && visitLength <= 9) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    } else if (visitLength >= 10 && visitLength <= 30) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    } else if (visitLength > 30 && visitLength <= 60) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30s_60s)
    } else if (visitLength > 60 && visitLength <= 180) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    } else if (visitLength > 180 && visitLength <= 600) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    } else if (visitLength > 600 && visitLength <= 1800) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    } else if (visitLength > 1800) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30m)
    }

  }

  def calculateStepLength(stepLength:Long, sessionStatisticAccumulator: SessionStatAccumulator): Unit ={
    if(stepLength >=1 && stepLength <=3){
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_1_3)
    }else if (stepLength >= 4 && stepLength <= 6) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_4_6)
    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_7_9)
    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_10_30)
    } else if (stepLength > 30 && stepLength <= 60) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_30_60)
    } else if (stepLength > 60) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_60)
    }
  }

  def getFilteredData(taskParam: JSONObject,
                      sessionStatAccumulator: SessionStatAccumulator,
                      sessionId2FullInfoRDD: RDD[(String, String)])= {

    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    var filterInfo = (if(startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds else "")

    if(filterInfo.endsWith("\\|"))
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)

    val sessionId2FilterRDD = sessionId2FullInfoRDD.filter{
      case (sessionId, fullInfo) =>
        var success = true

        if(!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE))
          success = false

        if(!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS))
          success = false

        if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES))
          success = false

        if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX))
          success = false

        if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS))
          success = false

        if (!ValidUtils.in(fullInfo, Constants.FIELD_CATEGORY_ID, filterInfo, Constants.PARAM_CATEGORY_IDS))
          success = false

        if(success){

          // 只要进入此处，就代表此session数据符合过滤条件，进行总数的计数
          sessionStatAccumulator.add(Constants.SESSION_COUNT)

          val visitLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
          val stepLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong

          calculateVisitLength(visitLength, sessionStatAccumulator)
          calculateStepLength(stepLength, sessionStatAccumulator)
        }
        success
    }

    sessionId2FilterRDD
  }


  def getFullInfoData(sparkSession: SparkSession,
                      sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])]) = {
    val userId2AggrInfoRDD = sessionId2GroupRDD.map{
      case (sid, iterableAction) =>
        var startTime:Date = null
        var endTime:Date = null

        var userId = -1L

        val searchKeywords = new StringBuffer("")
        val clickCategories = new StringBuffer("")

        var stepLength = 0

        for(action <- iterableAction){
          if(userId == -1L){
            userId = action.user_id
          }

          val actionTime = DateUtils.parseTime(action.action_time)

          if(startTime == null || startTime.after(actionTime))
            startTime = actionTime

          if(endTime == null || endTime.before(actionTime))
            endTime = actionTime

          val searchKeyword = action.search_keyword
          val clickCategory = action.click_category_id

          if(StringUtils.isNotEmpty(searchKeyword) &&
            !searchKeywords.toString.contains(searchKeyword))
            searchKeywords.append(searchKeyword + ",")

          if(clickCategory != -1L &&
            !clickCategories.toString.contains(clickCategory))
            clickCategories.append(clickCategory + ",")

          stepLength += 1
        }

        val searchKw = StringUtils.trimComma(searchKeywords.toString)
        val clickCg = StringUtils.trimComma(clickCategories.toString)

        val visitLength = (endTime.getTime - startTime.getTime) / 1000

        val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sid + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)

        (userId, aggrInfo)
    }

    val sql = "select * from user_info"

    import sparkSession.implicits._
    // sparkSession.sql(sql): DateFrame DateSet[Row]
    // sparkSession.sql(sql).as[UserInfo]: DateSet[UserInfo]
    //  sparkSession.sql(sql).as[UserInfo].rdd: RDD[UserInfo]
    // sparkSession.sql(sql).as[UserInfo].rdd.map(item => (item.user_id, item)): RDD[(userId, UserInfo)]
    val userInfoRDD = sparkSession.sql(sql).as[UserInfo].rdd.map(item => (item.user_id, item))

    userId2AggrInfoRDD.join(userInfoRDD).map{
      case (userId, (aggrInfo, userInfo)) =>
        val age = userInfo.age
        val professional = userInfo.professional
        val sex = userInfo.sex
        val city = userInfo.city

        val fullInfo = aggrInfo + "|" + Constants.FIELD_AGE + "=" + age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
          Constants.FIELD_SEX + "=" + sex + "|" +
          Constants.FIELD_CITY + "=" + city

        val sessionId = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)

        (sessionId, fullInfo)
    }
  }


  def getActionRDD(sparkSession: SparkSession, taskParam: JSONObject) = {

    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val sql = "select * from user_visit_action where date>='" + startDate + "' and date<='" +
      endDate + "'"

    import sparkSession.implicits._
    // sparkSession.sql(sql) : DataFrame   DateSet[Row]
    // sparkSession.sql(sql).as[UserVisitAction]: DateSet[UserVisitAction]
    // sparkSession.sql(sql).as[UserVisitAction].rdd: rdd[UserVisitAction]
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }

}
