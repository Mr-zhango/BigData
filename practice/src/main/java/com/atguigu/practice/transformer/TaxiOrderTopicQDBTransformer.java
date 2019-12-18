package com.atguigu.practice.transformer;

import com.atguigu.practice.kafka.EventType;
import com.atguigu.practice.kafka.KafkaTopic;
import com.atguigu.practice.kafka.TaxiOrderTopic;
import com.atguigu.practice.kafka.TaxiOrderTopicFields;
import com.atguigu.practice.model.Event;
import com.atguigu.practice.model.Type;
import com.atguigu.practice.utils.Const;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TaxiOrderTopicQDBTransformer implements  Transformer{

    private static final Logger LOGGER = LoggerFactory.getLogger(TaxiOrderTopicQDBTransformer.class);

    private static final String product = Const.TAXI;

    private static final int productId = 11;

    private static final String TAXI_ORDER_TOPIC = KafkaTopic.TAXI_ORDER.getCode();


    @Override
    public List<Event> transform(String message) {
        List<Event> events = new ArrayList<>();

        LOGGER.info(TaxiOrderTopicQDBTransformer.class + ": Transforming message");
        TaxiOrderTopic topic = new TaxiOrderTopic(message);
        String passengerId = topic.getPassengerId();
        String driverId = topic.getDriverId();
        long timestamp = topic.getTimestamp();
        TaxiOrderTopicFields.TaxiOrderStatus orderStatus = topic.getOrderStatus();
        Boolean isUpdatedOrderStatus = topic.getIsUpdatedOrderStatus();
        String db = topic.getDb();
        String table = topic.getTable();
        EventType eventType = topic.getEventType();
        int area =  topic.getArea();
        int startDestDistance = topic.getStartDestDistance();

        LOGGER.error("db:" + db + " table:" + table  + " eventType:" + eventType
                + " isupdate:" + isUpdatedOrderStatus + " orderStatus:" + orderStatus);
        if("app_dididache".equals(db) && "Order".equals(table)
                && StringUtils.isNotBlank(passengerId) && timestamp > 0) {

            Map<String, Number> counterMetrics = new HashMap<>();
            Map<String, Number> statusMetrics = new HashMap<>();


            /**
             * 完成事件
             */
            if(EventType.UPDATE.equals(eventType) && isUpdatedOrderStatus && TaxiOrderTopicFields.TaxiOrderStatus.PAY_SUCCESS.equals(orderStatus)) {

                /**
                 * Counter metrics
                 */

                counterMetrics.put(Const.CNT_FINISH + Const.UNDERSCORE + product, 1);
                //counterMetrics.put(Const.DIST_FINISH + Const.UNDERSCORE + product, (double)startDestDistance/1000);

                events.add(new Event(TAXI_ORDER_TOPIC, passengerId, timestamp, counterMetrics, true, Type.PASSENGER));
                /**全国总量*/
                events.add(new Event(TAXI_ORDER_TOPIC, "0", timestamp, counterMetrics, true, Type.PASSENGER));

                if(StringUtils.isNotBlank(driverId)){
                    events.add(new Event(TAXI_ORDER_TOPIC, driverId, timestamp, counterMetrics,true, Type.TAXI_DRIVER));
                }

                /**
                 * Status metrics
                 */
                statusMetrics.put(Const.LAST_FINISH_PRODUCT, productId);
                statusMetrics.put(Const.LAST_FINISH_AREA, area);
                statusMetrics.put(Const.LAST_FINISH + Const.UNDERSCORE + product + "_time", timestamp);
                events.add(new Event(TAXI_ORDER_TOPIC, passengerId, timestamp, statusMetrics, false, Type.PASSENGER));
            }
            /**
             * 抢单事件
             */
            else if (EventType.UPDATE.equals(eventType) && isUpdatedOrderStatus && TaxiOrderTopicFields.TaxiOrderStatus.STRIVED.equals(orderStatus) && StringUtils.isNotBlank(driverId)) {
                /**
                 *  metricName = metric_taxi. For example, cnt_finish_share_fast
                 *  increment value: 1
                 *  */

                counterMetrics.put(Const.CNT_STRIVE + Const.UNDERSCORE + product, 1);

                events.add(new Event(TAXI_ORDER_TOPIC, driverId, timestamp, counterMetrics, true, Type.TAXI_DRIVER));

            }
            /**
             * 发单事件
             */
            else if (EventType.INSERT.equals(eventType) && TaxiOrderTopicFields.TaxiOrderStatus.NOT_STRIVED.equals(orderStatus)) {

                counterMetrics.put(Const.CNT_CALL + Const.UNDERSCORE + product, 1);

                events.add(new Event(TAXI_ORDER_TOPIC, passengerId, timestamp, counterMetrics, true, Type.PASSENGER));
                /**全国总量*/
                events.add(new Event(TAXI_ORDER_TOPIC, "0", timestamp, counterMetrics, true, Type.PASSENGER));

            }
        }

        /**测试用*/
        if("1691469357516".equals(passengerId)) {
            //LOGGER.error("Taxi_events=" + events.toString());
            LOGGER.error("message=" + message);
        }
        LOGGER.error("events info:" + events);
        return events;
    }
}

