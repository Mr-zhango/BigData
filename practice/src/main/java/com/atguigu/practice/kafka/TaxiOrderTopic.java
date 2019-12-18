package com.atguigu.practice.kafka;

import com.atguigu.practice.model.ErrorCode;
import org.boon.json.JsonFactory;
import org.boon.json.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TaxiOrderTopic {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaxiOrderTopic.class);

    private String db;

    private String table;

    private String passengerId;

    private String driverId;

    private long timestamp = 0;

    private EventType eventType;

    private int startDestDistance = 0;

    private String startingLng;

    private String startingLat;

    private int area;

    private int statusValue;

    private TaxiOrderTopicFields.TaxiOrderStatus orderStatus;

    private Boolean isUpdatedOrderStatus = false;

    private final static DateTimeFormatter FORMATTER_T = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");

    private final static DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

    public TaxiOrderTopic(String message) {

        ObjectMapper mapper = JsonFactory.create();

        Map taxiOrderMap = mapper.readValue(message, Map.class);

        /**
         * Get kafka message timestamp
         */
        String time = (String) taxiOrderMap.get(TaxiOrderTopicFields.TIMESTAMP.getCode());
        if(time != null) {

            try {
                DateTime dateTime = FORMATTER_T.parseDateTime(time);
                timestamp = dateTime.getMillis();
            } catch (Exception e) {
                LOGGER.error(ErrorCode.ERRORCODE_FAIL_TO_PARSE_ARGUMENTS.getMsg(), e);
            }
        }

        /**
         *  Get database
         *  */
        db = (String) taxiOrderMap.get(TaxiOrderTopicFields.DB.getCode());

        /**
         *  Get table
         *  */
        table = (String) taxiOrderMap.get(TaxiOrderTopicFields.TABLE.getCode());

        /**
         * Get event type: insert or update
         * */
        eventType = EventType.getEventType((String) taxiOrderMap.get(TaxiOrderTopicFields.OPTYPE.getCode()));

        /**
         * Get passengerId
         */
        passengerId = (String) taxiOrderMap.get(TaxiOrderTopicFields.PASSENGER_ID.getCode());

        /**
         * Get driverId
         */
        driverId = (String) taxiOrderMap.get(TaxiOrderTopicFields.DRIVER_ID.getCode());

        /**
         * Get last modified order status. If order status is not modified in the event, then there is no such field f_order_status.
         */
        String f_order_status = (String) taxiOrderMap.get(TaxiOrderTopicFields.F_ORDER_STATUS.getCode());

        /**
         * Get current modified order status. If order status is not modified in the event, then there is no such field t_order_status.
         */
        String t_order_status = (String) taxiOrderMap.get(TaxiOrderTopicFields.T_ORDER_STATUS.getCode());

        LOGGER.error("f_status:" + f_order_status + " t_status:" + t_order_status + " equals:" + (f_order_status.equals(t_order_status)));
        if(f_order_status != null && t_order_status != null && !f_order_status.equals(t_order_status)) {
            isUpdatedOrderStatus = true;
        }

        /**
         * Get order status
         */
        String orderStatusStr = (String) taxiOrderMap.get(TaxiOrderTopicFields.ORDER_STATUS.getCode());

        /**
         * Get start_dest_distance
         */
        String startDestDistanceStr = (String) taxiOrderMap.get(TaxiOrderTopicFields.START_DEST_DISTANCE.getCode());

        /**
         * Get starting_lng, starting_lat
         */
        startingLng = (String) taxiOrderMap.get(TaxiOrderTopicFields.STARTING_LNG.getCode());

        startingLat = (String) taxiOrderMap.get(TaxiOrderTopicFields.STARTING_LAT.getCode());

        /**
         * Get area
         */
        String areaStr = (String) taxiOrderMap.get(TaxiOrderTopicFields.AREA.getCode());

        try{
            statusValue = Integer.parseInt(orderStatusStr);
            orderStatus = TaxiOrderTopicFields.TaxiOrderStatus.getTaxiOrderStatus(statusValue);
            startDestDistance = Integer.parseInt(startDestDistanceStr);
            area = Integer.parseInt(areaStr);

        } catch(NumberFormatException nfe){
            LOGGER.error(ErrorCode.ERRORCODE_FAIL_TO_CAST_STRING_TO_INT.getMsg(), nfe);
        }

    }

    public String getPassengerId(){
        return passengerId;
    }

    public String getDriverId() {
        return driverId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public EventType getEventType(){
        return eventType;
    }

    public TaxiOrderTopicFields.TaxiOrderStatus getOrderStatus(){
        return orderStatus;
    }

    public Boolean getIsUpdatedOrderStatus(){
        return isUpdatedOrderStatus;
    }

    public String getDb() { return db; }

    public String getTable() { return table; }


    public int getStartDestDistance() {
        return startDestDistance;
    }

    public String getStartingLng() {
        return startingLng;
    }

    public String getStartingLat() {
        return startingLat;
    }

    public int getArea() {
        return area;
    }

    public int getStatusValue() {
        return statusValue;
    }
}
