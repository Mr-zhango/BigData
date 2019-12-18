package com.atguigu.practice.kafka;

import java.util.HashMap;
import java.util.Map;

public enum TaxiOrderTopicFields {
    TIMESTAMP("timestamp"),

    DB("db"),

    TABLE("table"),

    OPTYPE("optype"),

    ORDER_ID("orderId"),

    DRIVER_ID("driverId"),

    PASSENGER_ID("passengerId"),

    F_ORDER_STATUS("f_status"),

    T_ORDER_STATUS("t_status"),

    ORDER_STATUS("status"),

    START_DEST_DISTANCE("length"),

    STARTING_LNG("lng"),

    STARTING_LAT("lat"),

    AREA("area");

    private String code;

    TaxiOrderTopicFields(String code) {

        this.code = code;
    }

    Map<Integer, Integer> map = new HashMap<>();

    public String getCode() {
        return code;
    }

    public enum TaxiOrderStatus{
        NOT_STRIVED(0),

        STRIVED(1),

        DRIVER_ARRIVED_DEPARTURE(2),

        PASSENGER_ON_BOARD(3),

        DRIVER_CANCEL_AFTER_STRIVE(4),

        WAITING(5),

        PASSENGER_CANCEL_BEFORE_STRIVE(7),

        DELETED_ORDER(8),

        PASSENGER_CANCEL_AFTER_STRIVE(9),

        PAY_SUCCESS(11);

        int code;
        TaxiOrderStatus(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public static TaxiOrderStatus getTaxiOrderStatus (int i) {
            TaxiOrderStatus orderStatus = null;
            switch(i) {
                case 0:
                    orderStatus = NOT_STRIVED;
                    break;
                case 1:
                    orderStatus = STRIVED;
                    break;
                case 2:
                    orderStatus = DRIVER_ARRIVED_DEPARTURE;
                    break;
                case 3:
                    orderStatus = PASSENGER_ON_BOARD;
                    break;
                case 4:
                    orderStatus = DRIVER_CANCEL_AFTER_STRIVE;
                    break;
                case 5:
                    orderStatus = WAITING;
                    break;
                case 7:
                    orderStatus = PASSENGER_CANCEL_BEFORE_STRIVE;
                    break;
                case 8:
                    orderStatus = DELETED_ORDER;
                    break;
                case 9:
                    orderStatus = PASSENGER_CANCEL_AFTER_STRIVE;
                    break;
                case 11:
                    orderStatus = PAY_SUCCESS;
                    break;
            }
            return orderStatus;
        }

/*
        0：未应答
        1：已应答
        2：已到达
        3：已接到
        4：司机接单情况下司机取消订单
        5：等待中
        7：没有司机接单情况下乘客取消订单
        8：删除订单(隐藏处于终结状态的订单
               9：司机接单情况下乘客取消订单
                       11：完成微信支付或者完成评论*/
    }


}


/**
 * {"timestamp":"2016-08-12T14:14:43",
 * "db":"app_dididache",
 * "table":"Order",
 * "optype":"i",
 * "table_filter":"Order",
 * "orderId":"2710355954","trip_id":"0",
 * "passengerId":"114781218","token":"0",
 * "driverId":"",
 * "status":"0",
 * "type":"0",
 * "lng":"118.651327",
 * "lat":"37.452796",
 * "address":"∂´”™«¯. §¿˚Ω÷µ¿.∂´∂˛¬∑|Ωıª™–°«¯-∂´√≈",
 * "destination":"∂´”™«¯±±∂˛¬∑|–¬¿À≥±≤Àπ›",
 * "setuptime":"2016-08-12 14:14:43",
 * "tip":"0","exp":"2","waittime":"0","callCount":"1",
 * "distance":"0","length":"9158",
 * "verifyMessage":"","createTime":"2016-08-12 14:14:43",
 * "striveTime":"0000-00-00 00:00:00","arriveTime":"","getofftime":"0000-00-00 00:00:00","aboardTime":"","cancelTime":"","pCommentGread":"1","pCommentText":"","pCommentTime":"","pCommentStatus":"0","dCommentGread":"1","dCommentText":"","dCommentTime":"","dCommentStatus":"0","channel":"102","area":"73","version":"3","remark":"","bonus":"0","voicelength":"0","voiceTime":"0","extra_info":"","pay_info":"{\"dynamic\":[{\"type\":0,\"price\":0}],\"estimate\":{\"ekey\":false},\"optstg\":{\"1km\":0,\"car\":0}}","destlng":"118.552187","destlat":"37.476817","srclng":"118.651327",
 * "order_terminate_pid":"0","srclat":"37.452796","kuaidi_oid":"0","thirdoid":""}
 */


/*
{"timestamp":"2016-08-12T14:14:43","db":"app_dididache","table":"Order",
"optype":"u","table_filter":"Order",
"orderId":"2710318904","trip_id":"0","passengerId":"1732878142902",
"token":"0","driverId":"5366117",
"f_status":"2","t_status":"3",
"status":"3","type":"0",
"lng":"113.911834",
"lat":"35.296675","address":"∫Ï∆Ï«¯’≈◊Øƒœ±±Ω÷”Î–¬—”¬∑Ωª≤Êø⁄Œ˜ƒœ150√◊|∫”ƒœ °√∫ÃÔµÿ÷ æ÷»˝∂”",
"destination":"∫Õ∆Ω¥Ûµ¿—ÿœﬂ|≥«πÿ–¬¥Â","setuptime":"2016-08-12 14:07:50",
"tip":"0","exp":"2","waittime":"0","callCount":"1",
"distance":"1427","length":"1398","verifyMessage":"",
"createTime":"2016-08-12 14:07:50",
"striveTime":"2016-08-12 14:08:04",
"f_arriveTime":"",
"t_arriveTime":"2016-08-12 14:14:43",
"arriveTime":"2016-08-12 14:14:43","getofftime":"0000-00-00 00:00:00",
"aboardTime":"2016-08-12 14:14:22","cancelTime":"","pCommentGread":"1","pCommentText":"STG_SENDINFO_2710318904_5366117_1",
"pCommentTime":"","pCommentStatus":"0","dCommentGread":"1","dCommentText":"","dCommentTime":"","dCommentStatus":"0","channel":"1200",
"area":"111","version":"3","remark":"","bonus":"0","voicelength":"0","voiceTime":"0","extra_info":"",
"pay_info":"{\"dynamic\":[{\"type\":0,\"price\":0}],\"estimate\":{\"ekey\":false},\"optstg\":{\"1km\":0,\"car\":0}}",
"destlng":"113.897527","destlat":"35.292036",
"srclng":"113.911834","order_terminate_pid":"0","srclat":"35.296675","kuaidi_oid":"0","thirdoid":""}
 */