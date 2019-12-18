package com.atguigu.practice.factory;


import com.atguigu.practice.model.OperatorEnum;
import com.atguigu.practice.store.Operator;
import com.atguigu.practice.store.QDBOperator;

/**
 * Created by linghongbo on 2016/3/23.
 */
public class OperatorFactory {

    public static Operator getOperator(String name) {

        Operator operator = null;
        OperatorEnum oper = OperatorEnum.getType(name);

        switch (oper) {
            case QDB:
                operator = new QDBOperator();
                break;
            case HBASE_GRID:
                //operator = new HBaseGridOperator();
                break;
            case HBASE_CITY_GRID:
                //operator = new HBaseCityGridOperator();
                break;
            case CASSANDRA:
                //TODO new CassandraOperator
                operator = null;
                break;
        }
        return operator;
    }
}
