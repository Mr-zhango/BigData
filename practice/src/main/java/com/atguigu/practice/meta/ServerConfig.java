package com.atguigu.practice.meta;



import com.atguigu.practice.utils.Utils;

import java.util.Properties;
import java.util.Set;

/**
 * Created by linghongbo on 2015/12/26.
 */
public class ServerConfig {

    private Properties props;

    public ServerConfig(Properties props) {
        this.props = props;
    }

    public int getJedisPoolMaxSize() {
        return Utils.getInt(props, "jedis.pool.max.size");
    }

    public int getJedisMaxWaitMills() {
        return Utils.getInt(props, "jedis.max.wait.millis");
    }

    public int getJedisSocketTimeout() {
        return Utils.getInt(props, "jedis.socket.timeout");
    }

    public String getQdbIp1() {
        return Utils.getString(props, "qdb.ip1");
    }

    public int getQdbPort1() {
        return Utils.getInt(props, "qdb.port1");
    }

    public String getQdbIp2() {
        return Utils.getString(props, "qdb.ip2");
    }

    public int getQdbPort2() {
        return Utils.getInt(props, "qdb.port2");
    }

    public String getQdbIp3() {
        return Utils.getString(props, "qdb.ip3");
    }

    public int getQdbPort3() {
        return Utils.getInt(props, "qdb.port3");
    }

    public int getExpireSec() {
        return Utils.getInt(props, "redis.key.expire");
    }

    public String getHBaseGridTableName() {
        return Utils.getString(props, "hbase.grid.table.name");
    }

    public String getHBaseCityGridTableName() {
        return Utils.getString(props, "hbase.city.grid.table.name");
    }

    public String getCallerMgmtUrl() {
        return Utils.getString(props, "caller.mgmt.url");
    }

    public String getCallerMgmtAllUrl() {
        return Utils.getString(props, "caller.mgmt.all.url");
    }

    public Set<String> getSupportedPassengerMetrics() {
        return Utils.getSet(props, "supported.passenger.metrics");
    }

    public Set<String> getSupportedGsDriverMetrics() {
        return Utils.getSet(props, "supported.gs_driver.metrics");
    }

    public Set<String> getSupportedTaxiDriverMetrics() {
        return Utils.getSet(props, "supported.taxi_driver.metrics");
    }

    public Set<String> getSupportedGridMetrics() {
        return Utils.getSet(props, "supported.grid.metrics");
    }

    public String getApplicationName() {
        return Utils.getString(props, "application.name");
    }

    public String getKafkaBootstrapServers() {
        return Utils.getString(props, "kafka.producer.bootstrap.servers");
    }

}
