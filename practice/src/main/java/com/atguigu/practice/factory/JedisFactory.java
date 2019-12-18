package com.atguigu.practice.factory;

import com.atguigu.practice.meta.ConfManager;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisFactory {
    private JedisPool[] jedisPools;

    public JedisFactory() {
        jedisPools = new JedisPool[3];

        int maxSize = ConfManager.serverConfig.getJedisPoolMaxSize();
        int waitMillis = ConfManager.serverConfig.getJedisMaxWaitMills();

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(maxSize);
        config.setMaxWaitMillis(waitMillis);

        String qdb1IP = ConfManager.serverConfig.getQdbIp1();
        int qdb1Port = ConfManager.serverConfig.getQdbPort1();
        int socketTimeout = ConfManager.serverConfig.getJedisSocketTimeout();

        jedisPools[0] = new JedisPool(config, qdb1IP, qdb1Port, socketTimeout);

        String qdb2IP = ConfManager.serverConfig.getQdbIp2();
        int qdb2Port = ConfManager.serverConfig.getQdbPort2();
        jedisPools[1] = new JedisPool(config, qdb2IP, qdb2Port, socketTimeout);

        String qdb3IP = ConfManager.serverConfig.getQdbIp3();
        int qdb3Port = ConfManager.serverConfig.getQdbPort3();
        jedisPools[2] = new JedisPool(config, qdb3IP, qdb3Port, socketTimeout);

    }

    private static class SingletonHelper {
        private static final JedisFactory INSTANCE = new JedisFactory();
    }
    public static JedisFactory getInstance() {
        return SingletonHelper.INSTANCE;
    }

    public JedisPool getJedisPool(int i) {
        if(i < 0 || i >= 3) {
            return null;
        }
        return jedisPools[i];
    }

}
