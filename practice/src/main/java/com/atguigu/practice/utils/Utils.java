package com.atguigu.practice.utils;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Created by linghongbo on 2015/12/26.
 */
public class Utils {
    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);
    /**
     * 先从jar包中加载,再从classpath 中加载并覆盖
     *
     * @param filename
     * @return properties
     * @throws RuntimeException
     *             while file not exist or loading fail
     */
    @SuppressWarnings("rawtypes")
    public static Properties loadPropsByCL(String filename, Class c) {

        Properties props = new Properties();
        InputStream in = null;
        try {
            // 先加在jar包中的配置
            in = Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream(filename);

            if (in != null) {
                props.load(in);
            }
            // 再加载当前线程所在classpath的配置
            in = c.getClassLoader().getResourceAsStream(filename);
            if (in != null) {
                Properties addProps = new Properties();
                addProps.load(in);

                for (Object o : addProps.keySet()) {
                    props.put(o.toString(), addProps.getProperty(o.toString()));
                }
            }

            return props;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } finally {
            Closer.closeQuietly(in);
        }

    }

    /**
     * Get a string property, or, if no such property is defined, return the
     * given default value
     *
     * @param props
     * @param name
     * @param defaultValue
     * @return value in the props or defaultValue while name not exist
     */
    public static String getString(Properties props, String name,
                                   String defaultValue) {
        return props.containsKey(name) ? props.getProperty(name) : defaultValue;
    }

    public static String getString(Properties props, String name) {
        if (props.containsKey(name)) {
            return props.getProperty(name);
        }
        throw new IllegalArgumentException("Missing required property '" + name
                + "'");
    }

    public static int getInt(Properties props, String name) {
        if (props.containsKey(name)) {
            return getInt(props, name, -1);
        }
        throw new IllegalArgumentException("Missing required property '" + name
                + "'");
    }

    public static int getInt(Properties props, String name, int defaultValue) {
        return getIntInRange(props, name, defaultValue, Integer.MIN_VALUE,
                Integer.MAX_VALUE);
    }

    public static int getIntInRange(Properties props, String name,
                                    int defaultValue, int min, int max) {
        int v = defaultValue;
        if (props.containsKey(name)) {
            v = Integer.valueOf(props.getProperty(name));
        }
        if (v >= min && v <= max) {
            return v;
        }
        throw new IllegalArgumentException(name + " has value " + v
                + " which is not in the range");
    }

    public static boolean getBoolean(Properties props, String name,
                                     boolean defaultValue) {
        if (!props.containsKey(name))
            return defaultValue;
        return "true".equalsIgnoreCase(props.getProperty(name));
    }

    public static List<String> getList(Properties props, String name) {
        if (props.containsKey(name)) {
            List<String> keys = new ArrayList<String>();
            String names = props.getProperty(name);
            for (String str : names.split(",")) {
                keys.add(str.trim());
            }

            if (keys.size() > 0) {
                return keys;
            }
        }
        throw new IllegalArgumentException("Missing required property '" + name
                + "'");
    }

    public static Set<String> getSet(Properties props, String name) {
        if (props.containsKey(name)) {
            Set<String> set = new HashSet<>();
            String names = props.getProperty(name);
            for (String str : names.split(",")) {
                set.add(str.trim());
            }

            if (set.size() > 0) {
                return set;
            }
        }
        throw new IllegalArgumentException("Missing required property '" + name
                + "'");
    }

    public static String format2Digit(int digit) {
       // return String.format("%02d", digit);
        return ((digit < 10) ? "0" : "") + digit;
    }

    public static String format3Digit(int digit) {
        return ( (digit >= 100) ? "" : ((digit >= 10) ? "0" : "00")) + digit;
    }

    public static String format4Digit(int digit) {
        return ( (digit >= 1000) ? "" : ((digit >= 100) ? "0" : ((digit >= 10) ? "00" : "000"))) + digit;
    }

    public static String format2Digit(String str) {
        if (StringUtils.isBlank(str)) {
            throw new IllegalArgumentException("The param is not allowed to null or empty!");
        }
        return String.format("%02d", str);
    }



    public static boolean isLngLat(String key) {
        return  key.matches("\\d+(\\.*)(\\d*),\\d+(\\.*)(\\d*)");

    }

    public static boolean isAllDigits(String key) {
        return key.matches("\\d+");
    }

    public static boolean isCityId(String key) {
        if(!isAllDigits(key)) {
            return false;
        }
        int number;
        try {
            number = Integer.parseInt(key);
        } catch(NumberFormatException nfe) {
            return false;
        }

        return number >= 0 && number <= 500;

    }

    public static boolean isCityIdLngLat(String key) {
        return key.matches("\\d+_\\d+(\\.*)(\\d*),\\d+(\\.*)(\\d*)");
    }

    public static boolean isCounter(String metric) {
        return metric.startsWith(Const.CNT_) || metric.startsWith(Const.AMT_) || metric.startsWith(Const.DIST_);
    }



}
