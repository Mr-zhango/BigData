package com.z.transformer.dimension.value;

import org.apache.hadoop.io.Writable;

import com.z.transformer.common.KpiType;

/**
 * 自定义顶级的输出value父类
 * 
 * @author Jinji
 *
 */
public abstract class BaseStatsValueWritable implements Writable {
    /**
     * 获取当前value对应的kpi值， 由kpi来确定具体数据的输出方式
     * 
     * @return
     */
    public abstract KpiType getKpi();
}
