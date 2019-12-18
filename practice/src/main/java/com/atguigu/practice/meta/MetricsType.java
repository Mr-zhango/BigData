package com.atguigu.practice.meta;

public enum MetricsType {
  // 累计指标，如订单数
  COUNTER,
  // 状态指标，如乘客的最后订单完成时间
  STATUS,

  /**
   * 订单指标
   */
  ORDER,

  // 日，志指标，如最近5单
  @Deprecated
  LOG,
  // 状态指标，带过期时间，两周过期，如订单状态
  @Deprecated
  STATUS_EXPIRE,
  // set hash表中的数据,如果value有值则set,没有值则del
  HASH_SET,
  // 集合数据新增,用于真身服务中的openId->pid集合
  SET_ADD,
  // 集合数据删除,用于真身服务更新openId->pid集合
  SET_DEL,

  RANGE_MMM,

  RANGE_COUNT;

  public static boolean isNotNeedAllDigits(MetricsType type) {
    if(type == null) {
      return false;
    }
    if(type.equals(HASH_SET) || type.equals(SET_ADD) || type.equals(SET_DEL)) {
      return true;
    }
    return false;
  }
}
