package cn.myfreeclouf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

public class Lower extends UDF {

    /**
     * 自定义转化为小写的一个函数
     * 方法名不能变,重写了父类的方法
     *
     * @param s
     * @return
     */
	public String evaluate (final String s) {
		
		if (s == null) {
			return null;
		}
		
		return s.toLowerCase();
	}
}
