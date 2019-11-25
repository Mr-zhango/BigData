package com.z.transformer.converter;

import java.io.Closeable;
import java.io.IOException;

import com.z.transformer.dimension.key.BaseDimension;

/**
 * 提供专门操作dimension表的接口
 * 根据dimension表的维度值获取维度id
 */
public interface IDimensionConverter extends Closeable{
	/**
	 * 根据dimension的value值获取id
	 * 如果数据库中有，那么直接返回。如果没有，那么进行插入后返回新的id值
	 * 
	 * @param dimension
	 * @return
	 * @throws IOException
	 */
	public int getDimensionIdByValue(BaseDimension dimension) throws IOException;
}
