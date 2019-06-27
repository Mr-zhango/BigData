package cn.myfreecloud.etl;


/**
 * 1.过滤脏数据
 * 2.去掉类别字段中的空格
 * 3.替换关联视频的分隔符
 * @author zhangyang
 * @date
 *
 */
public class ETLUtil {

    public static String oriString2ETLString(String line) {

        StringBuilder etlString = new StringBuilder();
        //切割数据
        String[] splits = line.split("\t");
        //数据长度不够,这个是脏数据
        if (splits.length < 9) {
            return null;
        }

        //去掉类别字段中的空格
        splits[3] = splits[3].replace(" ", "");
        //
        for (int i = 0; i < splits.length; i++) {
            //前9个数据
            if (i < 9) {
                if (i == splits.length - 1) {
                    //最后一个的时候直接添加,不要加tab
                    etlString.append(splits[i]);
                } else {
                    etlString.append(splits[i] + "\t");
                }
            } else {
                //9个之后的数据,更换分隔符
                if (i == splits.length - 1) {
                    //最后一个的时候直接添加,不要加tab
                    etlString.append(splits[i]);
                } else {
                    etlString.append(splits[i] + "&");
                }
            }
        }

        return etlString.toString();
    }
}
