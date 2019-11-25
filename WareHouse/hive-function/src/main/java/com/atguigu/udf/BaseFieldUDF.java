package com.atguigu.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

public class BaseFieldUDF extends UDF {

    /**
     * @Author: zhangyang
     * @description: 重写UDF方法 , 该函数负责处理公共字段,UDTF负责处理业务字段
     * @Param:
     * @return:
     * @time: 2019/11/20 14:10
     */
    public String evaluate(String line, String jsonkeysString) {


        // line 整个json数据

        // 0 准备一个sb
        StringBuilder sb = new StringBuilder();

        // 1 切割jsonkeys  mid uid vc vn l sr os ar md
        String[] jsonkeys = jsonkeysString.split(",");

        // 2 处理line   服务器时间 | json
        String[] logContents = line.split("\\|");

        // 3 数据合法性校验 服务器时间+json                   json
        if (logContents.length != 2 || StringUtils.isBlank(logContents[1])) {
            return "";
        }

        // 4 开始处理json
        try {
            JSONObject jsonObject = new JSONObject(logContents[1]);

            // 5.获取cm JSON对象 里面的对象
            JSONObject base = jsonObject.getJSONObject("cm");

            // 6.循环遍历取值
            for (int i = 0; i < jsonkeys.length; i++) {
                String jsonKey = jsonkeys[i].trim();

                if (base.has(jsonKey)) {
                    sb.append(base.getString(jsonKey)).append("\t");
                } else {
                    sb.append("\t");
                }
            }

            // 拼接事件字段
            sb.append(jsonObject.getString("et")).append("\t");

            //
            sb.append(logContents[0]).append("\t");
        } catch (JSONException e) {
            e.printStackTrace();
        }

        return sb.toString();
    }

    /**
     * @Author: zhangyang
     * @description: 测试UDF函数
     * @Param:
     * @return:
     * @time: 2019/11/20 14:09
     */
    public static void main(String[] args) {

        String line = "1541217850324|" +
                "{\n" +
                "    \"cm\":{\n" +
                "        \"mid\":\"m7856\",\n" +
                "        \"uid\":\"u8739\",\n" +
                "        \"ln\":\"-74.8\",\n" +
                "        \"sv\":\"V2.2.2\",\n" +
                "        \"os\":\"8.1.3\",\n" +
                "        \"g\":\"P7XC9126@gmail.com\",\n" +
                "        \"nw\":\"3G\",\n" +
                "        \"l\":\"es\",\n" +
                "        \"vc\":\"6\",\n" +
                "        \"hw\":\"640*960\",\n" +
                "        \"ar\":\"MX\",\n" +
                "        \"t\":\"1541204134250\",\n" +
                "        \"la\":\"-31.7\",\n" +
                "        \"md\":\"huawei-17\",\n" +
                "        \"vn\":\"1.1.2\",\n" +
                "        \"sr\":\"O\",\n" +
                "        \"ba\":\"Huawei\"\n" +
                "    },\n" +
                "    \"ap\":\"weather\",\n" +
                "    \"et\":[\n" +
                "        {\n" +
                "            \"ett\":\"1541146624055\",\n" +
                "            \"en\":\"display\",\n" +
                "            \"kv\":{\n" +
                "                \"goodsid\":\"n4195\",\n" +
                "                \"copyright\":\"ESPN\",\n" +
                "                \"content_provider\":\"CNN\",\n" +
                "                \"extend2\":\"5\",\n" +
                "                \"action\":\"2\",\n" +
                "                \"extend1\":\"2\",\n" +
                "                \"place\":\"3\",\n" +
                "                \"showtype\":\"2\",\n" +
                "                \"category\":\"72\",\n" +
                "                \"newstype\":\"5\"\n" +
                "            }\n" +
                "        },\n" +
                "        {\n" +
                "            \"ett\":\"1541213331817\",\n" +
                "            \"en\":\"loading\",\n" +
                "            \"kv\":{\n" +
                "                \"extend2\":\"\",\n" +
                "                \"loading_time\":\"15\",\n" +
                "                \"action\":\"3\",\n" +
                "                \"extend1\":\"\",\n" +
                "                \"type1\":\"\",\n" +
                "                \"type\":\"3\",\n" +
                "                \"loading_way\":\"1\"\n" +
                "            }\n" +
                "        },\n" +
                "        {\n" +
                "            \"ett\":\"1541126195645\",\n" +
                "            \"en\":\"ad\",\n" +
                "            \"kv\":{\n" +
                "                \"entry\":\"3\",\n" +
                "                \"show_style\":\"0\",\n" +
                "                \"action\":\"2\",\n" +
                "                \"detail\":\"325\",\n" +
                "                \"source\":\"4\",\n" +
                "                \"behavior\":\"2\",\n" +
                "                \"content\":\"1\",\n" +
                "                \"newstype\":\"5\"\n" +
                "            }\n" +
                "        },\n" +
                "        {\n" +
                "            \"ett\":\"1541202678812\",\n" +
                "            \"en\":\"notification\",\n" +
                "            \"kv\":{\n" +
                "                \"ap_time\":\"1541184614380\",\n" +
                "                \"action\":\"3\",\n" +
                "                \"type\":\"4\",\n" +
                "                \"content\":\"\"\n" +
                "            }\n" +
                "        },\n" +
                "        {\n" +
                "            \"ett\":\"1541194686688\",\n" +
                "            \"en\":\"active_background\",\n" +
                "            \"kv\":{\n" +
                "                \"active_source\":\"3\"\n" +
                "            }\n" +
                "        }\n" +
                "    ]\n" +
                "}";

        String lineOringinal = "1541217850324|{\"cm\":{\"mid\":\"m7856\",\"uid\":\"u8739\",\"ln\":\"-74.8\",\"sv\":\"V2.2.2\",\"os\":\"8.1.3\",\"g\":\"P7XC9126@gmail.com\",\"nw\":\"3G\",\"l\":\"es\",\"vc\":\"6\",\"hw\":\"640*960\",\"ar\":\"MX\",\"t\":\"1541204134250\",\"la\":\"-31.7\",\"md\":\"huawei-17\",\"vn\":\"1.1.2\",\"sr\":\"O\",\"ba\":\"Huawei\"},\"ap\":\"weather\",\"et\":[{\"ett\":\"1541146624055\",\"en\":\"display\",\"kv\":{\"goodsid\":\"n4195\",\"copyright\":\"ESPN\",\"content_provider\":\"CNN\",\"extend2\":\"5\",\"action\":\"2\",\"extend1\":\"2\",\"place\":\"3\",\"showtype\":\"2\",\"category\":\"72\",\"newstype\":\"5\"}},{\"ett\":\"1541213331817\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"15\",\"action\":\"3\",\"extend1\":\"\",\"type1\":\"\",\"type\":\"3\",\"loading_way\":\"1\"}},{\"ett\":\"1541126195645\",\"en\":\"ad\",\"kv\":{\"entry\":\"3\",\"show_style\":\"0\",\"action\":\"2\",\"detail\":\"325\",\"source\":\"4\",\"behavior\":\"2\",\"content\":\"1\",\"newstype\":\"5\"}},{\"ett\":\"1541202678812\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1541184614380\",\"action\":\"3\",\"type\":\"4\",\"content\":\"\"}},{\"ett\":\"1541194686688\",\"en\":\"active_background\",\"kv\":{\"active_source\":\"3\"}}]}";

        String x = new BaseFieldUDF().evaluate(line, "mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t");
        System.out.println(x);
    }
}
