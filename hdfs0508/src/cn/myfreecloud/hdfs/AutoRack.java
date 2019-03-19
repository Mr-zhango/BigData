package cn.myfreecloud.hdfs;

import org.apache.hadoop.net.DNSToSwitchMapping;

import java.util.ArrayList;
import java.util.List;

/**
 * hadoop的自动感知
 */
public class AutoRack implements DNSToSwitchMapping {


    @Override
    public List<String> resolve(List<String> ips) {

        // ips 主机名称
        // 输出是所有的机架


        // 0 准备一个返回机架的集合
        ArrayList<String> lists = new ArrayList<String>();


        int ipnumber = 0;

        if (ips != null && ips.size() > 0) {

            // 1.获取机架的ip


            //拿出所有的ip
            for (String ip : ips) {

                //hadoop102
                if (ip.startsWith("hadoop")) {
                    String ipnum = ip.substring(6);
                    ipnumber = Integer.parseInt(ipnum);

                    // 192.168.1.100
                } else if (ip.startsWith("192")) {
                    int index = ip.lastIndexOf(".");
                    String ipnumip = ip.substring(index + 1);

                    ipnumber = Integer.parseInt(ipnumip);
                }

                // 2.自定义机架感知(把102.103机架定义为机架1,吧104.105机架定义为机架2)

                if (ipnumber < 104) {
                    //定义为机架一
                    lists.add("/rack1/" + ipnumber);

                } else {
                    //定义为机架二
                    lists.add("/rack2/" + ipnumber);
                }
            }

        }

        // 3.返回处理好的list
        return lists;
    }

    @Override
    public void reloadCachedMappings() {

    }

    @Override
    public void reloadCachedMappings(List<String> list) {

    }
}
