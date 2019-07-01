package cn.myfreecloud.storm.weblog;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

/**
 * @author: zhangyang
 * @date: 2019/7/1 11:43
 * @description:
 */
public class GenerateData {
    public static void main(String[] args) {

        //准备文件
        File logFile = new File("e:/website.log");


        // 1 网站名称
        String[] hosts = { "www.myfreecloud.cn" };
        // 2 会话id
        String[] session_id = { "ABYH6Y4V4SCVXTG6DPB4VH9U123", "XXYH6YCGFJYERTT834R52FDXV9U34",
                "BBYH61456FGHHJ7JL89RG5VV9UYU7", "CYYH6Y2345GHI899OFG4V9U567", "VVVYH6Y4V4SFXZ56JIPDPB4V678" };
        // 3 访问网站时间
        String[] time = { "2017-08-07 08:40:50", "2017-08-07 08:40:51", "2017-08-07 08:40:52", "2017-08-07 08:40:53",
                "2017-08-07 09:40:49", "2017-08-07 10:40:49", "2017-08-07 11:40:49", "2017-08-07 12:40:49" };

        // 4 拼接网站访问日志
        StringBuffer sbBuffer = new StringBuffer();

        Random random = new Random();

        for (int i = 0; i < 200; i++) {
            sbBuffer.append(hosts[0] + "\t" + session_id[random.nextInt(5)] + "\t" + time[random.nextInt(8)] + "\n");
        }

        // 5 判断log日志是否存在，不存在要创建
        if (!logFile.exists()) {
            try {
                logFile.createNewFile();
            } catch (IOException e) {
                System.out.println("Create logFile fail !");
            }
        }
        byte[] b = (sbBuffer.toString()).getBytes();

        // 6 将拼接的日志信息写到日志文件中
        FileOutputStream fs = null;
        try {
            fs = new FileOutputStream(logFile);
            fs.write(b);
            fs.close();
            System.out.println("generate data over");
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            //关闭资源
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
