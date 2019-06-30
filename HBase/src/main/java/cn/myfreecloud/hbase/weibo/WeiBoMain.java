package cn.myfreecloud.hbase.weibo;

/**
 * @author: zhangyang
 * @date: 2019/6/29 23:48
 * @description:
 */
public class WeiBoMain {


    public static void init() throws Exception {

        //创建相关命名空间&表
        WeiBoUtil.createNamespace(Constant.NAMESPACE);

        //创建内容表
        WeiBoUtil.createTable(Constant.CONTENT,1,"info");

        //创建用户关系表
        WeiBoUtil.createTable(Constant.RELATIONS,1,"attends","fans");

        //创建收件箱表(多版本)
        WeiBoUtil.createTable(Constant.INBOX,2,"info");

    }


    public static void main(String[] args) throws Exception {
        //init();方法创建表
      //init();

        //发布微博
        //1001和1002发布微博
//        WeiBoUtil.createData("1001","我是1001,这是没有人关注我的时候我发布的微博1001");
//        WeiBoUtil.createData("1002","我是1002,这是没有人关注我的时候我发布的微博1002");

        //1001关注1002和1003

        //WeiBoUtil.addAttend("1001","1002","1003");

        //获取1001初始化页面信息(查询1002的收件箱信息)
//        WeiBoUtil.getInit("1001");
//
//        //1003发布微博
//        WeiBoUtil.createData("1003","我是1003,这是1001已经关注我的时候我发的这一个2333微博1003");
//
//        System.out.println("-------------------------");
        //获取1001初始化页面信息
        WeiBoUtil.getInit("1001");

        //取消关注
        WeiBoUtil.delAttend("1001","1002");

        WeiBoUtil.getInit("1001");
    }
}
