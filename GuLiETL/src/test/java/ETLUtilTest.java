import cn.myfreecloud.etl.ETLUtil;

/**
 * @author: zhangyang
 * @date: 2019/6/21 18:33
 * @description:
 */
public class ETLUtilTest {
    public static void main(String[] args) {

        String string = ETLUtil.oriString2ETLString("LKh7zAJ4nwo\tTheReceptionist\t653\tEntertainment\t424\t13021\t4.34\t1305\t744\tDjdA-5oKYFQ\tNxTDlnOuybo\tc-8VuICzXtU\tDH56yrIO5nI\tW1Uo5DQTtzc\tE-3zXq_r4w0\t1TCeoRPg5dE\tyAr26YhuYNY\t2ZgXx72XmoE\t-7ClGo-YgZ0\tvmdPOOd6cxI\tKRHfMQqSHpk\tpIMpORZthYw\t1tUDzOp10pk\theqocRij5P0\t_XIuvoH6rUg\tLGVU5DsezE0\tuO2kj6_D8B4\txiDqywcDQRM\tuX81lMev6_o");

        String string1 = ETLUtil.oriString2ETLString("LKh7zAJ4nwo\tTheReceptionist\t653\tEntertainment\t424\t13021\t4.34\t1305\t744");

        String string2 = ETLUtil.oriString2ETLString("SDNkMu8ZT68\tw00dy911\t630\tPeople & Blogs\t186\t10181\t3.49\t494\t257\trjnbgpPJUks");


        System.out.println(string);
        System.out.println("************************************");
        System.out.println(string1);

        System.out.println("----------------------------");
        System.out.println(string2);
    }
}
