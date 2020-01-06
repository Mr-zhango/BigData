package cn.myfreecloud.java.labdaexpress;

/**
 * 1.函数式编程
 * int age = 23;
 *
 * 面向对象+面向接口
 * lambda + 配合函数式接口使用
 *
 *
 *
 */

// 接口中有且一个方法,没有歧义

// 拷贝中括号,写死右箭头,落地大括号
// 函数式接口中,有且只能有一个方法,可以有一个或者多个已经实现的方法 default static

@FunctionalInterface
interface Foo {
    public void sayHello();

    default int mul(int x,int y){
       return x * y;
    }


    public static int div(int x,int y){
        return x / y;
    }

}


interface FooHaveInAndRetuen {
    public int sub(int x,int y);
}

interface FooHaveInAndRetuenAdd {
    public int add(int x,int y);
}



public class LabdaExpress {
    public static void main(String[] args) {


//        传统匿名内部类的写法
//        Foo foo = new Foo() {
//            @Override
//            public void sayHello() {
//                System.out.println("********");
//            }
//        };
//
//        foo.sayHello();

//        Foo foo = () -> {
//            System.out.println("hello sayHello Method");
//        };
//
//        foo.sayHello();


        FooHaveInAndRetuen fooHaveInAndRetuen = (int x,int y) -> x - y;

        System.out.println(fooHaveInAndRetuen.sub(2, 1));


        FooHaveInAndRetuenAdd fooHaveInAndRetuenAdd = Integer::sum;

        System.out.println(fooHaveInAndRetuenAdd.add(2, 1));
    }
}
