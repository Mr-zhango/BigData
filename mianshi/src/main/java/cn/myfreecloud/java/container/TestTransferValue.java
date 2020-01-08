package cn.myfreecloud.java.container;

public class TestTransferValue{

	public void changeValue1(int age){
		age = 30;
	}

	public void changeValue2(Person person){
		person.setName("xxx");
	}

	public void changeValue3(String str){
		str = "xxx";
	}

	public static void main(String[] args){
		TestTransferValue testTransferValue = new TestTransferValue();
		//基本类型传入副本
		int age = 20;
		testTransferValue.changeValue1(age);
		System.out.println("age--------" + age);

		//引用位于栈，实例位于堆
		Person person = new Person("abc");
		testTransferValue.changeValue2(person);
		System.out.println("personName---------" + person.getName());

		//字符串特殊，会存在与常量池，如果有复用，如果没有新建
		String str = "abc";
		testTransferValue.changeValue3(str);
		System.out.println("String---------" + str);
	}
}

class Person{
	public Person(String name){
		setName(name);
	}
	private String name;

	public String getName(){
		return name;
	}

	public void setName(String name){
		this.name = name;
	}
}