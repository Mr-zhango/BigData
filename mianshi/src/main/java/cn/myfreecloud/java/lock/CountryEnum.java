package cn.myfreecloud.java.lock;

import lombok.Getter;

public enum CountryEnum {

    ONE(1,"秦"),TWO(2,"楚"),THREE(3,"燕"),FOUR(4,"赵"),FIVE(5,"魏"),SIX(6,"韩");

    @Getter private Integer retCode;
    @Getter private String retMessage;

    CountryEnum(Integer retCode, String retMessage) {
        this.retCode = retCode;
        this.retMessage = retMessage;
    }

    // 枚举类中的自定义的一个方法,用来根据key返回value
    public static CountryEnum forEach_CountryEnum(int index){
        CountryEnum[] values = CountryEnum.values();
        for (CountryEnum value : values) {
            if(index == value.getRetCode()){
                // 找到之后就返回对应的枚举
                return value;
            }
        }
        // 找不到了就返回 null
        return null;
    }
}
