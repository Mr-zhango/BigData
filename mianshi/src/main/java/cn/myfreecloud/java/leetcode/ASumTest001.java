package cn.myfreecloud.java.leetcode;

import java.util.HashMap;

public class ASumTest001 {

    public static void main(String[] args) {

        // 初始化数组的大小
        // int[] arr = new int[3];

        // 直接创建好数组
        int[] intArray = new int[]{2, 7, 11, 15};
        int[] ints = twoSum(intArray, 9);

        for (int anInt : ints) {
            System.out.println(anInt);
        }

        System.out.println("*********************************");

        int[] intsTwo = twoSum2(intArray, 9);
        for (int i : intsTwo) {
            System.out.println(i);
        }

    }

    /**
     * 给定一个整数数组 nums 和一个目标值 target，请你在该数组中找出和为目标值的那 两个 整数，并返回他们的数组下标。
     * 你可以假设每种输入只会对应一个答案。但是，你不能重复利用这个数组中同样的元素。
     */
    public static int[] twoSum(int[] nums, int target) {
        for (int i = 0; i < nums.length; i++) {
            for (int j = i + 1; j < nums.length; j++) {
                if (nums[j] == target - nums[i]) {
                    return new int[]{i, j};
                }
            }
        }
        throw new IllegalArgumentException("No two sum solution");
    }

    public static int[] twoSum2(int[] nums, int target) {

        // 初始化返回值
        int[] result = new int[2];

        // 数据校验操作
        if(nums == null || nums.length <=1){
            return result;
        }
        HashMap<Integer,Integer> map = new HashMap<>();

        for (int i = 0; i < nums.length; i++) {
            // 当前值
            int num = nums[i];
            // 需要找的值
            int val = target - num;
            // 如果map中已经有了需要找的值,就返回
            if(map.containsKey(val)){
                result[0] = i;
                result[1] = map.get(val);
                return result;
            }else {
                // 如果map中没有需要找的值,就放进map中,标记该元素的索引
                map.put(num,i);
            }
        }

        return result;
    }
}
