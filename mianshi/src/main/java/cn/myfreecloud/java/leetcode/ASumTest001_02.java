package cn.myfreecloud.java.leetcode;

public class ASumTest001_02 {

    public static void main(String[] args) {

//
//        int[] intArray = new int[]{2, 7, 11, 15};
//        int[] ints = twoSum(intArray, 9);
//
//
//        for (int anInt : ints) {
//            System.out.println(anInt);
//        }


        boolean b = judgeSquareSum(4);
        System.out.println(b);

    }


    public static int[] twoSum(int[] numbers, int target) {
        if (numbers == null) return null;
        int i = 0, j = numbers.length - 1;
        while (i < j) {
            int sum = numbers[i] + numbers[j];
            if (sum == target) {
                return new int[]{i + 1, j + 1};
            } else if (sum < target) {
                i++;
            } else {
                j--;
            }
        }
        return null;
    }


    public static boolean judgeSquareSum(int target) {
        if (target < 0) return false;
        int i = 0, j = (int) Math.sqrt(target);
        while (i <= j) {
            int powSum = i * i + j * j;
            if (powSum == target) {
                return true;
            } else if (powSum > target) {
                j--;
            } else {
                i++;
            }
        }
        return false;
    }
}
