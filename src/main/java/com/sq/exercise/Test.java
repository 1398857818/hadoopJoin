package com.sq.exercise;

/**
 * Created by sq on 2018/1/27.
 */
public class Test {
    public static void main(String[] args){
        String  a = "aa";
        System.out.println(a.hashCode());
        String b = "aa";
        
        System.out.println(b.hashCode());
        String c = a;
        System.out.println(c.hashCode());
        
    }
}
