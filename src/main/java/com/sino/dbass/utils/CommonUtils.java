package com.sino.dbass.utils;

/**
 * sink,tran...是否打印输出
 */
public class CommonUtils {
    /**
     * 验证是否打印输出
     * @param msg
     */
    public static void println(Object msg){
        if (ConfigurationManager.getBoolean("check.System.out.println")){
            System.out.println(msg);
        }
    }

    public static void printlnMain(Object msg,boolean checkPrintln){
        if (checkPrintln){
            System.out.println(msg);
        }
    }

    public static void printlnTran(Object msg,boolean checkPrintln){
        if (checkPrintln){
            System.out.println(msg);
        }
    }

    public static void printlnSink(Object msg,boolean checkPrintln){
        if (checkPrintln){
            System.out.println(msg);
        }
    }
}
