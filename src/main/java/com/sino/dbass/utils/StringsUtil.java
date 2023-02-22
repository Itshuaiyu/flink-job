package com.sino.dbass.utils;

public class StringsUtil {
    public static String getStrFromStr2(String sourceString, String seprator, int position) {
        /**
         * position:代表要获取的字符串
         *
         */
        String res = "";
        switch (position) {
            case 1:
                String substring = sourceString.substring(0, sourceString.lastIndexOf(seprator));
                res = substring.substring(substring.lastIndexOf(seprator) + 1);
                break;
            case 2:
                res = sourceString.substring(sourceString.lastIndexOf(seprator) + 1);
                break;
            default:
                ;
        }
        return res;
    }

    public static String getStrFromStr1(String sourceString, String seprator, int position) {
        String[] split = sourceString.split(seprator, -1);
        if (position < split.length) {
            return split[position];
        } else {
            return "";
        }
    }
}
