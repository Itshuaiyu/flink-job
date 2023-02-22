package com.sino.dbass.utils;

import org.apache.log4j.Logger;

/**
 * 校验
 */
public class IsAvailableUtil {
  private static Logger logger = Logger.getLogger(IsAvailableUtil.class);

    /**
     * 校验字符串
     */
    public static Boolean isStringAvailable(String str){
        return (str != null && !("".equals(str)));
    }

    /**
     * 校验对象
     */
    public static Boolean isObjectAvailable(Object o){
        return o != null ;
    }
}
