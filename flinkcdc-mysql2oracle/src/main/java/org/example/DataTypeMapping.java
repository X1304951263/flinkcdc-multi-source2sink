package org.example;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * @创建人 君子固穷
 * @创建时间 2024-02-29
 * @描述
 */
public class DataTypeMapping {
    public static Map<String,Integer> dataTypeMap = new HashMap<>();

    static {
        //mysql
        dataTypeMap.put("varchar", Types.BIGINT);
        dataTypeMap.put("bit", Types.BIT);
        dataTypeMap.put("date", Types.JAVA_OBJECT);
        dataTypeMap.put("datetime", Types.JAVA_OBJECT);
        dataTypeMap.put("decimal", Types.DECIMAL);
        dataTypeMap.put("int", Types.INTEGER);
        dataTypeMap.put("bigint", Types.BIGINT);
        dataTypeMap.put("binary", Types.BIGINT);
        dataTypeMap.put("tinyint", Types.TINYINT);
        dataTypeMap.put("char", Types.TINYINT);
        dataTypeMap.put("double", Types.DOUBLE);
        dataTypeMap.put("float", Types.FLOAT);
        dataTypeMap.put("smallint", Types.SMALLINT);
        dataTypeMap.put("text", Types.LONGVARCHAR);
    }

}
