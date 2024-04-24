package org.example;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class MySqlDateTimeConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public void configure(Properties props) {

    }

    @Override
    public void converterFor(RelationalColumn field, ConverterRegistration<SchemaBuilder> registration) {
        String sqlType = field.typeName().toUpperCase();
        SchemaBuilder schemaBuilder = null;
        Converter converter = null;

        //1.注意，mysqlcdc读取的所有时间字段都会转成时间戳，但是doris不支持时间戳类型，导致导入的时间均为null
        //2.doris的double类型，小数部分取6位，且四舍五入
        if ("DATETIME".equals(sqlType)) {
            schemaBuilder = SchemaBuilder.string().optional().name("com.darcytech.debezium.datetime.string");
            converter = this::convertDateTime;
        }

        if ("DATE".equals(sqlType)) {
            schemaBuilder = SchemaBuilder.string().optional().name("com.darcytech.debezium.date.string");
            converter = this::convertDate;
        }

        if ("TIMESTAMP".equals(sqlType)) {
            schemaBuilder = SchemaBuilder.string().optional().name("com.darcytech.debezium.timestamp.string");
            converter = this::convertTimestamp;
        }

//        if("DOUBLE".equals( sqlType) || "DECIMAL".equals(sqlType)){
//            schemaBuilder = SchemaBuilder.string().optional().name("com.darcytech.debezium.decimal.string");
//            converter = this::convertDecimalOrDouble;
//        }


        if (schemaBuilder != null) {
            registration.register(schemaBuilder, converter);
        }

    }

    private String convertDateTime(Object input) {
        if (input == null)
            return null;
        return String.valueOf(input);
    }
    private String convertDate(Object input) {
        if (input == null)
            return null;
        return String.valueOf(input);
    }

    private String convertTimestamp(Object input) {
        if (input == null)
            return null;
        Long timestamp = Long.parseLong(String.valueOf(input));
        return sdf.format(new Date(timestamp));
    }

    private Object convertDecimalOrDouble(Object value) {
        if (value == null) {
            return null;
        }

        double originalValue = Double.parseDouble(String.valueOf(value));
        System.out.println("原来的："+originalValue);
        DecimalFormat df = new DecimalFormat("#.00");
        String formattedValue = df.format(originalValue);
        double result = Double.parseDouble(formattedValue);
        System.out.println("现在的："+result);

        return result;
    }



}
