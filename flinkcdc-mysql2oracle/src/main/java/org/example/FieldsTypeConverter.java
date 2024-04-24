package org.example;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.text.SimpleDateFormat;
import java.util.Properties;

public class FieldsTypeConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public void configure(Properties props) {

    }

    @Override
    public void converterFor(RelationalColumn field, ConverterRegistration<SchemaBuilder> registration) {
        String sqlType = field.typeName().toUpperCase();
        SchemaBuilder schemaBuilder = null;
        Converter converter = null;

        //1.注意，mysqlcdc读取的所有时间字段都会转成时间戳

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
            converter = this::convertDateTime;
        }


        if (schemaBuilder != null) {
            registration.register(schemaBuilder, converter);
        }

    }

    private String convertDateTime(Object input) {
        if (input == null) {
            return null;
        }
        String inputStr = String.valueOf(input);
        // 截取日期和时间部分
        String datePart = inputStr.substring(0, 10); // 从索引0开始，截取10个字符
        String timePart = inputStr.substring(11,19);    // 从索引11开始，截取剩余的字符
        return datePart + " " + timePart;
    }
    private String convertDate(Object input) {
        if (input == null) {
            return null;
        }

        String inputStr = input + " 00:00:00";
        try {
            java.util.Date date = sdf.parse(inputStr);
            return sdf.format(date);
        }catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
