package org.example.sink;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class JdbcOutput extends RichOutputFormat<List<JSONObject>> implements Output, java.io.Serializable {

    private static final long serialVersionUID = 132422341L;

    private static Logger logger = LoggerFactory.getLogger(JdbcOutput.class);
    public JdbcConnectionProvider jdbcConnectionProvider;
    public JdbcStatementExecutor jdbcStatementExecutor;
    public JdbcExecutionOptions executionOptions;

    //{"user":"uer1"}
    public Map<String, String> tablesMapping;

    //{"user": ["id"]}
    public Map<String, List<String>> priKeyOrUniKeyMap;

    //{"user": 8096}
    public Map<String, Integer> batchCountMap = new HashMap<>();

    //{"user": "insert into user (id, name) values ('1', 1)"}
    public Map<String, StringBuilder> upsertSqlMap = new HashMap<>();

    //{"user": "update user set name = 1 where id = 1"}
    public Map<String, StringBuilder> updateSqlMap = new HashMap<>();

    //{"user": "delete from user where id = 1"}
    public Map<String, StringBuilder> deleteSqlMap = new HashMap<>();

    //{"user": {"id":"id1"}}
    public Map<String, Map<String, String>> fieldsMapping = new HashMap<>();

    //{"user": ["id","num"]}
    public Map<String, List<String>> fieldsListMap = new HashMap<>();

    public Map<String, StringBuilder> insertSqlMap = new HashMap<>();

    public Map<String, StringBuilder> insertSqlPrefixMap = new HashMap<>();


    public String schema;

    public Map<String, String> upsertPrefixSqlMap = new HashMap<>();

    public Map<String, String> upsertSuffixSqlMap = new HashMap<>();

    //是否是增量阶段的标识
    public Map<String, Boolean> incrementalPhaseFlag = new HashMap<>();




    public Map<String, Map<String, String>> tableFieldsWithTypeMap = new HashMap<>();

    public String jobName = "";

    public Boolean ALERT_FLAG = true;

    public AlertUtil alertUtil;

    public final SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd");

    public final SimpleDateFormat sdfDateTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    public final SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");


    public final Calendar calendar = Calendar.getInstance();


    public JdbcOutput(JdbcExecutionOptions executionOptions, JdbcConnectionOptions connectionOptions,
                      Map<String, String> tablesMapping,
                      Map<String, Map<String, String>> tableFieldsWithTypeMap,
                      Map<String, List<String>> priKeyOrUniKeyMap,
                      Map<String, Map<String, String>> fieldsMapping,
                      Map<String, List<String>> fieldsListMap,
                      String schema,
                      String jobName,
                      AlertUtil alertUtil) throws Exception {
        this.tablesMapping = tablesMapping;
        this.tableFieldsWithTypeMap = tableFieldsWithTypeMap;
        this.priKeyOrUniKeyMap = priKeyOrUniKeyMap;
        this.fieldsMapping = fieldsMapping;
        this.fieldsListMap = fieldsListMap;
        this.schema = schema;
        this.jobName = jobName;
        this.alertUtil = alertUtil;
        this.init();

        this.jdbcConnectionProvider = new JdbcConnectionProvider(connectionOptions);
        this.executionOptions = executionOptions;
        this.jdbcStatementExecutor = new JdbcStatementExecutor(this.jdbcConnectionProvider);

    }

    private void init() {
        for (String tableName : this.tablesMapping.keySet()) {

            this.incrementalPhaseFlag.put(tableName, false);

            this.batchCountMap.put(tableName, 0);
            this.insertSqlMap.put(tableName, new StringBuilder());
            this.upsertSqlMap.put(tableName, new StringBuilder());
            this.updateSqlMap.put(tableName, new StringBuilder());
            this.deleteSqlMap.put(tableName, new StringBuilder());

            StringBuilder insertSqlPrefix = new StringBuilder();
            insertSqlPrefix.append(" INTO ")
                    .append(this.schema)
                    .append(".\"")
                    .append(this.tablesMapping.get(tableName))
                    .append("\"(");

            List<String> fields = this.fieldsListMap.get(tableName);
            Map<String, String> fieldsMappingTmp = this.fieldsMapping.get(tableName);

            for (String field : fields) {
                insertSqlPrefix.append("\"")
                        .append(fieldsMappingTmp.get(field))
                        .append("\",");
            }
            insertSqlPrefix = insertSqlPrefix.deleteCharAt(insertSqlPrefix.length() - 1);
            insertSqlPrefix.append(") VALUES (");
            this.insertSqlPrefixMap.put(tableName, insertSqlPrefix);


            StringBuilder upsertSqlPrefix = new StringBuilder();
            upsertSqlPrefix.append("MERGE INTO ")
                    .append(this.schema)
                    .append(".\"")
                    .append(this.tablesMapping.get(tableName))
                    .append("\" target \nUSING (");

            this.upsertPrefixSqlMap.put(tableName, upsertSqlPrefix.toString());


            StringBuilder upsertSqlSuffix = new StringBuilder();
            upsertSqlSuffix.append(" ) source ON (");

            Map<String, String> fieldsMap = fieldsMapping.get(tableName);
            if (!this.priKeyOrUniKeyMap.get(tableName).isEmpty()) {
                for (String priKey : this.priKeyOrUniKeyMap.get(tableName)) {
                    upsertSqlSuffix
                            .append(" target.")
                            .append("\"")
                            .append(fieldsMap.get(priKey))
                            .append("\" = ")
                            .append("source.")
                            .append("\"")
                            .append(fieldsMap.get(priKey))
                            .append("\"")
                            .append(" AND ");
                }
                upsertSqlSuffix.delete(upsertSqlSuffix.length() - 4, upsertSqlSuffix.length());
            } else {
                for (String key : fieldsMap.keySet()) {
                    upsertSqlSuffix
                            .append(" target.")
                            .append("\"")
                            .append(fieldsMap.get(key))
                            .append("\" = ")
                            .append("source.")
                            .append("\"")
                            .append(fieldsMap.get(key))
                            .append("\"")
                            .append(" AND ");
                }
                upsertSqlSuffix.delete(upsertSqlSuffix.length() - 4, upsertSqlSuffix.length());
            }
            upsertSqlSuffix.append(")\nWHEN MATCHED THEN\n")
                    .append("    UPDATE SET");

            StringBuilder targetFields = new StringBuilder();
            StringBuilder sourceFields = new StringBuilder();

            this.fieldsListMap.get(tableName).forEach(field -> {
                if (!this.priKeyOrUniKeyMap.get(tableName).contains(field)) {
                    upsertSqlSuffix
                            .append("\n        target.")
                            .append("\"")
                            .append(fieldsMap.get(field))
                            .append("\" = ")
                            .append("source.")
                            .append("\"")
                            .append(fieldsMap.get(field))
                            .append("\",");
                }
                targetFields
                        .append("\n        target.")
                        .append("\"")
                        .append(fieldsMap.get(field))
                        .append("\",");
                sourceFields
                        .append("\n        source.")
                        .append("\"")
                        .append(fieldsMap.get(field))
                        .append("\",");
            });

            upsertSqlSuffix.delete(upsertSqlSuffix.length() - 1, upsertSqlSuffix.length());
            targetFields.delete(targetFields.length() - 1, targetFields.length());
            sourceFields.delete(sourceFields.length() - 1, sourceFields.length());

            upsertSqlSuffix
                    .append("\nWHEN NOT MATCHED THEN\n")
                    .append("    INSERT (")
                    .append(targetFields)
                    .append("\n    ) VALUES (")
                    .append(sourceFields)
                    .append("\n    )");

            this.upsertSuffixSqlMap.put(tableName, upsertSqlSuffix.toString());
        }


    }


    @Override
    public void configure(Configuration configuration) {
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
    }

    public synchronized void writeRecord(List<JSONObject> jsonObjects) throws IOException {
        try {
            for (JSONObject value : jsonObjects) {
                //System.out.println(value);
                String type = value.getString("type");
                String tableName = value.getString("tableName");

                if ("create".equals(type) || "read".equals(type)) {
//                    if (!this.incrementalPhaseFlag.get(tableName)) {
//                        //全量阶段使用insert
//                        this.createInsertSql(value);
//                    } else {
                    //增量阶段使用upsert
                    this.createUpsertSql(value);
                    //  }
                    this.batchCountMap.put(tableName, this.batchCountMap.get(tableName) + 1);

                } else if ("update".equals(type)) {

                    this.createUpdateSql(value);
                    this.flushAndUpdate();

                } else if ("delete".equals(type)) {

                    this.createDeleteSql(value);
                    this.flushAndDelete();
                }
            }
            this.flush();
        } catch (Exception var3) {
            if (this.ALERT_FLAG) {
                this.ALERT_FLAG = false;
                //发送飞书告警
                String causedBy = var3.getCause() == null ? "" : var3.getMessage();
                causedBy = causedBy.replace("\n", " ");
                String message = "警告：flinkcdc任务:" + this.jobName + ",写入数据失败，请及时查看。"
                        + "异常信息：" + causedBy;
                //System.out.println(message);
                this.alertUtil.sendAlert(message);
            }

            throw new IOException("Writing records to JDBC failed.", var3);
        }

    }


    protected synchronized void flush() throws Exception {
        for (String tableName : this.batchCountMap.keySet()) {
            if (this.batchCountMap.get(tableName) == 0) {
                continue;
            }

            int i = 1;
            while (i <= this.executionOptions.getMaxRetries()) {
                try {
                    String sql = "";

                    //if (this.incrementalPhaseFlag.get(tableName)) {
                    //增量阶段
                    StringBuilder sqlBuilder = new StringBuilder();
                    sqlBuilder.append(this.upsertSqlMap.get(tableName));
                    sqlBuilder.delete(sqlBuilder.length() - 9, sqlBuilder.length());

                    sql = (this.upsertPrefixSqlMap.get(tableName) + sqlBuilder +
                            this.upsertSuffixSqlMap.get(tableName)).toString();
                    this.jdbcStatementExecutor.executeStatement(sql);
                    this.batchCountMap.put(tableName, 0);
                    this.upsertSqlMap.get(tableName).setLength(0);
//                    } else {
//                        //全量阶段
//                        sql = this.insertSqlMap.get(tableName).toString();
//                        this.jdbcStatementExecutor.executeStatement(sql);
//                        this.batchCountMap.put(tableName, 0);
//                        this.insertSqlMap.get(tableName).setLength(0);
//                    }
                    break;
                } catch (SQLException var6) {
                    logger.error("JDBC executeBatch error, retry times = {}", i, var6);
                    if (i >= this.executionOptions.getMaxRetries()) {
                        throw new IOException(var6);
                    }

                    try {
                        Thread.sleep(1000L * i);
                    } catch (InterruptedException var4) {
                        Thread.currentThread().interrupt();
                        throw new IOException("unable to flush; interrupted while doing another attempt", var6);
                    }

                    ++i;
                }
            }
        }
    }

    protected synchronized void flushAndUpdate() throws Exception {

        this.flush();

        for (String tableName : this.tablesMapping.keySet()) {
            String sql = this.updateSqlMap.get(tableName).toString();
            if (sql == null || sql.length() == 0) {
                continue;
            }

            int i = 1;
            while (i <= this.executionOptions.getMaxRetries()) {
                try {
                    //System.out.println(sql);
                    this.jdbcStatementExecutor.executeStatement(sql);
                    this.updateSqlMap.get(tableName).setLength(0);
                    this.incrementalPhaseFlag.put(tableName, true);
                    break;
                } catch (SQLException var6) {
                    logger.error("JDBC executeBatch error, retry times = {}", i, var6);
                    if (i >= this.executionOptions.getMaxRetries()) {
                        throw new IOException(var6);
                    }

                    try {
                        Thread.sleep(1000L * i);
                    } catch (InterruptedException var4) {
                        Thread.currentThread().interrupt();
                        throw new IOException("unable to flush; interrupted while doing another attempt", var6);
                    }

                    ++i;
                }
            }

        }
    }


    protected synchronized void flushAndDelete() throws Exception {

        this.flush();

        for (String tableName : this.tablesMapping.keySet()) {
            String sql = this.deleteSqlMap.get(tableName).toString();
            if (sql == null || sql.length() == 0) {
                continue;
            }
            int i = 1;
            while (i <= this.executionOptions.getMaxRetries()) {
                try {
                    this.jdbcStatementExecutor.executeStatement(sql);
                    this.deleteSqlMap.get(tableName).setLength(0);
                    this.incrementalPhaseFlag.put(tableName, true);
                    break;
                } catch (SQLException var6) {
                    logger.error("JDBC execute delete error, retry times = {}", i, var6);
                    if (i >= this.executionOptions.getMaxRetries()) {
                        throw new IOException(var6);
                    }

                    try {
                        Thread.sleep(1000L * i);
                    } catch (InterruptedException var4) {
                        Thread.currentThread().interrupt();
                        throw new IOException("unable to flush; interrupted while doing another attempt", var6);
                    }

                    ++i;
                }
            }
        }
    }

    private void createPrefixInsertSql(String tableName) {
        StringBuilder insertSql = this.insertSqlMap.get(tableName);
        insertSql.append("INSERT ALL ").append(" SELECT * FROM DUAL");
    }

    private void createInsertSql(JSONObject value) {
        if (value == null) {
            throw new IllegalArgumentException("Invalid input record");
        }
        String tableName = value.getString("tableName");
        JSONObject afterJson = value.getJSONObject("after");

        if (this.batchCountMap.get(tableName) == 0) {
            this.createPrefixInsertSql(tableName);
        }

        StringBuilder insertSql = this.insertSqlMap.get(tableName);
        insertSql.delete(insertSql.length() - 19, insertSql.length());

        String prefixSql = this.insertSqlPrefixMap.get(tableName).toString();
        insertSql.append(prefixSql);
        for (String field : this.fieldsListMap.get(tableName)) {
            this.appendSqlValueBasedOnType(insertSql, afterJson, field,
                    this.tableFieldsWithTypeMap.get(tableName).get(field));

            insertSql.append(",");
        }

        insertSql.setLength(insertSql.length() - 1);
        insertSql.append(")").append(" SELECT * FROM DUAL");
    }


    private void createUpsertSql(JSONObject value) {
        if (value == null) {
            throw new IllegalArgumentException("Invalid input record");
        }
        String tableName = value.getString("tableName");
        JSONObject afterJson = value.getJSONObject("after");

        StringBuilder selectSql = this.upsertSqlMap.get(tableName);
        selectSql.append("\n    SELECT\n        ");
        for (String field : this.fieldsMapping.get(tableName).keySet()) {
            this.appendSqlValueBasedOnType(selectSql, afterJson, field,
                    this.tableFieldsWithTypeMap.get(tableName).get(field));

            selectSql.append(" AS \"")
                    .append(this.fieldsMapping.get(tableName).get(field))
                    .append("\"");

            selectSql.append(",");
        }

        selectSql.setLength(selectSql.length() - 1);
        selectSql.append("\n    FROM DUAL\n    UNION ALL");
    }

    private void createUpdateSql(JSONObject value) {
        if (value == null) {
            throw new IllegalArgumentException("Invalid input record");
        }
        String tableName = value.getString("tableName");
        JSONObject beforeJson = value.getJSONObject("before");
        JSONObject afterJson = value.getJSONObject("after");

        StringBuilder updateSql = this.updateSqlMap.get(tableName);
        updateSql.setLength(0);

        updateSql
                .append("UPDATE ").append(this.schema).append(".\"")
                .append(this.tablesMapping.get(tableName))
                .append("\" SET ");

        Map<String, String> fieldsMap = fieldsMapping.get(tableName);

        Iterator<Map.Entry<String, Object>> iterator = afterJson.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            String column = entry.getKey();

            if(!this.fieldsListMap.get(tableName).contains(column)){
                continue;
            }

            updateSql.append("\"").append(fieldsMap.get(column))
                    .append("\" = ");

            this.appendSqlValueBasedOnType(updateSql, afterJson, column,
                    this.tableFieldsWithTypeMap.get(tableName).get(column));

            updateSql.append(",");
        }

        updateSql.setLength(updateSql.length() - 1);

        updateSql.append(" WHERE ");

        if (!this.priKeyOrUniKeyMap.get(tableName).isEmpty()) {
            for (String priKey : this.priKeyOrUniKeyMap.get(tableName)) {
                updateSql.append("\"").append(fieldsMap.get(priKey))
                        .append("\" = ");

                this.appendSqlValueBasedOnType(updateSql, beforeJson, priKey,
                        this.tableFieldsWithTypeMap.get(tableName).get(priKey));

                updateSql.append(" AND ");
            }
        } else {
            Iterator<Map.Entry<String, Object>> iterator2 = beforeJson.entrySet().iterator();
            while (iterator2.hasNext()) {
                Map.Entry<String, Object> entry = iterator2.next();
                String column = entry.getKey();

                if(!this.fieldsListMap.get(tableName).contains(column)){
                    continue;
                }

                updateSql.append("\"").append(fieldsMap.get(column))
                        .append("\" = ");

                this.appendSqlValueBasedOnType(updateSql, beforeJson, column,
                        this.tableFieldsWithTypeMap.get(tableName).get(column));

                updateSql.append(" AND ");
            }
        }

        updateSql.setLength(updateSql.length() - 4);
    }

    private void createDeleteSql(JSONObject value) {
        String tableName = value.getString("tableName");
        JSONObject beforeJson = value.getJSONObject("before");

        StringBuilder deleteSqlBuilder = this.deleteSqlMap.get(tableName);
        deleteSqlBuilder.setLength(0);

        deleteSqlBuilder
                .append("DELETE FROM ").append(this.schema).append(".\"")
                .append(this.tablesMapping.get(tableName))
                .append("\" WHERE ");

        Map<String, String> fieldsMap = fieldsMapping.get(tableName);
        if (!this.priKeyOrUniKeyMap.get(tableName).isEmpty()) {
            for (String priKey : this.priKeyOrUniKeyMap.get(tableName)) {
                deleteSqlBuilder
                        .append("\"")
                        .append(fieldsMap.getOrDefault(priKey, priKey))
                        .append("\" = ");

                this.appendSqlValueBasedOnType(deleteSqlBuilder, beforeJson, priKey,
                        this.tableFieldsWithTypeMap.get(tableName).get(priKey));

                deleteSqlBuilder.append(" and ");
            }
        } else {
            Iterator<Map.Entry<String, Object>> iterator = beforeJson.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                String column = entry.getKey();

                if(!this.fieldsListMap.get(tableName).contains(column)){
                    continue;
                }

                deleteSqlBuilder
                        .append("\"")
                        .append(fieldsMap.getOrDefault(column, column))
                        .append("\" = ");

                this.appendSqlValueBasedOnType(deleteSqlBuilder, beforeJson, column,
                        this.tableFieldsWithTypeMap.get(tableName).get(column));

                deleteSqlBuilder.append(" AND ");
            }
        }

        deleteSqlBuilder.setLength(deleteSqlBuilder.length() - 4);
    }

    private void appendSqlValueBasedOnType(StringBuilder sqlBuilder, JSONObject value, String key, String dataType) {

        if (value.getString(key) == null) {

            sqlBuilder.append("null");

        } else if (dataType.equals("bit")) {

            if ((Boolean) value.get(key)) {
                sqlBuilder.append("1");
            } else {
                sqlBuilder.append("0");
            }

        } else if (dataType.equals("date")) {
            try {
                int daysSinceEpoch = value.getIntValue(key);
                calendar.set(1970, 0, 1); // Set the calendar to the epoch date
                calendar.add(Calendar.DATE, daysSinceEpoch); // Add the days to the epoch date

                sqlBuilder.append("TO_DATE('" + sdfDate.format(calendar.getTime()) + "','yyyy-MM-dd')");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        } else if (dataType.equals("datetime")) {
            try {
                long datetimeValue = value.getLongValue(key);
                int length = String.valueOf(datetimeValue).length();
                if (length > 13) {
                    datetimeValue = (long) (datetimeValue / Math.pow(10, (length - 13)));
                }
                datetimeValue = datetimeValue - (8 * 60 * 60 * 1000);
                Date date = new Date(datetimeValue);
                sqlBuilder.append("TO_DATE('" + sdfDateTime.format(date) + "','yyyy-MM-dd HH24:mi:ss')");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else if (dataType.equals("timestamp")) {
            String timestampString = value.getString(key);
            try {
                Date timestampDate = inputFormat.parse(timestampString);
                sqlBuilder.append("TO_DATE('" + sdfDateTime.format(timestampDate) + "','yyyy-MM-dd HH24:mi:ss')");
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

        } else if (dataType.equals("int") || dataType.equals("tinyint") ||
                dataType.equals("smallint") || dataType.equals("mediumint")) {
            sqlBuilder.append(value.getInteger(key));

        } else if (dataType.equals("binary")) {
            sqlBuilder.append("");

        } else if (dataType.equals("bigint")) {
            sqlBuilder.append(value.getLong(key));

        } else if (dataType.equals("double") || dataType.equals("float") ||
                dataType.equals("decimal")) {
            sqlBuilder.append(value.getString(key));

        } else if (dataType.equals("varchar") || dataType.equals("char") ||
                dataType.equals("text") || dataType.equals("longtext") ||
        dataType.equals("mediumtext") || dataType.equals("tinytext")) {

            sqlBuilder.append("'" + this.handleString(value.getString(key)) + "'");
        } else {
            sqlBuilder.append(value.getString(key));
        }
    }

    private String handleString(String value) {

        if (value == null || value.getBytes().length > 1000) {
            return "";
        }

        if (value.contains("'")) {
            value = value.replace("'", "\"");
        }
        return value;
    }


    @Override
    public synchronized void close() throws IOException {
        try {
            if (this.jdbcStatementExecutor != null) {
                this.jdbcStatementExecutor.closeStatements();
            }
        } catch (Exception var2) {
            logger.warn("Close JDBC writer failed.", var2);
        }

        this.jdbcConnectionProvider.closeConnection();
    }

}
