package org.example;

import org.example.common.CommonConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @创建人 君子固穷
 * @创建时间 2024-03-06
 * @描述
 */
public class ParamsBuilder {

    public static Map<String, String> sourceTable2TargetTable(String[] tbs) {
        Map<String, String> tablesMapping = new HashMap<>();
        for (int i = 0; i < tbs.length; i++) {
            if (!CommonConfig.TablePrefix.equals("")) {
                tablesMapping.put(tbs[i], (CommonConfig.TablePrefix + tbs[i]).toUpperCase());
            } else {
                tablesMapping.put(tbs[i], tbs[i].toUpperCase());
            }
            if (CommonConfig.diyTab2Tab.containsKey(tbs[i])) {
                tablesMapping.put(tbs[i], CommonConfig.diyTab2Tab.get(tbs[i]).toUpperCase());
            }
        }
        return tablesMapping;
    }

    public static Map<String, Map<String, String>> sourceField2TargetField(Map<String, String> tablesMapping) {
        Map<String, Map<String, String>> fieldsMapping = new HashMap<>();
        for (String table : tablesMapping.keySet()) {
            Map<String, String> t = new HashMap<>();

            List<String> fieldsList = CommonConfig.tableFieldsMap.get(table);
            for (String field : fieldsList) {
                t.put(field, field.toUpperCase());

                if (CommonConfig.diyColMap.containsKey(table)) {
                    if (CommonConfig.diyColMap.get(table).containsKey(field)) {
                        t.put(field, CommonConfig.diyColMap.get(table).get(field).toUpperCase());
                    }
                }
            }
            fieldsMapping.put(table, t);
        }
        return fieldsMapping;
    }










}
