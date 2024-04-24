package org.example.common;



import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

public class Util {
    private static final Logger log = Logger.getLogger(Util.class.getName());

    public static Set<String> userParams = new HashSet<>();
    public static String[] requiredParams = new String[]{  //必须参数
            "--source_host", "--source_port", "--source_username", "--source_password",
            "--source_database", "--source_table", "--sink_host", "--sink_port",
            "--sink_username", "--sink_password"//, "--sink_database"
    };

    public static void parseArgs(String[] args) throws Exception {
        String s = String.join(" ", args);
        log.info("load args: " + s);
        // 遍历命令行参数
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--operation":
                    if (i + 1 < args.length) {
                        CommonConfig.operation = args[i + 1];
                        i++;
                    }else {
                        log.severe("Missing operation argument after --operation.");
                        System.exit(1);
                    }
                    break;
                case "--sink_sid":
                    if (i + 1 < args.length) {
                        CommonConfig.SinkSID = args[i + 1];
                        i++;
                    } else {
                        log.severe("Missing sink-sid argument after --sink_sid.");
                        System.exit(1);
                    }
                    break;
                case "--source_schema":
                    if (i + 1 < args.length) {
                        CommonConfig.SourceSchema = args[i + 1];
                        i++;
                    } else {
                        log.severe("Missing source-schema argument after --source_schema.");
                        System.exit(1);
                    }
                    break;
                case "--sink_schema":
                    if (i + 1 < args.length) {
                        CommonConfig.SinkSchema = args[i + 1];
                        i++;
                    } else {
                        log.severe("Missing sink-schema argument after --sink_schema.");
                        System.exit(1);
                    }
                    break;
                case "--job_name":
                    if (i + 1 < args.length) {
                        Util.userParams.add("--job_name");
                        CommonConfig.JobName = args[i + 1];
                        i++; // 跳过主机名参数
                    } else {
                        log.severe("Missing host argument after --job_name.");
                        System.exit(1);
                    }
                    break;
                case "--source_host":
                    if (i + 1 < args.length) {
                        Util.userParams.add("--source_host");
                        CommonConfig.SourceHost = args[i + 1];
                        i++; // 跳过主机名参数
                    } else {
                        log.severe("Missing host argument after --source_host.");
                        System.exit(1);
                    }
                    break;
                case "--source_port":
                    if (i + 1 < args.length) {
                        try {
                            Util.userParams.add("--source_port");
                            CommonConfig.SourcePort = Integer.parseInt(args[i + 1]);
                            i++; // 跳过端口号参数
                        } catch (NumberFormatException e) {
                            log.severe("Invalid port number after --source_port.");
                            System.exit(1);
                        }
                    } else {
                        log.severe("Missing port argument after --source_port.");
                        System.exit(1);
                    }
                    break;
                case "--source_username":
                    if (i + 1 < args.length) {
                        Util.userParams.add("--source_username");
                        CommonConfig.SourceUser = args[i + 1];
                        i++; // 跳过用户名参数
                    } else {
                        log.severe("Missing user argument after --source_username.");
                        System.exit(1);
                    }
                    break;
                case "--source_password":
                    if (i + 1 < args.length) {
                        Util.userParams.add("--source_password");
                        CommonConfig.SourcePassword = URLDecoder.decode(args[i + 1], "UTF-8");
                        i++; // 跳过密码参数
                    } else {
                        log.severe("Missing password argument after --source_password.");
                        System.exit(1);
                    }
                    break;
                case "--source_database":
                    if (i + 1 < args.length) {
                        Util.userParams.add("--source_database");
                        CommonConfig.SourceDatabase = args[i + 1];
                        i++; // 跳过数据库参数
                    } else {
                        log.severe("Missing database argument after --source_database.");
                        System.exit(1);
                    }
                    break;
                case "--source_table":
                    if (i + 1 < args.length) {
                        Util.userParams.add("--source_table");
                        CommonConfig.SourceTable = args[i + 1];
                        i++; // 跳过表名参数
                    } else {
                        log.severe("Missing table argument after --source_table.");
                        System.exit(1);
                    }
                    break;
                case "--sink_host":
                    if (i + 1 < args.length) {
                        Util.userParams.add("--sink_host");
                        CommonConfig.SinkHost = args[i + 1];
                        i++; // 跳过主机名参数
                    } else {
                        log.severe("Missing host argument after --sink_host.");
                        System.exit(1);
                    }
                    break;
                case "--sink_port":
                    if (i + 1 < args.length) {
                        try {
                            Util.userParams.add("--sink_port");
                            CommonConfig.SinkPort = Integer.parseInt(args[i + 1]);
                            i++; // 跳过端口号参数
                        } catch (NumberFormatException e) {
                            log.severe("Invalid port number after --sink_port.");
                            System.exit(1);
                        }
                    } else {
                        log.severe("Missing port argument after --sink_port.");
                        System.exit(1);
                    }
                    break;
                case "--sink_username":
                    if (i + 1 < args.length) {
                        Util.userParams.add("--sink_username");
                        CommonConfig.SinkUser = args[i + 1];
                        i++; // 跳过用户名参数
                    } else {
                        log.severe("Missing user argument after --sink_username.");
                        System.exit(1);
                    }
                    break;
                case "--sink_password":
                    if (i + 1 < args.length) {
                        Util.userParams.add("--sink_password");
                        CommonConfig.SinkPassword = URLDecoder.decode(args[i + 1], "UTF-8");
                        i++; // 跳过密码参数
                    } else {
                        log.severe("Missing password argument after --sink_password.");
                        System.exit(1);
                    }
                    break;
                case "--sink_database":
                    if (i + 1 < args.length) {
                        Util.userParams.add("--sink_database");
                        CommonConfig.SinkDatabase = args[i + 1];
                        i++; // 跳过数据库参数
                    } else {
                        log.severe("Missing database argument after --sink_database.");
                        System.exit(1);
                    }
                    break;
                case "--table_prefix":
                    if (i + 1 < args.length) {
                        Util.userParams.add("--table_prefix");
                        CommonConfig.TablePrefix = args[i + 1];
                        i++; // 跳过前缀参数
                    } else {
                        log.severe("Missing prefix argument after --table_prefix.");
                        System.exit(1);
                    }
                    break;
                case "--sink_label":
                    if (i + 1 < args.length) {
                        Util.userParams.add("--sink_label");
                        CommonConfig.SinkLabel = args[i + 1];
                        i++; // 跳过前缀参数
                    } else {
                        log.severe("Missing label argument after --sink_label.");
                        System.exit(1);
                    }
                    break;
                case "--intervals":
                    if (i + 1 < args.length) {
                        try {
                            Util.userParams.add("--intervals");
                            CommonConfig.intervals = Long.parseLong(args[i + 1]);
                            i++; // 跳过前缀参数
                        } catch (NumberFormatException e) {
                            log.severe("Invalid intervals after --intervals.");
                            System.exit(1);
                        }
                    } else {
                        log.severe("Missing queue size argument after --queue_size.");
                        System.exit(1);
                    }
                    break;
                case "--batch_size":
                    if (i + 1 < args.length) {
                        try {
                            Util.userParams.add("--batch_size");
                            CommonConfig.batchSize = Integer.parseInt(args[i + 1]);
                            i++; // 跳过前缀参数
                        } catch (NumberFormatException e) {
                            log.severe("Invalid batch size after --batch_size.");
                            System.exit(1);
                        }
                    } else {
                        log.severe("Missing batch size argument after --batch_size.");
                        System.exit(1);
                    }
                    break;
                case "--tab2tab":
                    if (i + 1 < args.length) {
                        try {
                            Util.userParams.add("--tab2tab");
                            CommonConfig.Tab2Tab.add(args[i + 1]);
                            i++; // 跳过前缀参数
                        } catch (NumberFormatException e) {
                            log.severe("Invalid tab2tab after ----tab2tab.");
                            System.exit(1);
                        }
                    } else {
                        log.severe("Missing batch size argument after --batch_size.");
                        System.exit(1);
                    }
                    break;
                default:
                    log.severe("Invalid argument: " + args[i]);
                    System.exit(1);
            }
        }
    }

    public static void checkRequiredParams() {
        for (String param : Util.requiredParams) {
            if (!Util.userParams.contains(param)) {
                log.severe("Missing required parameter: " + param);
                System.exit(1);
            }
        }
    }

    public static void main(String[] args) throws UnsupportedEncodingException {
        System.out.println(URLEncoder.encode("LPD#test1234", "UTF-8"));
    }
}
