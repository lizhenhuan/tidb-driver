

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.*;

public class TestUK {

    private static String IP_PORT;
    private static String SCHEMA;
    private static String TABLE_NAME;
    private static String USER;
    private static String PASSWORD;

    private static void parseArgs(String [] args) {
        if (args.length == 5) {
            IP_PORT = args[0];
            SCHEMA = args[1];
            TABLE_NAME = args[2];
            USER = args[3];
            PASSWORD = args[4];

        } else {
            System.out.println("please input 5 args: IP_PORT SCHEMA  TABLE_NAME USER PASSWORD ");
            System.exit(1);
        }
    }

    public static void main(String[] args) throws Exception {
        parseArgs(args);
        validateUpsertUniqueConstraint();

    }

    private static void validateUpsertUniqueConstraint() throws Exception {

        String driver = "com.mysql.jdbc.Driver";
        String url = String.format("jdbc:mysql://%s/%s?useUnicode=true", IP_PORT, SCHEMA);
        Class.forName(driver);
        Connection connection = DriverManager.getConnection(url, USER, PASSWORD);
        Map<String, List<String>> uniqueIndexMap = new HashMap<>();
        try {
            // t_ods_t_sett_payment_apply_sync_test2  t_ods_t_sett_payment_apply_sync
            ResultSet resultSetUnique = connection.getMetaData()
                    .getIndexInfo(null, SCHEMA, TABLE_NAME, true, false);
            String uniqueKey = null;
            while (resultSetUnique.next()) {
                String uniqueKeyName = resultSetUnique.getString("INDEX_NAME");
                String columnNameUppercase = resultSetUnique.getString("COLUMN_NAME").toUpperCase(Locale.ROOT);
                if (uniqueIndexMap.containsKey(uniqueKeyName)) {
                    uniqueIndexMap.get(uniqueKeyName).add(columnNameUppercase);
                } else {
                    List<String> columnNameList = new ArrayList<>();
                    columnNameList.add(columnNameUppercase);
                    uniqueIndexMap.put(uniqueKeyName, columnNameList);
                }
                uniqueKey = uniqueKeyName;
            }

            if (uniqueIndexMap.size() == 0) {
                throw new RuntimeException("Illegal param for Tidb ON DUPLICATE KEY UPDATE function, Cause as [unique key] not exist ");
            }

            if (uniqueIndexMap.size() > 1) {
                throw new RuntimeException(String.format("Illegal param for Tidb ON DUPLICATE KEY UPDATE function not support multiple uniqueKeys, Details: %s"
                        , uniqueIndexMap.toString()));
            }
            System.out.println(uniqueIndexMap.get(uniqueKey));
//            if (!uniqueColumnsMappingValidate(uniqueIndexMap.get(uniqueKey))) {
//                throw new RuntimeException(String.format("Illegal param uniqueKeys, input param:[%s] target table：[%s]"
//                        , super.getUniqueColumnNameList(), uniqueIndexMap.toString()));
//            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
