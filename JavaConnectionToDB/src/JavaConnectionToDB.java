import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

import java.sql.*;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;


public class JavaConnectionToDB {

    public static void main(String[] args) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        String connectionURL = "jdbc:db2://10.4.53.14:50000/SAMPLE";

        Connection conn = null;
        Statement sqlStatement = null;
        ResultSet result = null;

        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver").newInstance();
            conn = DriverManager.getConnection(connectionURL, "stds61107", "ds690");
            sqlStatement = conn.createStatement();


            String st = "select FIRSTNME, SALARY, LASTNAME, PROJNAME, ACTNO\n" +
                    "from DB2INST1.EMPLOYEE, DB2INST1.EMPPROJACT, DB2INST1.PROJECT\n" +
                    "where DB2INST1.EMPLOYEE.EMPNO = DB2INST1.EMPPROJACT.EMPNO\n" +
                    "and DB2INST1.EMPPROJACT.PROJNO = DB2INST1.PROJECT.PROJNO";
            result = sqlStatement.executeQuery(st);

            int row = 0;
            while (result.next()) {
                System.out.print(result.getString("FIRSTNME") + " ");
                System.out.print(result.getString(2) + " ");
                System.out.print(result.getDouble("SALARY") + " ");
                System.out.print(result.getString("PROJNAME") + " ");
                System.out.print(result.getInt("ACTNO"));

                System.out.println();
            }

//            st = "INSERT  into  STUDENT values (101, 'Jack'), (102, 'Ann')";
//            row = sqlStatement.executeUpdate(st);

//            System.out.print("Inserted : " + row + " rows");

            conn.close();

        } catch (ClassNotFoundException ex) {
            Logger.getLogger(JavaConnectionToDB.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            Logger.getLogger(JavaConnectionToDB.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(JavaConnectionToDB.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        cassandraConnect();
    }

    public static void cassandraConnect() {
        String modeIP = "10.4.53.7";
        Cluster.Builder clusterBuilder = Cluster.builder().addContactPoint(modeIP).withPort(9042);
        Cluster cluster = clusterBuilder.build();

        Metadata metaData = cluster.getMetadata();
        System.out.println("Connected to Cluster : " + metaData.getClusterName());

        for (Host host: metaData.getAllHosts()) {
            System.out.println("Data center: " + host.getDatacenter()
            + " host " + host.getAddress() + " rack " + host.getRack());
        }

        Session session = cluster.connect();
        session.execute("use salekeyspace");
        com.datastax.driver.core.ResultSet rs = session.execute("select * from emptimesheetbydate");

        rs.forEach( r -> {

            System.out.print("emp id is " + r.getInt("emp_id"));
            System.out.print("date " + r.getDate("workdate").toString());
            System.out.print("start time " + r.getTime("start_time" ));
            Set<String> email = r.getSet("email", String.class);

            for (String em: email) {
                System.out.print(em + ", ");
            }
            List<String> list = r.getList("location", String.class);
            for (String l: list) {
                System.out.print(" " + list + " ");
            }

            System.out.println("");


        });

        cluster.close();
    }
}
