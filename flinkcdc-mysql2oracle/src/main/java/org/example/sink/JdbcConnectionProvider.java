package org.example.sink;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JdbcConnectionProvider implements Serializable {

    private static final long serialVersionUID = 132422341789L;

    private JdbcConnectionOptions jdbcConnectionOptions;

    private DriverManager driver;

    private transient Connection connection;



    public JdbcConnectionProvider(JdbcConnectionOptions jdbcConnectionOptions) throws Exception {
        this.jdbcConnectionOptions = jdbcConnectionOptions;

    }

    public Connection getConnection() {
        return this.connection;
    }

    public boolean isConnectionValid() throws SQLException {
        return this.connection != null && this.connection.isValid(10);
    }


    public Connection getOrEstablishConnection() throws SQLException, ClassNotFoundException {
//        if (this.connection != null) {
//            return this.connection;
//        }

        Class.forName("oracle.jdbc.OracleDriver");
        this.connection = DriverManager
                .getConnection(this.jdbcConnectionOptions.getUrl(),
                        this.jdbcConnectionOptions.getUser(),
                        this.jdbcConnectionOptions.getPassword());
        if (this.connection == null) {
            throw new SQLException("Connection could not be established.");
        }

        return this.connection;
    }


    public Connection getNewConnection() throws SQLException, ClassNotFoundException {
        Class.forName("oracle.jdbc.OracleDriver");
        this.connection = DriverManager
                .getConnection(this.jdbcConnectionOptions.getUrl(),
                        this.jdbcConnectionOptions.getUser(),
                        this.jdbcConnectionOptions.getPassword());
        if (this.connection == null) {
            throw new SQLException("Connection could not be established.");
        }
        return this.connection;
    }

    public void closeConnection() {
        if (this.connection != null) {
            try {
                this.connection.close();
            } catch (SQLException var5) {
            } finally {
                this.connection = null;
            }
        }
    }

    public Connection reestablishConnection() throws SQLException, ClassNotFoundException {
        this.closeConnection();
        return this.getOrEstablishConnection();
    }


}
