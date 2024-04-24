package org.example.sink;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.Statement;

public class JdbcStatementExecutor implements Serializable {

    private static final long serialVersionUID = 1323245422341L;


    private JdbcConnectionProvider jdbcConnectionProvider;

    private transient Connection connection;
    private transient Statement statement;


    public JdbcStatementExecutor(JdbcConnectionProvider jdbcConnectionProvider) {
        this.jdbcConnectionProvider = jdbcConnectionProvider;
    }


    public void executeStatement(String sql) throws SQLException, ClassNotFoundException {
        //System.out.println(sql);
        try {
            if (this.connection == null  || this.connection.isClosed()) {
                this.connection = this.jdbcConnectionProvider.getOrEstablishConnection();
            }
            statement = connection.createStatement();
            statement.execute(sql);
            statement.close();
        }catch (SQLRecoverableException e) {
            this.connection = this.jdbcConnectionProvider.getOrEstablishConnection();
        }

    }

    public void closeStatements() throws SQLException {

        if(statement != null && !statement.isClosed()) {
            try {
                statement.close();
                statement = null;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }






}
