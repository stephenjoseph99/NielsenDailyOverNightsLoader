package com.futuresmedia;

import com.sun.istack.internal.NotNull;
import com.sun.istack.internal.Nullable;

import java.sql.*;

/**
 * Created by scott on 4/19/17.
 */
public class FMDBConnector implements AutoCloseable {

    private final String host;
    private final String port = "5432";
    private final String userName;
    private final String password;
    private final String dbName;
    private Connection connection;


    public FMDBConnector(String host, String userName, String password, String dbName) {
        this.host = host;
        this.userName = userName;
        this.password = password;
        this.dbName = dbName;
        connect();
    }


    private void connect() {
        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection("jdbc:postgresql://" + host + ":" + port + "/" + dbName, userName, password);
            connection.setAutoCommit(false);



        } catch (ClassNotFoundException e) {
            System.out.println("Postgres Driver Not Found On Class Path");
            System.out.println(e.getMessage());

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public Connection getConnection() {
        return this.connection;
    }


    public @Nullable ResultSet selectQuery(@NotNull String query, Object... params) {

        ResultSet response = null;

        PreparedStatement ps = setQuery(query, params);

        if (ps != null) {
            try {
                response = ps.executeQuery();
            } catch (SQLException e) {
                e.printStackTrace();
                System.out.println(e.getMessage());
                rollBack();
            }
        }

        return response;


    }

    public @Nullable ResultSet insertOrUpdateQuery(@NotNull String query, Object... params) {
        int response = -1;
        ResultSet rs = null;

        PreparedStatement ps = setQuery(query, params);


        if (ps != null) {
            try {
                response = ps.executeUpdate();
                 rs = ps.getGeneratedKeys();
            } catch (SQLException e) {
                e.printStackTrace();
                rollBack();

            }
        }

        return rs;

    }


    private void rollBack() {
        try {
            connection.rollback();
            System.out.println("Transaction Safely Rolled Back...");
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("Transaction Not Rolled Back... SQL Exception Was Thrown");
        }
    }

    public void commit() {
        try {
            connection.commit();
            System.out.println("Transaction Successfully Committed...");
        } catch (SQLException e) {
            e.printStackTrace();
            rollBack();
        }
    }


    private @Nullable PreparedStatement setQuery(String query, Object[] params) {
        PreparedStatement ps;
        try {
             ps = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            int i = 1;
            for (Object ob : params) {
                ps.setObject(i, ob);
                i++;

            }


        } catch (SQLException e) {
            e.printStackTrace();
            rollBack();
            return null;

        }

        return ps;
    }




    @Override
    public void close() throws Exception {
        connection.close();
    }
}
