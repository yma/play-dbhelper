package fr.zenexity.dbhelper.play;

import java.sql.Connection;
import java.util.List;

import fr.zenexity.dbhelper.JdbcException;
import fr.zenexity.dbhelper.JdbcIterator;
import fr.zenexity.dbhelper.JdbcResult;
import fr.zenexity.dbhelper.JdbcStatementException;
import fr.zenexity.dbhelper.Sql;
import fr.zenexity.dbhelper.SqlScript;

import play.db.DB;

public class Jdbc extends fr.zenexity.dbhelper.Jdbc {

    public static <T> JdbcIterator<T> run(Sql.Query query, JdbcResult.Factory<T> resultFactory) throws JdbcException {
        return new Jdbc().execute(query, resultFactory);
    }

    public static <T> JdbcIterator<T> run(Sql.Query query, Class<T> resultClass) throws JdbcException {
        return new Jdbc().execute(query, resultClass);
    }

    public static <T> JdbcIterator<T> run(Sql.Query query, Class<T> resultClass, String... fields) throws JdbcException {
        return new Jdbc().execute(query, resultClass, fields);
    }

    public static <T> JdbcIterator<T> run(Sql.Query query, Class<T> resultClass, List<String> fields) throws JdbcException {
        return new Jdbc().execute(query, resultClass, fields);
    }

    public static <T> JdbcIterator<T> run(Sql.Query query, int offset, int size, JdbcResult.Factory<T> resultFactory) throws JdbcException {
        return new Jdbc().execute(query, offset, size, resultFactory);
    }

    public static <T> JdbcIterator<T> run(Sql.Query query, int offset, int size, Class<T> resultClass) throws JdbcException {
        return new Jdbc().execute(query, offset, size, resultClass);
    }

    public static <T> JdbcIterator<T> run(Sql.Query query, int offset, int size, Class<T> resultClass, String... fields) throws JdbcException {
        return new Jdbc().execute(query, offset, size, resultClass, fields);
    }

    public static <T> JdbcIterator<T> run(Sql.Query query, int offset, int size, Class<T> resultClass, List<String> fields) throws JdbcException {
        return new Jdbc().execute(query, offset, size, resultClass, fields);
    }

    public static int runUpdate(String query, Object... params) throws JdbcStatementException {
        return new Jdbc().executeUpdate(query, params);
    }

    public static int run(Sql.UpdateQuery query) throws JdbcStatementException {
        return new Jdbc().execute(query);
    }

    public static void run(SqlScript script) throws JdbcStatementException {
        new Jdbc().execute(script);
    }

    public Jdbc(Connection connection) {
        super(connection);
    }

    public Jdbc() {
        this(DB.getConnection());
    }

}
