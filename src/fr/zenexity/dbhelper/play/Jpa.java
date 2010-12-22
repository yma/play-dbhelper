package fr.zenexity.dbhelper.play;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import play.db.jpa.JPA;

import fr.zenexity.dbhelper.Sql;

public class Jpa {

    public static <T> List<T> run(Sql.Query query) {
        return new Jpa().execute(query);
    }

    public static <T> List<T> run(String query, Object... params) {
        return new Jpa().execute(query, params);
    }

    public static <T> List<T> run(Sql.Query query, int offset, int size) {
        return new Jpa().execute(query, offset, size);
    }

    public static <T> List<T> run(String query, int offset, int size, Object... params) {
        return new Jpa().execute(query, offset, size, params);
    }

    public final EntityManager em;

    public Jpa(EntityManager em) {
        this.em = em;
    }

    public Jpa() {
        this(JPA.em());
    }

    public Query query(Sql.Query query) {
        int i = 0;
        Query jpaQuery = em.createQuery(query.toString());
        for (Object param : query.params()) jpaQuery.setParameter(++i, param);
        return jpaQuery;
    }

    public Query query(String query, Object... params) {
        int i = 0;
        Query jpaQuery = em.createQuery(query);
        for (Object param : params) jpaQuery.setParameter(++i, param);
        return jpaQuery;
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> execute(Sql.Query query) {
        return query(query).getResultList();
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> execute(String query, Object... params) {
        return query(query, params).getResultList();
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> execute(Sql.Query query, int offset, int size) {
        return query(query)
                .setFirstResult(offset)
                .setMaxResults(size)
                .getResultList();
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> execute(String query, int offset, int size, Object... params) {
        return query(query, params)
                .setFirstResult(offset)
                .setMaxResults(size)
                .getResultList();
    }

}
