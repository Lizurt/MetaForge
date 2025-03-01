package com.lizurt.metaforge;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest
public class PostgreSQLTests {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeAll
    public void setupDatabase() {
        ResourceDatabasePopulator populator;

        populator = new ResourceDatabasePopulator(new ClassPathResource("sql/clear_schema.sql"));
        populator.execute(jdbcTemplate.getDataSource());

        populator = new ResourceDatabasePopulator(new ClassPathResource("sql/init_original_tables.sql"));
        populator.execute(jdbcTemplate.getDataSource());

        populator = new ResourceDatabasePopulator(new ClassPathResource("sql/init_broken_tables.sql"));
        populator.execute(jdbcTemplate.getDataSource());

        populator = new ResourceDatabasePopulator(new ClassPathResource("sql/fill_tables.sql"));
        populator.execute(jdbcTemplate.getDataSource());
    }

    @Test
    public void testDomainsAndConstraints() {
        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(getDomainOf("users_broken", "id"))
                .matches(value -> value.contains("UUID"), "users.id should be UUID");
        softly.assertThat(getDomainOf("users_broken", "name"))
                .matches(value -> value.contains("TEXT"), "users.name should be TEXT");
        softly.assertThat(getDomainOf("users_broken", "birth_date"))
                .matches(value -> value.contains("DATE"), "users.birth_date should be DATE");
        softly.assertThat(getDomainOf("users_broken", "email"))
                .matches(value -> value.contains("TEXT"), "users.email should be TEXT");

        softly.assertThat(getDomainOf("products_broken", "id"))
                .matches(value -> value.contains("INTEGER"), "products.id should be INTEGER");
        softly.assertThat(getDomainOf("products_broken", "name"))
                .matches(value -> value.contains("TEXT"), "products.name should be TEXT");
        softly.assertThat(getDomainOf("products_broken", "price"))
                .matches(value -> value.contains("NUMERIC"), "products.price should be NUMERIC");

        softly.assertThat(getDomainOf("orders_broken", "id"))
                .matches(value -> value.contains("TEXT"), "orders.id should be TEXT");
        softly.assertThat(getDomainOf("orders_broken", "user_id"))
                .matches(value -> value.contains("UUID"), "orders.user_id should be UUID");
        softly.assertThat(getDomainOf("orders_broken", "product_id"))
                .matches(value -> value.contains("INTEGER"), "orders.product_id should be INTEGER");
        softly.assertThat(getDomainOf("orders_broken", "quantity"))
                .matches(value -> value.contains("INTEGER"), "orders.quantity should be INTEGER");
        softly.assertThat(getDomainOf("orders_broken", "order_date"))
                .matches(value -> value.contains("TIMESTAMP"), "orders.order_date should be TIMESTAMP");

        softly.assertThat(getConstraintType("users_broken", "users_pkey"))
                .matches(value -> value.contains("PRIMARY KEY"), "users_pkey should be PRIMARY KEY");
        softly.assertThat(getConstraintType("products_broken", "products_pkey"))
                .matches(value -> value.contains("PRIMARY KEY"), "products_pkey should be PRIMARY KEY");
        softly.assertThat(getConstraintType("orders_broken", "orders_pkey"))
                .matches(value -> value.contains("PRIMARY KEY"), "orders.pkey should be PRIMARY KEY");

        softly.assertThat(getConstraintType("orders_broken", "orders_user_id_fkey"))
                .matches(value -> value.contains("FOREIGN KEY"), "orders_user_id_fkey should be FOREIGN KEY");
        softly.assertThat(getConstraintType("orders_broken", "orders_product_id_fkey"))
                .matches(value -> value.contains("FOREIGN KEY"), "orders_product_id_fkey should be FOREIGN KEY");

        softly.assertAll();
    }

    private String getDomainOf(String table, String column) {

        String sql = "SELECT data_type FROM information_schema.columns WHERE table_name = ? AND column_name = ?";
        try {
            return jdbcTemplate.queryForObject(sql, new Object[]{table, column}, String.class).toUpperCase();
        } catch (Exception e) {
            return "";
        }
    }

    private String getConstraintType(String table, String constraint) {
        String sql = "SELECT constraint_type FROM information_schema.table_constraints WHERE table_name = ? AND constraint_name = ?";
        try {
            return jdbcTemplate.queryForObject(sql, new Object[]{table, constraint}, String.class).toUpperCase();
        } catch (Exception e) {
            return "";
        }
    }
}
