package br.com.giulianabezerra.sbdistributedtransactions;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.jta.JtaTransactionManager;

import com.atomikos.icatch.jta.UserTransactionImp;
import com.atomikos.icatch.jta.UserTransactionManager;
import com.atomikos.jdbc.AtomikosDataSourceBean;
import com.mysql.cj.jdbc.MysqlXADataSource;

@Configuration
public class DatasourceConfig {
  @Primary
  @Bean
  @ConfigurationProperties(prefix = "spring.datasource")
  public DataSource springDS() {
    return DataSourceBuilder.create().build();
  }

  @Bean
  @ConfigurationProperties(prefix = "db1.datasource")
  public DataSourceProperties db1Props() {
    return new DataSourceProperties();
  }

  @Bean
  @ConfigurationProperties(prefix = "db1.datasource")
  public DataSource db1DS(@Qualifier("db1Props") DataSourceProperties dsProps) {
    MysqlXADataSource xaDataSource = new MysqlXADataSource();
    xaDataSource.setUrl(dsProps.getUrl());
    xaDataSource.setUser(dsProps.getUsername());
    xaDataSource.setPassword(dsProps.getPassword());
    AtomikosDataSourceBean bean = new AtomikosDataSourceBean();
    bean.setXaDataSource(xaDataSource);
    bean.setUniqueResourceName("xaDb1");
    return bean;
  }

  @Bean
  @ConfigurationProperties(prefix = "db2.datasource")
  public DataSourceProperties db2Props() {
    return new DataSourceProperties();
  }

  @Bean
  @ConfigurationProperties(prefix = "db2.datasource")
  public DataSource db2DS(@Qualifier("db2Props") DataSourceProperties dsProps) {
    MysqlXADataSource xaDataSource = new MysqlXADataSource();
    xaDataSource.setUrl(dsProps.getUrl());
    xaDataSource.setUser(dsProps.getUsername());
    xaDataSource.setPassword(dsProps.getPassword());
    AtomikosDataSourceBean bean = new AtomikosDataSourceBean();
    bean.setXaDataSource(xaDataSource);
    bean.setUniqueResourceName("xaDb2");
    return bean;
  }

  @Primary
  @Bean(name = "transactionManagerJta")
  public PlatformTransactionManager transactionManagerJta() {
    return new JtaTransactionManager(new UserTransactionImp(), new UserTransactionManager());
  }
}