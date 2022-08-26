package br.com.giulianabezerra.sbdistributedtransactions;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.support.builder.CompositeItemWriterBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableBatchProcessing
public class BatchConfig {
  @Autowired
  private JobBuilderFactory jobBuilderFactory;

  @Autowired
  private StepBuilderFactory stepBuilderFactory;

  @Autowired
  @Qualifier("transactionManagerJta")
  private PlatformTransactionManager transactionManagerApp;

  @Bean
  public Job job(Step step) {
    return jobBuilderFactory
        .get("job")
        .start(step)
        .incrementer(new RunIdIncrementer())
        .build();
  }

  @Bean
  public Step step(ItemReader<Pessoa> reader,
      @Qualifier("writer") ItemWriter<Pessoa> writer) {
    return stepBuilderFactory
        .get("step")
        .<Pessoa, Pessoa>chunk(200)
        .reader(reader)
        .writer(writer)
        .transactionManager(transactionManagerApp)
        .build();
  }

  @Bean
  public ItemReader<Pessoa> reader() {
    return new FlatFileItemReaderBuilder<Pessoa>()
        .name("reader")
        .resource(new FileSystemResource("files/pessoas.csv"))
        .comments("--")
        .delimited()
        .names("nome", "email", "dataNascimento", "idade", "id")
        .targetType(Pessoa.class)
        .build();
  }

  @Bean
  public ItemWriter<Pessoa> writer(
      @Qualifier("writerDb1") ItemWriter<Pessoa> writerDb1, @Qualifier("writerDb2") ItemWriter<Pessoa> writerDb2) {
    return new CompositeItemWriterBuilder<Pessoa>()
        .delegates(List.of(writerDb1, writerDb2))
        .build();

  }

  @Bean
  public ItemWriter<Pessoa> writerDb1(@Qualifier("db1DS") DataSource dataSource) {
    return new JdbcBatchItemWriterBuilder<Pessoa>()
        .dataSource(dataSource)
        .sql(
            "INSERT INTO pessoa (id, nome, email, data_nascimento, idade) VALUES (:id, :nome, :email, :dataNascimento, :idade)")
        .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
        .build();
  }

  @Bean
  public ItemWriter<Pessoa> writerDb2(@Qualifier("db2DS") DataSource dataSource) {
    return new JdbcBatchItemWriterBuilder<Pessoa>()
        .dataSource(dataSource)
        .sql(
            "INSERT INTO pessoa (id, nome, email, data_nascimento, idade) VALUES (?, ?, ?, ?, ?)")
        .itemPreparedStatementSetter(itemPreparedStatementSetter())
        .build();
  }

  // Para simular um erro no db2
  private ItemPreparedStatementSetter<Pessoa> itemPreparedStatementSetter() {
    return new ItemPreparedStatementSetter<Pessoa>() {

      @Override
      public void setValues(Pessoa pessoa, PreparedStatement ps) throws SQLException {
        if (pessoa.getId() == 1071)
          throw new SQLException("Opa, deu erro!");

        ps.setInt(1, pessoa.getId());
        ps.setString(2, pessoa.getNome());
        ps.setString(3, pessoa.getEmail());
        ps.setString(4, pessoa.getDataNascimento());
        ps.setInt(5, pessoa.getIdade());
      }

    };
  }
}
