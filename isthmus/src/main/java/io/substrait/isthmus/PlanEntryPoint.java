package io.substrait.isthmus;

import com.google.protobuf.util.JsonFormat;
import io.substrait.proto.Plan;
import picocli.CommandLine;

import java.util.List;
import java.util.concurrent.Callable;
import static picocli.CommandLine.*;

@Command(
    name = "isthmus",
    version = "isthmus 0.1",
    description = "Converts a SQL query to a Substrait Plan")
public class PlanEntryPoint implements Callable<Integer> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PlanEntryPoint.class);

//  @Parameters(index = "0", description = "The sql we should parse.")
//  private String sql;

//  @Parameters(index = "1", description = "The sql we should parse.")
//  private String sql1;
//
//  @Parameters(index = "2", description = "The sql we should parse.")
//  private String sql2;
//
//  @Parameters(index = "3", description = "The sql we should parse.")
//  private String sql3;

  @Option(names = {"-q", "--query"}, description = "select statements e.g. select foo, bar from t1")
  private List<String> queryStatements;

  @Option(names = {"-c", "--create"}, description = "Create table statements e.g. CREATE TABLE T1(foo int, bar bigint)")
  private List<String> createStatements;

  @Override
  public Integer call() throws Exception {
    SqlToSubstrait converter = new SqlToSubstrait();

    for (String query : queryStatements) {
      Plan plan = converter.execute(query, createStatements);
      System.out.println(JsonFormat.printer().includingDefaultValueFields().print(plan));
    }

//    Plan plan = converter.execute(sql, createStatements);
//    System.out.println(JsonFormat.printer().includingDefaultValueFields().print(plan));
//
//    Plan plan1 = converter.execute(sql1, createStatements);
//    System.out.println(JsonFormat.printer().includingDefaultValueFields().print(plan1));
//
//    Plan plan2 = converter.execute(sql1, createStatements);
//    System.out.println(JsonFormat.printer().includingDefaultValueFields().print(plan2));
//
//    Plan plan3 = converter.execute(sql3, createStatements);
//    System.out.println(JsonFormat.printer().includingDefaultValueFields().print(plan3));

    return 0;
  }

  // this example implements Callable, so parsing, error handling and handling user
  // requests for usage help or version help can be done with one line of code.
  public static void main(String... args) {
    int exitCode = new CommandLine(new PlanEntryPoint()).execute(args);
    System.exit(exitCode);
  }
}
