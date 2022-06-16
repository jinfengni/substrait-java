package io.substrait.isthmus.trino;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.warnings.WarningCollector;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.sql.planner.LogicalPlanner;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.planprinter.PlanPrinter;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.TestingSession;
import java.util.List;
import java.util.Map;

public class TrinoPlanner {
  private final Map<String, String> sessionProperties;
  private LocalQueryRunner queryRunner;

  public TrinoPlanner() {
    this(ImmutableMap.of());
  }

  public TrinoPlanner(Map<String, String> sessionProperties) {
    this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
    this.queryRunner = createLocalQueryRunner();
  }

  // Subclasses should implement this method to inject their own query runners
  private LocalQueryRunner createLocalQueryRunner() {
    Session.SessionBuilder sessionBuilder =
        TestingSession.testSessionBuilder()
            .setCatalog("local")
            .setSchema("tiny")
            .setSystemProperty(
                "task_concurrency", "1"); // these tests don't handle exchanges from local parallel

    sessionProperties.forEach(sessionBuilder::setSystemProperty);

    LocalQueryRunner queryRunner = LocalQueryRunner.create(sessionBuilder.build());

    queryRunner.createCatalog(
        queryRunner.getDefaultSession().getCatalog().get(),
        new TpchConnectorFactory(1),
        ImmutableMap.of());
    return queryRunner;
  }

  public Plan getPlan(String sql) {
    return getPlan(sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED);
  }

  public Plan getPlan(String sql, LogicalPlanner.Stage stage) {
    List<PlanOptimizer> optimizers = queryRunner.getPlanOptimizers(true);
    return queryRunner.inTransaction(
        transactionSession -> {
          Plan actual =
              queryRunner.createPlan(
                  transactionSession, sql, optimizers, stage, WarningCollector.NOOP);
          return actual;
        });
  }

  public String getFormattedPlan(String sql) {
    return getFormattedPlan(sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED);
  }

  public String getFormattedPlan(String sql, LogicalPlanner.Stage stage) {
    List<PlanOptimizer> optimizers = queryRunner.getPlanOptimizers(true);
    return getFormattedPlan(sql, stage, optimizers);
  }

  private String getFormattedPlan(
      String sql, LogicalPlanner.Stage stage, List<PlanOptimizer> optimizers) {
    return queryRunner.inTransaction(
        transactionSession -> {
          Plan actual =
              queryRunner.createPlan(
                  transactionSession, sql, optimizers, stage, WarningCollector.NOOP);
          String formattedPlan =
              PlanPrinter.textLogicalPlan(
                  actual.getRoot(),
                  actual.getTypes(),
                  queryRunner.getMetadata(),
                  queryRunner.getFunctionManager(),
                  StatsAndCosts.empty(),
                  transactionSession,
                  0,
                  false);
          return formattedPlan;
        });
  }
}
