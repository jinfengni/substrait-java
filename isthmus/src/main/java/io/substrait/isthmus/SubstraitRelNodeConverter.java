package io.substrait.isthmus;

import static io.substrait.isthmus.SqlToSubstrait.EXTENSION_COLLECTION;

import io.substrait.function.SimpleExtension;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.ExpressionRexConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.proto.AggregateFunction;
import io.substrait.relation.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

/**
 * RelVisitor to convert Substrait Rel plan to Calcite RelNode plan. Unsupported Rel node will call
 * visitFallback and throw UnsupportedOperationException.
 */
public class SubstraitRelNodeConverter extends AbstractRelVisitor<RelNode, RuntimeException> {

  private final RelOptCluster relOptCluster;
  private final CalciteCatalogReader catalogReader;

  private final SimpleExtension.ExtensionCollection extensions;

  private final ScalarFunctionConverter scalarFunctionConverter;

  private final AggregateFunctionConverter aggregateFunctionConverter;
  private final ExpressionRexConverter expressionRexConverter;

  private final RelBuilder relBuilder;

  public SubstraitRelNodeConverter(
      SimpleExtension.ExtensionCollection extensions,
      RelOptCluster relOptCluster,
      CalciteCatalogReader catalogReader,
      SqlParser.Config parserConfig) {
    this.relOptCluster = relOptCluster;
    this.catalogReader = catalogReader;
    this.extensions = extensions;

    this.relBuilder =
        RelBuilder.create(
            Frameworks.newConfigBuilder()
                .parserConfig(parserConfig)
                .defaultSchema(catalogReader.getRootSchema().plus())
                .traitDefs((List<RelTraitDef>) null)
                .programs()
                .build());

    this.scalarFunctionConverter =
        new ScalarFunctionConverter(
            this.extensions.scalarFunctions(), relOptCluster.getTypeFactory());

    this.aggregateFunctionConverter =
        new AggregateFunctionConverter(
            extensions.aggregateFunctions(), relOptCluster.getTypeFactory());

    this.expressionRexConverter =
        new ExpressionRexConverter(
            relOptCluster.getTypeFactory(), scalarFunctionConverter, aggregateFunctionConverter);
  }

  public static RelNode convert(
      Rel relRoot,
      RelOptCluster relOptCluster,
      CalciteCatalogReader calciteCatalogReader,
      SqlParser.Config parserConfig) {
    return relRoot.accept(
        new SubstraitRelNodeConverter(
            EXTENSION_COLLECTION, relOptCluster, calciteCatalogReader, parserConfig));
  }

  @Override
  public RelNode visit(Filter filter) throws RuntimeException {
    RelNode input = filter.getInput().accept(this);
    RexNode filterCondition = filter.getCondition().accept(expressionRexConverter);
    return relBuilder.push(input).filter(filterCondition).build();
  }

  @Override
  public RelNode visit(NamedScan namedScan) throws RuntimeException {
    return relBuilder.scan(namedScan.getNames()).build();
  }

  @Override
  public RelNode visit(Project project) throws RuntimeException {
    RelNode child = project.getInput().accept(this);
    List<RexNode> rexList =
        project.getExpressions().stream().map(expr -> expr.accept(expressionRexConverter)).toList();

    return relBuilder.push(child).project(rexList).build();
  }

  @Override
  public RelNode visit(Join join) throws RuntimeException {
    var left = join.getLeft().accept(this);
    var right = join.getRight().accept(this);
    var condition =
        join.getCondition()
            .map(c -> c.accept(expressionRexConverter))
            .orElse(relBuilder.literal(true));
    var joinType =
        switch (join.getJoinType()) {
          case INNER -> JoinRelType.INNER;
          case LEFT -> JoinRelType.LEFT;
          case RIGHT -> JoinRelType.RIGHT;
          case OUTER -> JoinRelType.FULL;
          case SEMI -> JoinRelType.SEMI;
          case ANTI -> JoinRelType.ANTI;
          case UNKNOWN -> throw new UnsupportedOperationException(
              "Unknown join type is not supported");
        };
    return relBuilder.push(left).push(right).join(joinType, condition).build();
  }

  @Override
  public RelNode visit(Aggregate aggregate) throws RuntimeException {
    RelNode child = aggregate.getInput().accept(this);
    var groupExprs =
        aggregate.getGroupings().stream()
            .map(
                gr ->
                    gr.getExpressions().stream()
                        .map(expr -> expr.accept(expressionRexConverter))
                        .toList())
            .toList();
    // TODO: group by key + grouping set.
    RelBuilder.GroupKey groupKey = relBuilder.groupKey(groupExprs.get(0), groupExprs);

    List<AggregateCall> aggregateCalls =
        aggregate.getMeasures().stream().map(m -> fromMeasure(m)).toList();
    return relBuilder.push(child).aggregate(groupKey, aggregateCalls).build();
  }

  private AggregateCall fromMeasure(Aggregate.Measure measure) {
    var arguments =
        measure.getFunction().arguments().stream()
            .map(expr -> expr.accept(expressionRexConverter))
            .toList();
    var operator =
        aggregateFunctionConverter.getSqlOperatorFromSubstraitFunc(
            measure.getFunction().declaration().key(), measure.getFunction().outputType());
    if (!operator.isPresent()) {
      throw new UnsupportedOperationException(
          String.format(
              "Unable to find binding for call %s", measure.getFunction().declaration().name()));
    }
    List<Integer> argIndex = new ArrayList<>();
    for (RexNode arg : arguments) {
      if (!(arg instanceof RexInputRef)) {
        throw new UnsupportedOperationException(
            String.format(
                "aggregation function only support column as argument. % is not support", arg));
      }
      argIndex.add(((RexInputRef) arg).getIndex());
    }

    boolean distinct =
        measure.getFunction().invocation()
            == AggregateFunction.AggregationInvocation.AGGREGATION_INVOCATION_DISTINCT;

    SqlAggFunction aggFunction;
    RelDataType returnType =
        TypeConverter.convert(relOptCluster.getTypeFactory(), measure.getFunction().getType());

    if (operator.get() == SqlStdOperatorTable.COUNT) {
      aggFunction = SqlStdOperatorTable.COUNT;
    } else if (operator.get() == SqlStdOperatorTable.SUM) {
      aggFunction = SqlStdOperatorTable.SUM;
    } else if (operator.get() == SqlStdOperatorTable.AVG) {
      aggFunction = SqlStdOperatorTable.AVG;
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Unsupported agg operator % for agg function ",
              operator.get(), measure.getFunction().declaration().name()));
    }

    return AggregateCall.create(
        aggFunction,
        distinct,
        false,
        false,
        argIndex,
        -1,
        null,
        RelCollations.EMPTY,
        returnType,
        null);
  }

  @Override
  public RelNode visitFallback(Rel rel) throws RuntimeException {
    throw new UnsupportedOperationException(
        String.format(
            "Rel $ of type %s not handled by visitor type %s.",
            rel, rel.getClass().getCanonicalName(), this.getClass().getCanonicalName()));
  }
}
