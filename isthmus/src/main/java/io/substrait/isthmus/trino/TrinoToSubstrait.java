package io.substrait.isthmus.trino;

import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.function.SimpleExtension;
import io.substrait.relation.ImmutableProject;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Rel;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import java.util.List;

public class TrinoToSubstrait extends PlanVisitor<Rel, TypeProvider> {

  private final SimpleExtension.ExtensionCollection extensions;

  public TrinoToSubstrait(SimpleExtension.ExtensionCollection extensions) {
    this.extensions = extensions;
  }

  @Override
  protected Rel visitPlan(PlanNode node, TypeProvider context) {
    throw new UnsupportedOperationException(
        String.format(
            "Unsupported PlanNode: %s for Visitor: %s",
            node.getClass().getCanonicalName(), this.getClass().getSimpleName()));
  }

  @Override
  public Rel visitTableScan(TableScanNode node, TypeProvider context) {
    var type = TrinoTypeConverter.toNamedStruct(context, node.getOutputSymbols());
    // TODO: how to get tablename from TableScanNode ??
    return NamedScan.builder()
        .initialSchema(type)
        .addAllNames(
            List.of(
                ((TpchTableHandle) (node.getTable().getConnectorHandle()))
                    .getTableName()
                    .toUpperCase()))
        .build();
  }

  @Override
  public Rel visitProject(ProjectNode node, TypeProvider context) {
    var inputSymboles = node.getSource().getOutputSymbols();

    var expressions =
        node.getAssignments().getExpressions().stream()
            .map(e -> toExpression(e, context, inputSymboles))
            .toList();

    return ImmutableProject.builder()
        .expressions(expressions)
        .input(node.getSource().accept(this, context))
        .build();
  }

  @Override
  public Rel visitOutput(OutputNode node, TypeProvider context) {
    TrinoSymbolContext symbolContext =
        new TrinoSymbolContext(context, node.getSource().getOutputSymbols());

    var expressions =
        node.getOutputSymbols().stream().map(e -> toExpression(e, symbolContext)).toList();

    return ImmutableProject.builder()
        .expressions(expressions)
        .input(node.getSource().accept(this, context))
        .build();
  }

  private Expression toExpression(
      io.trino.sql.tree.Expression trinoExpr, TypeProvider context, List<Symbol> inputSymbols) {
    return new TrinoExprToSubstraitExpr(extensions)
        .process(trinoExpr, new TrinoSymbolContext(context, inputSymbols));
  }

  private Expression toExpression(Symbol symbol, TrinoSymbolContext context) {
    var type = TrinoTypeConverter.convert(context.getType(symbol));
    return FieldReference.newRootStructReference(context.toOrdinal(symbol), type);
  }
}
