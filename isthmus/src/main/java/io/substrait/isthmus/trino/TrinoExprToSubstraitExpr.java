package io.substrait.isthmus.trino;

import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.function.SimpleExtension;
import io.substrait.function.ToTypeString;
import io.substrait.type.Type;
import io.trino.sql.planner.Symbol;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.SymbolReference;
import java.util.List;
import java.util.stream.Collectors;

class TrinoExprToSubstraitExpr
    extends AstVisitor<io.substrait.expression.Expression, TrinoSymbolContext> {

  private final TrinoScalarFuncConverter funcConverter;

  TrinoExprToSubstraitExpr(SimpleExtension.ExtensionCollection extensionCollection) {
    funcConverter = new TrinoScalarFuncConverter(extensionCollection.scalarFunctions());
  }

  @Override
  protected io.substrait.expression.Expression visitArithmeticBinary(
      ArithmeticBinaryExpression node, TrinoSymbolContext context) {
    var left = this.process(node.getLeft(), context);
    var right = this.process(node.getRight(), context);

    var opTypes = List.of(left.getType(), right.getType());
    var operands = List.of(left, right);

    var outputType = left.getType(); // TODO: output type

    String name =
        switch (node.getOperator()) {
          case ADD:
            yield "add";
          case SUBTRACT:
            yield "substract";
          case MULTIPLY:
            yield "multiply";
          default:
            throw new UnsupportedOperationException(
                String.format(
                    "Unsupported operator %s in expression %s", node.getOperator(), node));
        };

    String key = constructKeyFromTypes(name, opTypes);

    SimpleExtension.ScalarFunctionVariant function = funcConverter.getMatchedFunc(key).get();

    return Expression.ScalarFunctionInvocation.builder()
        .outputType(outputType)
        .declaration(function)
        .arguments(operands)
        .build();
  }

  @Override
  protected io.substrait.expression.Expression visitSymbolReference(
      SymbolReference node, TrinoSymbolContext context) {
    var symbol = Symbol.from(node);
    var type = TrinoTypeConverter.convert(context.getType(symbol));
    return FieldReference.newRootStructReference(context.toOrdinal(symbol), type);
  }

  @Override
  protected Expression visitNode(Node node, TrinoSymbolContext context) {
    throw new UnsupportedOperationException(
        String.format(
            "Node: %s type : %s is not supported in Visitor :%s",
            node, node.getClass().getCanonicalName(), this.getClass().getCanonicalName()));
  }

  private String constructKeyFromTypes(String name, List<Type> arguments) {
    try {
      return name
          + ":"
          + "opt_"
          + arguments.stream()
              .map(t -> t.accept(ToTypeString.INSTANCE))
              .collect(Collectors.joining("_"));
    } catch (UnsupportedOperationException ex) {
      throw new UnsupportedOperationException(
          String.format("Failure converting types of function %s.", name), ex);
    }
  }
}
