package io.substrait.isthmus.trino;

import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import io.trino.spi.type.CharType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.VarcharType;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import java.util.ArrayList;
import java.util.List;

public class TrinoTypeConverter {

  public static Type convert(io.trino.spi.type.Type type) {
    TypeCreator creator = Type.withNullability(true);

    return switch (type.getTypeSignature().getBase()) {
      case StandardTypes.INTEGER -> creator.I32;
      case StandardTypes.BIGINT -> creator.I64;
      case StandardTypes.DATE -> creator.DATE;
      case StandardTypes.DOUBLE -> creator.FP64;
      case StandardTypes.REAL -> creator.FP32;
      case StandardTypes.CHAR -> creator.fixedChar(((CharType) type).getLength());
      case StandardTypes.VARCHAR -> creator.varChar(((VarcharType) type).getBoundedLength());
      default -> throw new UnsupportedOperationException(
          String.format("Unable to convert type: %s.", type));
    };
  }

  public static NamedStruct toNamedStruct(TypeProvider provider, List<Symbol> symbols) {
    TypeCreator creator = Type.withNullability(true);
    var names = new ArrayList<String>();
    var children = new ArrayList<Type>();
    for (Symbol symbol : symbols) {
      names.add(symbol.getName());
      children.add(convert(provider.get(symbol)));
    }
    return NamedStruct.of(names, creator.struct(children));
  }
}
