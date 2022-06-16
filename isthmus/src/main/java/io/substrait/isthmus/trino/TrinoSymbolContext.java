package io.substrait.isthmus.trino;

import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import java.util.List;

public class TrinoSymbolContext {
  private final TypeProvider typeProvider;
  private final List<Symbol> inputSymbols;

  public TrinoSymbolContext(TypeProvider typeProvider, List<Symbol> inputSymbols) {
    this.typeProvider = typeProvider;
    this.inputSymbols = inputSymbols;
  }

  public io.trino.spi.type.Type getType(Symbol symbol) {
    return typeProvider.get(symbol);
  }

  public int toOrdinal(Symbol symbol) {
    int index = 0;
    for (Symbol is : inputSymbols) {
      if (is.getName().equalsIgnoreCase(symbol.getName())) {
        return index;
      }
      index++;
    }
    throw new IllegalArgumentException(
        String.format("Could not resolve Symbol: %s in input symbol list.", symbol));
  }
}
