package io.substrait.isthmus.trino;

import io.substrait.function.SimpleExtension;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TrinoScalarFuncConverter {
  private final Map<String, SimpleExtension.ScalarFunctionVariant> functionVariantMultimap;

  public TrinoScalarFuncConverter(List<SimpleExtension.ScalarFunctionVariant> functions) {
    functionVariantMultimap = new HashMap<>();

    for (var f : functions) {
      functionVariantMultimap.put(f.key(), f);
    }
  }

  public Optional<SimpleExtension.ScalarFunctionVariant> getMatchedFunc(String key) {
    return Optional.of(functionVariantMultimap.get(key));
  }
}
