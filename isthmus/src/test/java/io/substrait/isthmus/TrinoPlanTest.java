package io.substrait.isthmus;

import static io.substrait.isthmus.SqlConverterBase.EXTENSION_COLLECTION;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.expression.proto.FunctionCollector;
import io.substrait.isthmus.trino.TrinoPlanner;
import io.substrait.isthmus.trino.TrinoToSubstrait;
import io.substrait.proto.PlanRel;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.LogicalPlanner;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

public class TrinoPlanTest extends PlanTestBase {

  @Test
  public void testTrinoPlanner() throws IOException, SqlParseException {

    String sql = "select orderkey, partkey, LINESTATUS, TAX from lineitem";
    String convertedSql = "SELECT ORDERKEY, PARTKEY, LINESTATUS, TAX\n" + "FROM LINEITEM";

    trinoPlannerConversion(sql, convertedSql);
  }

  @Test
  public void testTrinoPlanner2() throws IOException, SqlParseException {

    String sql = "select orderkey + partkey as myexpr from lineitem";

    // TODO: because Calcite has a 'AS $f17'
    // String convertedSql = "SELECT ORDERKEY + PARTKEY AS MYEXPR\n" + "FROM LINEITEM";

    trinoPlannerConversion(sql, List.of("SELECT ORDERKEY + PARTKEY", "FROM LINEITEM"));
  }

  @Test
  public void testTrioParser() throws IOException, SqlParseException {
    final String sql =
        "select * from \n"
            + "((select columns[0] as col0 from emps t1) \n"
            + "union all \n"
            + "(select columns[0] c2 from emps t2))";

    SqlParser parser = new SqlParser();
    System.out.println(parser.createStatement(sql, new ParsingOptions()));
  }

  private void trinoPlannerConversion(String sql, String convertedSql)
      throws IOException, SqlParseException {
    trinoPlannerConversion(sql, List.of(convertedSql));
  }

  private void trinoPlannerConversion(String sql, List<String> expectedSqls)
      throws IOException, SqlParseException {
    TrinoPlanner planner = new TrinoPlanner();
    var plan = planner.getPlan(sql, LogicalPlanner.Stage.CREATED);
    Rel rel = plan.getRoot().accept(new TrinoToSubstrait(EXTENSION_COLLECTION), plan.getTypes());

    FunctionCollector functionCollector = new FunctionCollector();
    var relProtoConverter = new RelProtoConverter(functionCollector);
    var protoPlan = io.substrait.proto.Plan.newBuilder();
    protoPlan
        .addRelations(
            PlanRel.newBuilder()
                .setRoot(
                    io.substrait.proto.RelRoot.newBuilder()
                        .setInput(rel.accept(relProtoConverter))
                        .addAllNames(List.of())))
        .build();

    String[] values = asString("tpch/trino_tpchschema.sql").split(";");
    var creates = Arrays.stream(values).filter(t -> !t.trim().isBlank()).toList();

    SubstraitToSql s = new SubstraitToSql();
    RelNode reoNode = s.substraitRelToCalciteRel(rel, creates);
    String actualConvertedSql = SubstraitToSql.toSql(reoNode);

    System.out.println(String.format("convertedSql:\n%s", actualConvertedSql));
    for (String key : expectedSqls) {
      assertTrue(actualConvertedSql.contains(key));
    }
  }
}
