/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.mysql;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.Database;
import io.airbyte.db.Databases;
import io.airbyte.integrations.source.mysql.MySqlSource.ReplicationMethod;
import io.airbyte.integrations.standardtest.source.AbstractSourceDatabaseTypeTest;
import io.airbyte.integrations.standardtest.source.TestDataHolder;
import io.airbyte.integrations.standardtest.source.TestDestinationEnv;
import io.airbyte.protocol.models.JsonSchemaPrimitive;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.jooq.SQLDialect;
import org.testcontainers.containers.MySQLContainer;

public class MySqlSourceDatatypeTest extends AbstractSourceDatabaseTypeTest {

  private MySQLContainer<?> container;
  private JsonNode config;

  @Override
  protected JsonNode getConfig() {
    return config;
  }

  @Override
  protected void tearDown(final TestDestinationEnv testEnv) {
    container.close();
  }

  @Override
  protected String getImageName() {
    return "airbyte/source-mysql:dev";
  }

  @Override
  protected Database setupDatabase() throws Exception {
    container = new MySQLContainer<>("mysql:8.0");
    container.start();

    config = Jsons.jsonNode(ImmutableMap.builder()
        .put("host", container.getHost())
        .put("port", container.getFirstMappedPort())
        .put("database", container.getDatabaseName())
        .put("username", container.getUsername())
        .put("password", container.getPassword())
        .put("replication_method", ReplicationMethod.STANDARD)
        .build());

    final Database database = Databases.createDatabase(
        config.get("username").asText(),
        config.get("password").asText(),
        String.format("jdbc:mysql://%s:%s/%s",
            config.get("host").asText(),
            config.get("port").asText(),
            config.get("database").asText()),
        "com.mysql.cj.jdbc.Driver",
        SQLDialect.MYSQL,
        "zeroDateTimeBehavior=convertToNull");

    // It disable strict mode in the DB and allows to insert specific values.
    // For example, it's possible to insert date with zero values "2021-00-00"
    database.query(ctx -> ctx.fetch("SET @@sql_mode=''"));

    return database;
  }

  @Override
  protected String getNameSpace() {
    return container.getDatabaseName();
  }

  @Override
  protected void initTests() {
    // bit defaults to bit(1), which is equivalent to boolean
    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("bit")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .addInsertValues("null", "1", "0")
            .addExpectedValues(null, "true", "false")
            .build());

    // bit(1) is equivalent to boolean
    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("bit")
            .fullSourceDataType("bit(1)")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .addInsertValues("null", "1", "0")
            .addExpectedValues(null, "true", "false")
            .build());

    // bit(>1) is binary
    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("bit")
            .fullSourceDataType("bit(7)")
            .airbyteType(JsonSchemaPrimitive.STRING)
            // 1000001 is binary for A
            .addInsertValues("null", "b'1000001'")
            // QQo= is base64 encoding in charset UTF-8 for A
            .addExpectedValues(null, "QQ==")
            .build());

    // tinyint without width
    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("tinyint")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .addInsertValues("null", "-128", "127")
            .addExpectedValues(null, "-128", "127")
            .build());

    // tinyint(1) is equivalent to boolean
    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("tinyint")
            .fullSourceDataType("tinyint(1)")
            .airbyteType(JsonSchemaPrimitive.BOOLEAN)
            .addInsertValues("null", "1", "0")
            .addExpectedValues(null, "true", "false")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("tinyint")
            .fullSourceDataType("tinyint(2)")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .addInsertValues("null", "-128", "127")
            .addExpectedValues(null, "-128", "127")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("smallint")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .addInsertValues("null", "-32768", "32767")
            .addExpectedValues(null, "-32768", "32767")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("smallint")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .fullSourceDataType("smallint zerofill")
            .addInsertValues("1")
            .addExpectedValues("1")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("smallint")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .fullSourceDataType("smallint unsigned")
            .addInsertValues("null", "0", "65535")
            .addExpectedValues(null, "0", "65535")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("mediumint")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .addInsertValues("null", "-8388608", "8388607")
            .addExpectedValues(null, "-8388608", "8388607")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("mediumint")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .fullSourceDataType("mediumint zerofill")
            .addInsertValues("1")
            .addExpectedValues("1")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("int")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .addInsertValues("null", "-2147483648", "2147483647")
            .addExpectedValues(null, "-2147483648", "2147483647")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("int")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .fullSourceDataType("int unsigned")
            .addInsertValues("3428724653")
            .addExpectedValues("3428724653")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("int")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .fullSourceDataType("int zerofill")
            .addInsertValues("1")
            .addExpectedValues("1")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("bigint")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .addInsertValues("null", "9223372036854775807")
            .addExpectedValues(null, "9223372036854775807")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("float")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .addInsertValues("null", "10.5")
            .addExpectedValues(null, "10.5")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("double")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .addInsertValues("null", "power(10, 308)", "1/power(10, 45)", "10.5")
            .addExpectedValues(null, String.valueOf(Math.pow(10, 308)), String.valueOf(1 / Math.pow(10, 45)), "10.5")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("decimal")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .fullSourceDataType("decimal(10,4)")
            .addInsertValues("0.188", "null")
            .addExpectedValues("0.188", null)
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("decimal")
            .airbyteType(JsonSchemaPrimitive.NUMBER)
            .fullSourceDataType("decimal(19,2)")
            .addInsertValues("1700000.00")
            .addInsertValues("1700000.00")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("date")
            .airbyteType(JsonSchemaPrimitive.STRING)
            .addInsertValues("null", "'2021-01-01'")
            .addExpectedValues(null, "2021-01-01T00:00:00Z")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("datetime")
            .airbyteType(JsonSchemaPrimitive.STRING)
            .addInsertValues("null", "'2005-10-10 23:22:21'")
            .addExpectedValues(null, "2005-10-10T23:22:21Z")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("timestamp")
            .airbyteType(JsonSchemaPrimitive.STRING)
            .addInsertValues("null", "'2021-01-00'", "'2021-00-00'", "'0000-00-00'")
            .addExpectedValues(null, null, null, null)
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("time")
            .airbyteType(JsonSchemaPrimitive.STRING)
            // JDBC driver can process only "clock"(00:00:00-23:59:59) values.
            .addInsertValues("null", "'-23:59:59.123456'", "'00:00:00'")
            .addExpectedValues(null, "1970-01-01T23:59:59Z", "1970-01-01T00:00:00Z")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("year")
            .airbyteType(JsonSchemaPrimitive.STRING)
            // MySQL converts values in the ranges '0' - '69' to YEAR value in the range 2000 - 2069
            // and '70' - '99' to 1970 - 1999.
            .addInsertValues("null", "1997", "99", "50")
            .addExpectedValues(null, "1997", "1999", "2050")
            .build());

    for (final String charType : Set.of("char", "varchar")) {
      addDataTypeTestData(
          TestDataHolder.builder()
              .sourceType(charType)
              .airbyteType(JsonSchemaPrimitive.STRING)
              .fullSourceDataType(charType + "(63)")
              .addInsertValues("null", "'Airbyte'", "'!\"#$%&\\'()*+,-./:;<=>?\\@[\\]^_\\`{|}~'")
              .addExpectedValues(null, "Airbyte", "!\"#$%&'()*+,-./:;<=>?@[]^_`{|}~")
              .build());

      addDataTypeTestData(
          TestDataHolder.builder()
              .sourceType(charType)
              .airbyteType(JsonSchemaPrimitive.STRING)
              .fullSourceDataType(charType + "(63) character set utf16")
              .addInsertValues("0xfffd")
              .addExpectedValues("�")
              .build());

      addDataTypeTestData(
          TestDataHolder.builder()
              .sourceType(charType)
              .airbyteType(JsonSchemaPrimitive.STRING)
              .fullSourceDataType(charType + "(63) character set cp1251")
              .addInsertValues("'тест'")
              .addExpectedValues("тест")
              .build());

      // when charset is binary, return binary in base64 encoding in charset UTF-8
      addDataTypeTestData(
          TestDataHolder.builder()
              .sourceType(charType)
              .airbyteType(JsonSchemaPrimitive.STRING)
              .fullSourceDataType(charType + "(7) character set binary")
              .addInsertValues("'Airbyte'")
              .addExpectedValues("QWlyYnl0ZQ==")
              .build());
    }

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("varbinary")
            .airbyteType(JsonSchemaPrimitive.STRING)
            .fullSourceDataType("varbinary(256)")
            .addInsertValues("null", "'test'")
            // @TODO Returns binary value instead of text
            // #5878 binary value issue
            // .addExpectedValues(null, "test")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("blob")
            .airbyteType(JsonSchemaPrimitive.STRING)
            .addInsertValues("null", "'test'")
            // @TODO Returns binary value instead of text
            // #5878 binary value issue
            // .addExpectedValues(null, "test")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("mediumtext")
            .airbyteType(JsonSchemaPrimitive.STRING)
            .addInsertValues(getLogString(1048000), "'test'")
            .addExpectedValues(StringUtils.leftPad("0", 1048000, "0"), "test")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("tinytext")
            .airbyteType(JsonSchemaPrimitive.STRING)
            .addInsertValues("null", "'test'")
            .addExpectedValues(null, "test")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("longtext")
            .airbyteType(JsonSchemaPrimitive.STRING)
            .addInsertValues("null", "'test'")
            .addExpectedValues(null, "test")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("text")
            .airbyteType(JsonSchemaPrimitive.STRING)
            .addInsertValues("null", "'test'")
            .addExpectedValues(null, "test")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("json")
            .airbyteType(JsonSchemaPrimitive.STRING)
            .addInsertValues("null", "'{\"a\": 10, \"b\": 15}'")
            .addExpectedValues(null, "{\"a\": 10, \"b\": 15}")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("point")
            .airbyteType(JsonSchemaPrimitive.OBJECT)
            .addInsertValues("null", "(ST_GeomFromText('POINT(1 1)'))")
            .build());

    addDataTypeTestData(
        TestDataHolder.builder()
            .sourceType("bool")
            .airbyteType(JsonSchemaPrimitive.STRING)
            // MySql boolean logic: Only value "1" is true
            .addInsertValues("null", "1", "0", "127", "-128")
            .addExpectedValues(null, "true", "false", "false", "false")
            .build());
  }

  private String getLogString(final int length) {
    final int maxLpadLength = 262144;
    final StringBuilder stringBuilder = new StringBuilder("concat(");
    final int fullChunks = length / maxLpadLength;
    for (int i = 1; i <= fullChunks; i++) {
      stringBuilder.append("lpad('0', 262144, '0'),");
    }
    stringBuilder.append("lpad('0', ").append(length % maxLpadLength).append(", '0'))");
    return stringBuilder.toString();
  }

}
