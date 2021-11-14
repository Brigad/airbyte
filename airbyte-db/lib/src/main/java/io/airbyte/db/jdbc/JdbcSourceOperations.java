/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.jdbc;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.db.SourceOperations;
import io.airbyte.protocol.models.JsonSchemaPrimitive;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Implementation of source operations with standard JDBC types.
 */
public class JdbcSourceOperations extends AbstractJdbcCompatibleSourceOperations<JDBCType> implements SourceOperations<ResultSet, JDBCType> {

  private JDBCType safeGetJdbcType(final int columnTypeInt) {
    try {
      return JDBCType.valueOf(columnTypeInt);
    } catch (final Exception e) {
      return JDBCType.VARCHAR;
    }
  }

  protected void setJsonField(final ResultSet resultSet, final int colIndex, final ObjectNode json) throws SQLException {
    final int columnTypeInt = resultSet.getMetaData().getColumnType(colIndex);
    final String columnName = resultSet.getMetaData().getColumnName(colIndex);
    final JDBCType columnType = safeGetJdbcType(columnTypeInt);

    // https://www.cis.upenn.edu/~bcpierce/courses/629/jdkdocs/guide/jdbc/getstart/mapping.doc.html
    switch (columnType) {
      case BIT, BOOLEAN -> putBoolean(json, columnName, resultSet, colIndex);
      case TINYINT, SMALLINT -> putShortInt(json, columnName, resultSet, colIndex);
      case INTEGER -> putInteger(json, columnName, resultSet, colIndex);
      case BIGINT -> putBigInt(json, columnName, resultSet, colIndex);
      case FLOAT, DOUBLE -> putDouble(json, columnName, resultSet, colIndex);
      case REAL -> putReal(json, columnName, resultSet, colIndex);
      case NUMERIC, DECIMAL -> putNumber(json, columnName, resultSet, colIndex);
      case CHAR, VARCHAR, LONGVARCHAR -> putString(json, columnName, resultSet, colIndex);
      case DATE -> putDate(json, columnName, resultSet, colIndex);
      case TIME -> putTime(json, columnName, resultSet, colIndex);
      case TIMESTAMP -> putTimestamp(json, columnName, resultSet, colIndex);
      case BLOB, BINARY, VARBINARY, LONGVARBINARY -> putBinary(json, columnName, resultSet, colIndex);
      default -> putDefault(json, columnName, resultSet, colIndex);
    }
  }

  @Override
  public void setStatementField(final PreparedStatement preparedStatement,
                                final int parameterIndex,
                                final JDBCType cursorFieldType,
                                final String value)
      throws SQLException {
    switch (cursorFieldType) {

      case TIMESTAMP -> setTimestamp(preparedStatement, parameterIndex, value);
      case TIME -> setTime(preparedStatement, parameterIndex, value);
      case DATE -> setDate(preparedStatement, parameterIndex, value);
      case BIT -> setBit(preparedStatement, parameterIndex, value);
      case BOOLEAN -> setBoolean(preparedStatement, parameterIndex, value);
      case TINYINT, SMALLINT -> setShortInt(preparedStatement, parameterIndex, value);
      case INTEGER -> setInteger(preparedStatement, parameterIndex, value);
      case BIGINT -> setBigInteger(preparedStatement, parameterIndex, value);
      case FLOAT, DOUBLE -> setDouble(preparedStatement, parameterIndex, value);
      case REAL -> setReal(preparedStatement, parameterIndex, value);
      case NUMERIC, DECIMAL -> setDecimal(preparedStatement, parameterIndex, value);
      case CHAR, NCHAR, NVARCHAR, VARCHAR, LONGVARCHAR -> setString(preparedStatement, parameterIndex, value);
      case BINARY, BLOB -> setBinary(preparedStatement, parameterIndex, value);
      // since cursor are expected to be comparable, handle cursor typing strictly and error on
      // unrecognized types
      default -> throw new IllegalArgumentException(String.format("%s is not supported.", cursorFieldType));
    }
  }

  @Override
  public JsonSchemaPrimitive getType(final JDBCType jdbcType) {
    return switch (jdbcType) {
      case BIT, BOOLEAN -> JsonSchemaPrimitive.BOOLEAN;
      case TINYINT, SMALLINT -> JsonSchemaPrimitive.NUMBER;
      case INTEGER -> JsonSchemaPrimitive.NUMBER;
      case BIGINT -> JsonSchemaPrimitive.NUMBER;
      case FLOAT, DOUBLE -> JsonSchemaPrimitive.NUMBER;
      case REAL -> JsonSchemaPrimitive.NUMBER;
      case NUMERIC, DECIMAL -> JsonSchemaPrimitive.NUMBER;
      case CHAR, NCHAR, NVARCHAR, VARCHAR, LONGVARCHAR -> JsonSchemaPrimitive.STRING;
      case DATE -> JsonSchemaPrimitive.STRING_DATE;
      case TIME -> JsonSchemaPrimitive.STRING_TIME;
      case TIMESTAMP -> JsonSchemaPrimitive.STRING_DATETIME;
      case BLOB, BINARY, VARBINARY, LONGVARBINARY -> JsonSchemaPrimitive.STRING;
      // since column types aren't necessarily meaningful to Airbyte, liberally convert all unrecgonised
      // types to String
      default -> JsonSchemaPrimitive.STRING;
    };
  }

}
