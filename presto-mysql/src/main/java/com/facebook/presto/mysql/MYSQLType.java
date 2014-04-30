/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.mysql;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.Bytes;
import com.facebook.presto.mysql.util.MySQLUtils;
import com.facebook.presto.spi.ColumnType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public enum MYSQLType
        implements FullMySQLType
{
    CHAR(ColumnType.STRING, String.class),
    BIGINT(ColumnType.LONG, Long.class),
    BLOB(ColumnType.STRING, ByteBuffer.class),
    TEXT(ColumnType.STRING, ByteBuffer.class),
    CUSTOM(ColumnType.STRING, ByteBuffer.class),
    BOOLEAN(ColumnType.BOOLEAN, Boolean.class),
    COUNTER(ColumnType.LONG, Long.class),
    DECIMAL(ColumnType.DOUBLE, BigDecimal.class),
    DOUBLE(ColumnType.DOUBLE, Double.class),
    FLOAT(ColumnType.DOUBLE, Float.class),
    INET(ColumnType.STRING, InetAddress.class),
    INT(ColumnType.LONG, Integer.class),
    INTEGER(ColumnType.LONG, Integer.class),
    SMALLINT(ColumnType.LONG, Integer.class),
    TINYINT(ColumnType.LONG, Integer.class),
    TIMESTAMP(ColumnType.LONG, Date.class),
    TIME(ColumnType.LONG, Date.class),
    DATE(ColumnType.LONG, Date.class),
    UUID(ColumnType.STRING, java.util.UUID.class),
    TIMEUUID(ColumnType.STRING, java.util.UUID.class),
    VARCHAR(ColumnType.STRING, String.class),
    NVARCHAR(ColumnType.STRING, String.class),
    LONGVARCHAR(ColumnType.STRING, String.class),
    VARINT(ColumnType.STRING, BigInteger.class),
    LIST(ColumnType.STRING, null),
    MAP(ColumnType.STRING, null),
    SET(ColumnType.STRING, null);

    private final ColumnType nativeType;
    private final Class<?> javaType;

    MYSQLType(ColumnType nativeType, Class<?> javaType)
    {
        this.nativeType = checkNotNull(nativeType, "nativeType is null");
        this.javaType = javaType;
    }

    public ColumnType getNativeType()
    {
        return nativeType;
    }

    public int getTypeArgumentSize()
    {
        switch (this) {
            case LIST:
            case SET:
                return 1;
            case MAP:
                return 2;
            default:
                return 0;
        }
    }

    public static MYSQLType getMySQLType(String value)
    {
        switch (value) {
            case "CHAR":
                return CHAR;
            case "BIGINT":
                return BIGINT;
            case "BLOB":
                return BLOB;
            case "BOOLEAN":
                return BOOLEAN;
            case "DECIMAL":
                return DECIMAL;
            case "DOUBLE":
                return DOUBLE;
            case "FLOAT":
                return FLOAT;
            case "TIMESTAMP":
                return TIMESTAMP;
            case "DATE":
                return DATE;
            case "VARCHAR":
                return VARCHAR;
            case "NVARCHAR":
                return NVARCHAR;
            case "LONGNVARCHAR":
                return LONGVARCHAR;
            case "SMALLINT":
                return SMALLINT;
            case "TINYINT":
                return TINYINT;
            case "INT":
                return INT;
            case "TIME":
                return TIME;
            case "INTEGER":
                return INTEGER;
            case "TEXT":
                return TEXT;
            default:
                return null;
        }
    }

    public static int getNativeSQLType(String type)
    {
        switch (type) {
            case "CHAR":
                return Types.CHAR;
            case "BIGINT":
                return Types.BIGINT;
            case "BLOB":
                return Types.BLOB;
            case "BOOLEAN":
                return Types.BOOLEAN;
            case "DECIMAL":
                return Types.DECIMAL;
            case "DOUBLE":
                return Types.DOUBLE;
            case "FLOAT":
                return Types.FLOAT;
            case "TIMESTAMP":
                return Types.TIMESTAMP;
            case "DATE":
                return Types.DATE;
            case "VARCHAR":
                return Types.VARCHAR;
            case "NVARCHAR":
                return Types.NVARCHAR;
            case "LONGVARCHAR":
                return Types.LONGNVARCHAR;
            case "SMALLINT":
                return Types.SMALLINT;
            case "TINYINT":
                return Types.TINYINT;
            case "TIME":
                return Types.TIME;
            case "INT":
            case "INTEGER":
                return Types.INTEGER;
            default:
                return -1;
        }
    }

    public static Comparable<?> getColumnValue(Row row, int i, FullMySQLType fullMySQLType)
    {
        return getColumnValue(row, i, fullMySQLType.getMySQLType(), fullMySQLType.getTypeArguments());
    }

    public static Comparable<?> getColumnValue(Row row, int i, MYSQLType mySQLType,
            List<MYSQLType> typeArguments)
    {
        if (row.isNull(i)) {
            return null;
        }
        else {
            switch (mySQLType) {
                case CHAR:
                case TEXT:
                case VARCHAR:
                case NVARCHAR:
                case LONGVARCHAR:
                    return row.getString(i);
                case INT:
                    return (long) row.getInt(i);
                case BIGINT:
                case COUNTER:
                    return row.getLong(i);
                case BOOLEAN:
                    return row.getBool(i);
                case DOUBLE:
                    return row.getDouble(i);
                case FLOAT:
                    return (double) row.getFloat(i);
                case DECIMAL:
                    return row.getDecimal(i).doubleValue();
                case UUID:
                case TIMEUUID:
                    return row.getUUID(i).toString();
                case TIMESTAMP:
                    return row.getDate(i).getTime();
                case INET:
                    return row.getInet(i).toString();
                case VARINT:
                    return row.getVarint(i).toString();
                case BLOB:
                case CUSTOM:
                    return Bytes.toHexString(row.getBytesUnsafe(i));
                case SET:
                    checkTypeArguments(mySQLType, 1, typeArguments);
                    return buildSetValue(row, i, typeArguments.get(0));
                case LIST:
                    checkTypeArguments(mySQLType, 1, typeArguments);
                    return buildListValue(row, i, typeArguments.get(0));
                case MAP:
                    checkTypeArguments(mySQLType, 2, typeArguments);
                    return buildMapValue(row, i, typeArguments.get(0), typeArguments.get(1));
                default:
                    throw new IllegalStateException("Handling of type " + mySQLType
                            + " is not implemented");
            }
        }
    }

    public static Comparable<?> getMySQLColumnValue(ResultSet rows, int i, MYSQLType mySQLType,
            List<MYSQLType> typeArguments)
    {
        String pKeyValue = null;
        try {
          pKeyValue = rows.getString(i);
        }
        catch (SQLException e) {
           e.printStackTrace();
        }
        if (pKeyValue == null) {
            return null;
        }
        else {
            switch (mySQLType) {
                case CHAR:
                case TEXT:
                case VARCHAR:
                    return pKeyValue;
                case INT:
                case INTEGER:
                case SMALLINT:
                case TINYINT:
                case BIGINT:
                case COUNTER:
                    return Long.parseLong(pKeyValue);
                case BOOLEAN:
                    return Boolean.parseBoolean(pKeyValue);
                case DOUBLE:
                    return Double.parseDouble(pKeyValue);
                case FLOAT:
                    return Double.parseDouble(pKeyValue);
                case DECIMAL:
                    return new BigDecimal(pKeyValue).doubleValue();
                case UUID:
                case TIMEUUID:
                    return pKeyValue;
                case TIME:
                case DATE:
                case TIMESTAMP:
                    return new Date(Long.parseLong(pKeyValue)).getTime();
                case VARINT:
                    return new BigInteger(pKeyValue).toString();
                case BLOB:
                case CUSTOM:
                    return Bytes.toHexString(pKeyValue.getBytes());
                default:
                    throw new IllegalStateException("Handling of type " + mySQLType
                            + " is not implemented");
            }
        }
    }

    private static String buildSetValue(Row row, int i, MYSQLType elemType)
    {
        return buildArrayValue(row.getSet(i, elemType.javaType), elemType);
    }

    private static String buildListValue(Row row, int i, MYSQLType elemType)
    {
        return buildArrayValue(row.getList(i, elemType.javaType), elemType);
    }

    private static String buildMapValue(Row row, int i, MYSQLType keyType,
            MYSQLType valueType)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (Map.Entry<?, ?> entry : row.getMap(i, keyType.javaType, valueType.javaType).entrySet()) {
            if (sb.length() > 1) {
                sb.append(",");
            }
            sb.append(objectToString(entry.getKey(), keyType));
            sb.append(":");
            sb.append(objectToString(entry.getValue(), valueType));
        }
        sb.append("}");
        return sb.toString();
    }

    private static String buildArrayValue(Collection<?> collection, MYSQLType elemType)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (Object value : collection) {
            if (sb.length() > 1) {
                sb.append(",");
            }
            sb.append(objectToString(value, elemType));
        }
        sb.append("]");
        return sb.toString();
    }

    private static void checkTypeArguments(MYSQLType type, int expectedSize,
            List<MYSQLType> typeArguments)
    {
        if (typeArguments == null || typeArguments.size() != expectedSize) {
            throw new IllegalArgumentException("Wrong number of type arguments " + typeArguments
                    + " for " + type);
        }
    }

    public static String getColumnValueForCql(Row row, int i, MYSQLType cassandraType)
    {
        if (row.isNull(i)) {
            return null;
        }
        else {
            switch (cassandraType) {
                case CHAR:
                case TEXT:
                case VARCHAR:
                    return MySQLUtils.quoteStringLiteral(row.getString(i));
                case INT:
                    return Integer.toString(row.getInt(i));
                case BIGINT:
                case COUNTER:
                    return Long.toString(row.getLong(i));
                case BOOLEAN:
                    return Boolean.toString(row.getBool(i));
                case DOUBLE:
                    return Double.toString(row.getDouble(i));
                case FLOAT:
                    return Float.toString(row.getFloat(i));
                case DECIMAL:
                    return row.getDecimal(i).toString();
                case UUID:
                case TIMEUUID:
                    return row.getUUID(i).toString();
                case TIMESTAMP:
                    return Long.toString(row.getDate(i).getTime());
                case INET:
                    return row.getInet(i).toString();
                case VARINT:
                    return row.getVarint(i).toString();
                case BLOB:
                case CUSTOM:
                    return Bytes.toHexString(row.getBytesUnsafe(i));
                default:
                    throw new IllegalStateException("Handling of type " + cassandraType
                            + " is not implemented");
            }
        }
    }

    public static String getMySQLColumnStringValue(ResultSet rows, int i, MYSQLType mySQLType)
    {
        String pKeyValue = null;
        try {
          pKeyValue = rows.getString(i);
        }
        catch (SQLException e) {
           e.printStackTrace();
        }
        if (pKeyValue == null) {
            return null;
        }
        else {
            return MySQLUtils.quoteStringLiteral(pKeyValue);
        }
    }

    private static String objectToString(Object object, MYSQLType elemType)
    {
        switch (elemType) {
            case CHAR:
            case TEXT:
            case VARCHAR:
            case UUID:
            case TIMEUUID:
            case TIMESTAMP:
            case INET:
            case VARINT:
                return MySQLUtils.quoteStringLiteral(object.toString());

            case BLOB:
            case CUSTOM:
                return MySQLUtils.quoteStringLiteral(Bytes.toHexString((ByteBuffer) object));

            case INT:
            case BIGINT:
            case COUNTER:
            case BOOLEAN:
            case DOUBLE:
            case FLOAT:
            case DECIMAL:
                return object.toString();
            default:
                throw new IllegalStateException("Handling of type " + elemType + " is not implemented");
        }
    }

    @Override
    public MYSQLType getMySQLType()
    {
        if (getTypeArgumentSize() == 0) {
            return this;
        }
        else {
            // must not be called for types with type arguments
            throw new IllegalStateException();
        }
    }

    @Override
    public List<MYSQLType> getTypeArguments()
    {
        if (getTypeArgumentSize() == 0) {
            return null;
        }
        else {
            // must not be called for types with type arguments
            throw new IllegalStateException();
        }
    }

    public Object getJavaValue(Comparable<?> comparable)
    {
        switch (this) {
            case CHAR:
            case TEXT:
            case VARCHAR:
            case BIGINT:
            case BOOLEAN:
            case DOUBLE:
            case INET:
            case COUNTER:
                return comparable;
            case INT:
                return ((Long) comparable).intValue();
            case FLOAT:
                // conversion can result in precision lost
                return ((Double) comparable).floatValue();
            case DECIMAL:
                // conversion can result in precision lost
                return new BigDecimal((Double) comparable);
            case TIMESTAMP:
                return new Date((Long) comparable);
            case UUID:
            case TIMEUUID:
                return java.util.UUID.fromString((String) comparable);
            case BLOB:
            case CUSTOM:
                return Bytes.fromHexString((String) comparable);
            case VARINT:
                return new BigInteger((String) comparable);
            case SET:
            case LIST:
            case MAP:
            default:
                throw new IllegalStateException("Back conversion not implemented for " + this);
        }
    }
}
