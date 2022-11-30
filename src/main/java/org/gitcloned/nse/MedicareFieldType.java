package org.gitcloned.nse;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;

import java.util.HashMap;
import java.util.Map;

public enum MedicareFieldType {

    STRING(String.class, "string");
//    BOOLEAN(Primitive.BOOLEAN),
//    BYTE(Primitive.BYTE),
//    CHAR(Primitive.CHAR),
//    SHORT(Primitive.SHORT),
//    INT(Primitive.INT),
//    LONG(Primitive.LONG),
//    FLOAT(Primitive.FLOAT),
//    DOUBLE(Primitive.DOUBLE),
//    DATE(java.sql.Date.class, "date"),
//    TIME(java.sql.Time.class, "time"),
//    TIMESTAMP(java.sql.Timestamp.class, "timestamp");

    private final Class clazz;
    private final String simpleName;

    private static final Map<String, MedicareFieldType> MAP = new HashMap<>();

    static {
        for (MedicareFieldType value : values()) {
            MAP.put(value.simpleName, value);
            System.out.println("************************************************))))))))))))))))))))");
            System.out.println(value);
            System.out.println("************************************************))))))))))))))))))))");
        }
    }

    MedicareFieldType(Primitive primitive) {
        this(primitive.boxClass, primitive.primitiveName);
    }

    MedicareFieldType(Class clazz, String simpleName) {
        this.clazz = clazz;
        this.simpleName = simpleName;
    }

    public RelDataType toType(JavaTypeFactory typeFactory) {
        RelDataType javaType = typeFactory.createJavaType(clazz);
        RelDataType sqlType = typeFactory.createSqlType(javaType.getSqlTypeName());
        System.out.println("****#################################################################3333333333333333333" + sqlType);
        System.out.println("****3333333333333333333333333333333333333333333333333333333333333333333333333333333333333333 " + javaType);
        return typeFactory.createTypeWithNullability(sqlType, true);
    }

    public static MedicareFieldType  of(String typeString) {
        return MAP.get(typeString);
    }
}
