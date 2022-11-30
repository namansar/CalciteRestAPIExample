package org.gitcloned.calcite.adapter.nse;

import com.google.gson.JsonArray;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Pair;
import org.gitcloned.nse.MedicareFieldType;
import org.gitcloned.nse.NseFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NSEScannableTable extends AbstractTable
        implements ScannableTable {

    private final NSESchema nseSchema;
    private final String group;
    private final String table;
    private final String name;

    protected List<String> fieldNames;
    protected List<NseFieldType> fieldTypes;

    protected List<MedicareFieldType> fieldTypesMedi;

    private List<RelDataType> fields = new ArrayList<>();

    public NSEScannableTable(NSESchema nseSchema, String group, String table, String name) {
        this.nseSchema = nseSchema;
        this.group = group;
        this.table = table;
        this.name = name;
    }

    public String getTableName() {
        return this.name;
    }

    public String getTable() {
        return this.table;
    }

    public Enumerable<Object[]> scan(DataContext dataContext) {

        System.out.println( "\u001B[33m" + "Inside scan Enumerable method of NSEScanable class  --outside try block"+"\u001B[0m");
        try {
            System.out.println( "\u001B[33m" + "Inside scan Enumerable method of NSCScnable class before json array "+"\u001B[0m");
            final JsonArray results = this.nseSchema.getNseSession().scanTable(this.group, this.table);
            System.out.println(results);
            //final JsonArray postResult = this.nseSchema.getNseSession().scanTable(this.group,this.table);
            System.out.println( "\u001B[33m" + "Inside scan Enumerable method of NSCScnable class "+"\u001B[0m");

            return new AbstractEnumerable<Object[]>() {
                public Enumerator<Object[]> enumerator() {

                    //Enumerator<Object[]> enumerator = new NseDataEnumerator(results, fieldTypes, fieldNames);
                    Enumerator<Object[]> enumerator1 = new MediDataEnumerator(results,fieldTypesMedi,fieldNames);
                    //System.out.println(enumerator1.toString());
                    return enumerator1;
                }
            };

        } catch (IOException e) {
            throw new RuntimeException("Got IO exception: " + e.getMessage());
        }
    }

    /**
     * Get fields and their types in a row
     * @param typeFactory
     * @return
     */
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {

        final JsonArray results;
        try {
            results = this.nseSchema.getNseSession().scanTable(this.group, this.table);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        System.out.println("\u001B[33m"+ "printing json results" + "\u001B[0m");
        System.out.println(results);

        System.out.println( "\u001B[33m" + "Inside RowType Method"+"\u001B[0m");


        if (fieldTypes == null || fieldNames == null || fieldTypesMedi == null) {

            fieldTypes = new ArrayList<>();
            fieldNames = new ArrayList<>();
            fieldTypesMedi = new ArrayList<>();

            // Post response updates
            fieldTypesMedi.add(MedicareFieldType.STRING);
            fields.add(MedicareFieldType.STRING.toType((JavaTypeFactory) typeFactory));
            fieldNames.add("FIELDS");

            fieldTypesMedi.add(MedicareFieldType.STRING);
            fields.add(MedicareFieldType.STRING.toType((JavaTypeFactory) typeFactory));
            fieldNames.add("ID");


//            fieldTypes.add(NseFieldType.STRING);
//            fields.add(NseFieldType.STRING.toType((JavaTypeFactory) typeFactory));
//            fieldNames.add("SYMBOL");
//
//            fieldTypes.add(NseFieldType.DOUBLE);
//            fields.add(NseFieldType.DOUBLE.toType((JavaTypeFactory) typeFactory));
//            fieldNames.add("open");
//
//            fieldTypes.add(NseFieldType.DOUBLE);
//            fields.add(NseFieldType.DOUBLE.toType((JavaTypeFactory) typeFactory));
//            fieldNames.add("high");
//
//            fieldTypes.add(NseFieldType.DOUBLE);
//            fields.add(NseFieldType.DOUBLE.toType((JavaTypeFactory) typeFactory));
//            fieldNames.add("low");
//
//            fieldTypes.add(NseFieldType.DOUBLE);
//            fields.add(NseFieldType.DOUBLE.toType((JavaTypeFactory) typeFactory));
//            fieldNames.add("previousClose");
//
//            fieldTypes.add(NseFieldType.DOUBLE);
//            fields.add(NseFieldType.DOUBLE.toType((JavaTypeFactory) typeFactory));
//            fieldNames.add("ltp");
//
//            fieldTypes.add(NseFieldType.FLOAT);
//            fields.add(NseFieldType.FLOAT.toType((JavaTypeFactory) typeFactory));
//            fieldNames.add("per");
//
//            fieldTypes.add(NseFieldType.DOUBLE);
//            fields.add(NseFieldType.DOUBLE.toType((JavaTypeFactory) typeFactory));
//            fieldNames.add("trdVolM");
//
//            fieldTypes.add(NseFieldType.DOUBLE);
//            fields.add(NseFieldType.DOUBLE.toType((JavaTypeFactory) typeFactory));
//            fieldNames.add("wkhi");
//
//            fieldTypes.add(NseFieldType.DOUBLE);
//            fields.add(NseFieldType.DOUBLE.toType((JavaTypeFactory) typeFactory));
//            fieldNames.add("wklo");
//
//            fieldTypes.add(NseFieldType.FLOAT);
//            fields.add(NseFieldType.FLOAT.toType((JavaTypeFactory) typeFactory));
//            fieldNames.add("yPC");
//
//            fieldTypes.add(NseFieldType.TIMESTAMP);
//            fields.add(NseFieldType.TIMESTAMP.toType((JavaTypeFactory) typeFactory));
//            fieldNames.add("time");
        }

       //System.out.println("FieldTypeMedi --->..........................>>"+fieldTypesMedi);
//        System.out.println("FieldNames --->>>"+fieldNames);
//        System.out.println("FieldType ---->>>" + fieldTypes);

        return typeFactory.createStructType(Pair.zip(fieldNames, fields));
    }
}
