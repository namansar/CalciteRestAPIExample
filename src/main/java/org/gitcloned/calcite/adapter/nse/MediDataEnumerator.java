package org.gitcloned.calcite.adapter.nse;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.commons.lang3.time.FastDateFormat;
import org.gitcloned.nse.MedicareFieldType;
import org.gitcloned.nse.MedicareFieldType;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

public class MediDataEnumerator implements Enumerator<Object[]> {
    private final JsonArray rows;
    private final String[] filterValues;
    private final ArrayRowConverter rowConverter;

    private int index = -1;
    private JsonObject current;

    private boolean isClosed = false;

    public MediDataEnumerator(JsonArray results, List<MedicareFieldType> fieldTypes, List<String> fieldNames) {
        this(results, fieldTypes, fieldNames, identityList(fieldTypes.size()));
    }

    public MediDataEnumerator(JsonArray results, List<MedicareFieldType> fieldTypes, List<String> fieldNames, int[] fields) {

        this(results, null, converter(fieldTypes, fieldNames, fields));
    }

    public MediDataEnumerator(JsonArray results, String[] filterValues, ArrayRowConverter rowConverter) {

        rows = results;

        this.filterValues = filterValues;
        this.rowConverter = rowConverter;
    }

    private static ArrayRowConverter converter(List<MedicareFieldType> fieldTypes, List<String> fieldNames,
                                                                 int[] fields) {
        return new ArrayRowConverter(fieldTypes, fieldNames, fields);
    }

    private JsonObject getNextRow() {

        return rows.get(++index).getAsJsonObject();
    }

    @Override
    public Object[] current() {

        try {
            return rowConverter.convertNormalRow(current);
        } catch (ParseException e) {
            throw new RuntimeException("Failed at parsing a row: " + e.getMessage());
        }
    }

    @Override
    public boolean moveNext() {

        if ((index + 1) == rows.size()) return false;

        current = getNextRow();

        return true;
    }

    @Override
    public void reset() {
        current = null;
    }

    @Override
    public void close() {
        isClosed = true;
    }

    /**
     * Returns an array of integers {0, ..., n - 1}.
     */
    static int[] identityList(int n) {
        int[] integers = new int[n];
        for (int i = 0; i < n; i++) {
            integers[i] = i;
        }
        return integers;
    }


    /**
     * Row converter.
     *
     * @param <E> element type
     */
    abstract static class RowConverter<E> {

        private static final FastDateFormat TIME_FORMAT_DATE;
        private static final FastDateFormat TIME_FORMAT_TIME;
        private static final FastDateFormat TIME_FORMAT_TIMESTAMP;

        static {
            final TimeZone gmt = TimeZone.getTimeZone("GMT");
            final TimeZone ist = TimeZone.getTimeZone("IST");
            TIME_FORMAT_DATE = FastDateFormat.getInstance("yyyy-MM-dd", gmt);
            TIME_FORMAT_TIME = FastDateFormat.getInstance("HH:mm:ss", gmt);
            TIME_FORMAT_TIMESTAMP =
                    // FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", gmt);
                    FastDateFormat.getInstance("MMM dd, yyyy HH:mm:ss", ist);
        }

        abstract E convertRow(JsonObject row);

        protected Object convert(MedicareFieldType fieldType, String string) throws ParseException {
            if (fieldType == null) {
                return string;
            }
            switch (fieldType) {
//                case BOOLEAN:
//                    if (string.length() == 0) {
//                        return null;
//                    }
//                    return Boolean.parseBoolean(string);
//                case BYTE:
//                    if (string.length() == 0) {
//                        return null;
//                    }
//                    return Byte.parseByte(string);
//                case SHORT:
//                    if (string.length() == 0) {
//                        return null;
//                    }
//                    return Short.parseShort(string);
//                case INT:
//                    if (string.length() == 0) {
//                        return null;
//                    }
//                    return Integer.parseInt(string);
//                case LONG:
//                    /*
//                    if (string.length() == 0) {
//                        return null;
//                    }
//                    return Long.parseLong(string);
//                    */
//                case FLOAT:
//                    // return Float.parseFloat(string);
//                case DOUBLE:
//                    if (string.length() == 0) {
//                        return null;
//                    }
//                    string = NumberFormat.getNumberInstance(Locale.getDefault()).parse(string).toString();
//                    if (fieldType == MedicareFieldType.LONG) {
//                        return Long.parseLong(string);
//                    } else if (fieldType == MedicareFieldType.DOUBLE) {
//                        return Double.parseDouble(string);
//                    } else if (fieldType == MedicareFieldType.FLOAT) {
//                        return Float.parseFloat(string);
//                    }
//                    // return Double.parseDouble(string);
//                case DATE:
//                    if (string.length() == 0) {
//                        return null;
//                    }
//                    try {
//                        Date date = TIME_FORMAT_DATE.parse(string);
//                        return (int) (date.getTime() / DateTimeUtils.MILLIS_PER_DAY);
//                    } catch (ParseException e) {
//                        return null;
//                    }
//                case TIME:
//                    if (string.length() == 0) {
//                        return null;
//                    }
//                    try {
//                        Date date = TIME_FORMAT_TIME.parse(string);
//                        return (int) date.getTime();
//                    } catch (ParseException e) {
//                        return null;
//                    }
//                case TIMESTAMP:
//                    if (string.length() == 0) {
//                        return null;
//                    }
//                    try {
//                        Date date = TIME_FORMAT_TIMESTAMP.parse(string);
//                        return date.getTime();
//                    } catch (ParseException e) {
//                        return null;
//                    }
                case STRING:
                    if(string.length() == 0){
                        System.out.println("String is null------>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                    }
                    //System.out.println("String is null and returning string------>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                    System.out.println(string);

                    return string;

                default:
                    return string;
            }
        }
    }

    /**
     * Array row converter.
     */
    static class ArrayRowConverter extends RowConverter<Object[]> {
        private final MedicareFieldType[] fieldTypes;
        private final String[] fieldNames;
        private final int[] fields;
        // whether the row to convert is from a stream
        private final boolean stream;

        ArrayRowConverter(List<MedicareFieldType> fieldTypes, List<String> fieldNames, int[] fields) {
            this.fieldTypes = fieldTypes.toArray(new MedicareFieldType[0]);
            this.fieldNames = fieldNames.toArray(new String[0]);
            this.fields = fields;
            this.stream = false;
        }

        public Object[] convertRow(JsonObject row) {
            try {
                return convertNormalRow(row);
            } catch (ParseException e) {
                throw new RuntimeException("Failed at parsing a row: " + e.getMessage());
            }
        }

        public Object[] convertNormalRow(JsonObject row) throws ParseException {
            final Object[] objects = new Object[fields.length];
            //System.out.println("Fields Length ------->>>>>>>@@@@@@@  "+ fields.length);
            //System.out.println("#################################################################");
            for (int i = 0; i < fields.length; i++) {
                int field = fields[i];
                System.out.println(field);
                System.out.println(row);
                System.out.println(row.get(fieldNames[i].toLowerCase())+"<<<<<<<>>>>>>>>>>>>>>>>>");
                System.out.println(fieldNames[i]);
              //  JsonArray value1 = row.get(fieldNames[i]).getAsJsonArray();
              //  System.out.println(value1+"<<<<<");
                JsonElement value = row.get(fieldNames[i].toLowerCase());
//                System.out.println(value.isJsonObject() +"  value is json object");
//                System.out.println(value.isJsonNull() +"  value is json Null");
//                System.out.println(value.isJsonArray() +"  value is json Array");

                if(i>0) {
                    //JsonElement value = row.get(fieldNames[0].toLowerCase()).getAsJsonObject().get(fieldNames[i].toLowerCase());
                    System.out.println("))))))))))))))))))))))((((((((((((((((((((((((((((((((((((((((((((((");
                    System.out.println(row.get(fieldNames[0].toLowerCase()).getAsJsonObject().get(fieldNames[i].toLowerCase()));
                    value = row.get(fieldNames[0].toLowerCase()).getAsJsonObject().get(fieldNames[i].toLowerCase());
                }

                if (value == null || value.isJsonNull()) {
                    objects[i] = null;
                    System.out.println("Object is null , check values......");
                } else if (value.isJsonObject()) {
                    System.out.println("Storing in object _____________>>>>>>>>>>>>>>>>>");
                    System.out.println(fieldTypes[field]);
                    System.out.println("&&&&&&&&&&&&&&&");
                    System.out.println(value.getAsJsonObject().toString());
                    System.out.println("&&&&&&&&&&&&&&&");
                    objects[i] = convert(fieldTypes[field], value.getAsJsonObject().toString());
                    System.out.println("Object is json object ");

                } else if (value.isJsonArray()) {
                    objects[i] = convert(fieldTypes[field], value.getAsJsonArray().toString());
                    System.out.println("Object is json Array ....... ");

                } else {
                    objects[i] = convert(fieldTypes[field], value.getAsString());
                    System.out.println("Object is converted to string .......");

                }
            }
              System.out.println("___________________________________________________________________");
            System.out.println(objects[0].toString());
            System.out.println(objects);
                //System.out.println("Object is null there is an issue .........&&&&&&&&&&&&&&&&&&&&&&&&&");

            return objects;
        }
    }

}
