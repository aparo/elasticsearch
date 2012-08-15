package org.apache.lucene.search;

/**
 * Created by IntelliJ IDEA.
 * User: alberto
 * Date: 29/05/12
 * Time: 17:42
 */
public class Comparators {
    public static FieldComparator getIntegerComparator(int numHits, String field, FieldCache.Parser parser, Integer missingValue){
        return new FieldComparator.IntComparator(numHits, field, parser, missingValue);
    }

    public static FieldComparator getFloatComparator(int numHits, String field, FieldCache.Parser parser, Float missingValue){
        return new FieldComparator.FloatComparator(numHits, field, parser, missingValue);
    }

    public static FieldComparator getDoubleComparator(int numHits, String field, FieldCache.Parser parser, Double missingValue){
        return new FieldComparator.DoubleComparator(numHits, field, parser, missingValue);
    }

    public static FieldComparator getByteComparator(int numHits, String field, FieldCache.Parser parser, Byte missingValue){
        return new FieldComparator.ByteComparator(numHits, field, parser, missingValue);
    }

    public static FieldComparator getLongComparator(int numHits, String field, FieldCache.Parser parser, Long missingValue){
        return new FieldComparator.LongComparator(numHits, field, parser, missingValue);
    }

    public static FieldComparator getShortComparator(int numHits, String field, FieldCache.Parser parser, Short missingValue){
        return new FieldComparator.ShortComparator(numHits, field, parser, missingValue);
    }

    public static FieldComparator getTermOrdValComparator(int numHits, String field){
        return new FieldComparator.TermOrdValComparator(numHits, field);
    }

}