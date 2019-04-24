package com.gridgain.experiment.gen;

import java.awt.event.KeyEvent;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Stupid data generator.
 */
public class CustomObjectRandomizer {

    private Class clazz;

    private Random random;

    private Map<Field, Random> randomizersMap;

    private int strLengthCap;

    private long minDate;

    private long maxDate;

    private Charset charset = StandardCharsets.US_ASCII;

    private List<String> fieldNames;

    private final List<Character> characters = collectPrintableCharactersOf(charset);

    private ConditionalObjectOverride objectOverride;

    public static List<Character> collectPrintableCharactersOf(Charset charset) {
        List<Character> chars = new ArrayList<>();

        for (int i = Character.MIN_VALUE; i < Character.MAX_VALUE; i++) {

            char character = (char)i;
            Character.UnicodeBlock block = Character.UnicodeBlock.of(character);

            if (block != null) {
                if (!Character.isISOControl(character) &&
                    character != KeyEvent.CHAR_UNDEFINED &&
                    block != null &&
                    block != Character.UnicodeBlock.SPECIALS) {

                    String characterAsString = Character.toString(character);
                    byte[] encoded = characterAsString.getBytes(charset);
                    String decoded = new String(encoded, charset);

                    if (characterAsString.equals(decoded))
                        chars.add(character);
                }
            }
        }
        return chars;
    }

    public <T> void init(Class<T> cls, List<String> fieldNames) throws IllegalAccessException, InstantiationException {
        this.clazz = cls;

        int mapCapacity = 1;

        if (fieldNames != null )
            mapCapacity = fieldNames.size();

        this.randomizersMap = new HashMap<>(mapCapacity, 1.0f);

        this.fieldNames = fieldNames;

        for (Field field : cls.getDeclaredFields()) {
            Class<?> type = field.getType();

            if (fieldNames == null || fieldNames.contains(field.getName()))
                randomizersMap.put(field, new Random());
        }

        Calendar minCal = Calendar.getInstance();
        minCal.set(Calendar.YEAR, 2015);
        minCal.set(Calendar.MONTH, 01);
        minCal.set(Calendar.DAY_OF_MONTH, 01);
        minDate = minCal.getTimeInMillis();

        Calendar maxCal = Calendar.getInstance();
        maxCal.set(Calendar.YEAR, 2019);
        maxCal.set(Calendar.MONTH, 02);
        maxCal.set(Calendar.DAY_OF_MONTH, 21);
        maxDate = maxCal.getTimeInMillis();
    }

    public void setConditionalObjectOverride(ConditionalObjectOverride coo) {
        objectOverride = coo;
    }

    public <T> T generateRandomObject(long i) throws IllegalAccessException, InstantiationException {
        T instance = (T)clazz.newInstance();

        for (Field field : clazz.getDeclaredFields()) {
            if ( fieldNames == null || fieldNames.contains(field.getName())) {
                field.setAccessible(true);
                Object val;
                if ( objectOverride.mustOverride(field.getName()) )
                    //val = objectOverride.lookup(field.getName(), Math.toIntExact(i));
                    val = cast( field, objectOverride.lookup(field.getName(), Math.toIntExact(i)) );
                else
                    val = randomize(field);

//                if (val == null)
//                    System.err.println("null for " + field.getName());

                field.set(instance, val);
            }
        }

        return instance;
    }

    private String randomizeString(Random random) {
        int length = 1 + random.nextInt(strLengthCap);
        char[] chars = new char[length];

        for (int i = 0; i < length; i++)
            chars[i] = characters.get(random.nextInt(characters.size()));

        return new String(chars);
    }

    private Integer randomizeInt(Random random) {
        return random.nextInt();
    }

    private Double randomizeDouble(Random random) {
        return random.nextDouble();
    }

    private Date randomizeDate(Random random) {
        return new Date((long) nextRangedDouble((double) minDate, (double) maxDate, random));
    }

    private BigDecimal randomizeBigDecimal(Random random) {
        return new BigDecimal(random.nextDouble());
    }

    private Timestamp randomizeTimestamp(Random random) {
        return new Timestamp((long) nextRangedDouble((double) minDate, (double) maxDate, random));
    }

    private Long randomizeLong(Random random) {
        return (long)nextRangedDouble((double)minDate, (double)maxDate, random);
    }

    private double nextRangedDouble(final double min, final double max, Random random) {
        double val = min + (random.nextDouble() * (max - min));
        if (val < min)
            return min;
        else if (val > max)
            return max;
        else
            return val;
    }

    private Object cast(Field field, String value) {
        Class<?> type = field.getType();

        if (type.equals(String.class))
            return value;

        else if (type.equals(Double.class))
            return Double.valueOf(value);

        else if (type.equals(BigDecimal.class))
            return new BigDecimal(value);

        else if (type.equals(Integer.class))
            return Integer.valueOf(value);

        else if (type.equals(Long.class))
            return Long.valueOf(value);

        return null;
    }

    private final Random nullRnd = new Random();

    private Object randomize(Field field) {
//        if (nullRnd.nextFloat() < .7)
//            return null;

        Class<?> type = field.getType();

        if (type.equals(String.class))
            return randomizeString(randomizersMap.get(field));

        else if (type.equals(Double.class))
            return randomizeDouble(randomizersMap.get(field));

        else if (type.equals(BigDecimal.class))
            return randomizeBigDecimal(randomizersMap.get(field));

        else if (type.equals(Date.class))
            return randomizeDate(randomizersMap.get(field));

        else if (type.equals(Integer.class))
            return randomizeInt(randomizersMap.get(field));

        else if (type.equals(Timestamp.class))
            return randomizeTimestamp(randomizersMap.get(field));

        else if (type.equals(Long.class))
            return randomizeLong(randomizersMap.get(field));

        return null;
    }

    public Map<Field, Random> getRandom() {
        return randomizersMap;
    }

    public void setRandom(Map<Field, Random> random) {
        this.randomizersMap = random;
    }

    public int getStrLengthCap() {
        return strLengthCap;
    }

    public void setStrLengthCap(int strLengthCap) {
        this.strLengthCap = strLengthCap;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public void setFieldNames(List<String> fieldNames) {
        this.fieldNames = fieldNames;
    }
}
