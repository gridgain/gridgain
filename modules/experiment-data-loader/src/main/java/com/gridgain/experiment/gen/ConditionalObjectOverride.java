package com.gridgain.experiment.gen;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ConditionalObjectOverride {

    private Map<String, ArrayList<String>> predefinedValuesSet = new HashMap<>();

    private Map<String, Integer> columnSelectivity = new HashMap<>();

    private Map<String, String> columnAliases = new HashMap<>();

    public void addJoinField(String col1, String col2, ArrayList<String> values) {
        predefinedValuesSet.put(col1+"!"+col2, values);
    }

    public void addFieldAlias(String col1, String col2) {
        columnAliases.put(col1, col2);
    }

    public void setWhereClauseConditionOnColumn(String column, ArrayList<String> predefinedValues, Integer selectivity) {
        if ( predefinedValues.size() < selectivity) {
            for (int i = predefinedValues.size(); i < selectivity; i ++)
                predefinedValues.add(column.toUpperCase() + String.valueOf(i));
        }

        columnSelectivity.put(column, selectivity);
        predefinedValuesSet.put(column, predefinedValues);
    }

    public ArrayList<String> getPredefinedValues(String column) {
        return predefinedValuesSet.get(column);
    }

    public boolean mustOverride(String column) {
        String alias = getAlias(column);
        return predefinedValuesSet.containsKey(alias);
    }

    public String lookup(String column, int idx) {
        String alias = getAlias(column);
        int index = idx % columnSelectivity.get(alias);
        if ( index >= predefinedValuesSet.get(alias).size())
            return "fsffgsfs";
        return predefinedValuesSet.get(alias).get(index);
    }

    private String getAlias(String column) {
        String alias = null;
        if ( columnAliases.containsKey(column) )
            return columnAliases.get(column);
        if ( predefinedValuesSet.containsKey(column))
            return column;
        return alias;
    }
}
