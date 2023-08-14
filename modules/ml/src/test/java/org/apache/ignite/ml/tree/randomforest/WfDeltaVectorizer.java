package org.apache.ignite.ml.tree.randomforest;

import static java.util.stream.Collectors.toSet;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.springframework.util.ReflectionUtils;

public class WfDeltaVectorizer extends Vectorizer<Object, WfDelta, String, Double> {

    private static final Set<String> LABELS = Stream.of(WfDeltaLabel.values())
            .map(WfDeltaLabel::getLabel)
            .collect(toSet());

    private static final Set<String> EXCLUDED_FIELDS = new HashSet<>(Arrays.asList(
            "ReportingPointKeyId", "Date1", "Date2", "Key", "Prod", "CpartyName", "ReportingPointSDSID", "LEid",
            "comment", "root", "Reason", "CSP", "Live_PFE_Tenor", "sReg_PFE_Tenor", "uReg_PFE_Tenor",
            "CreditSanctionTeamName", "RAG_tolerance", "DQ"
    ));

    public WfDeltaVectorizer(WfDeltaLabel label) {
        super(features());
        labeled(label.getLabel());
    }

    public static String[] features() {
        return Stream.of(WfDelta.class.getDeclaredFields())
                .map(Field::getName)
                .filter(field -> !EXCLUDED_FIELDS.contains(field) && !LABELS.contains(field))
                .toArray(String[]::new);
    }

    @Override
    protected Double feature(String name, Object key, WfDelta value) {
        return (Double) field(name, value);
    }

    @Override
    protected Double label(String name, Object key, WfDelta value) {
        return (Double) field(name, value);
    }

    private static Object field(String name, WfDelta value) {
        Field field = ReflectionUtils.findField(value.getClass(), name);
        field.setAccessible(true);
        return ReflectionUtils.getField(field, value);
    }

    @Override
    protected Double zero() {
        throw new RuntimeException("Should never be called");
    }

    @Override
    protected List<String> allCoords(Object key, WfDelta value) {
        throw new RuntimeException("Should never be called");
    }
}
