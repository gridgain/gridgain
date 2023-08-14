package org.apache.ignite.ml.tree.randomforest;

public enum WfDeltaLabel {

    PORTFOLIO_CHANGE("Portfolio_Change"),

    COLLATERAL_CHANGE("Collateral_Change"),

    MARKET_MOVE("Market_Move"),

    DYNAMIC_IM("Dynamic_IM"),

    CALL_MOVE("Call_Move"),

    KO("KO"),

    KO_REVERTING("KO_reverting");

    private final String label;

    WfDeltaLabel(String label) {
        this.label = label;
    }

    public String getLabel() {
        return label;
    }

}
