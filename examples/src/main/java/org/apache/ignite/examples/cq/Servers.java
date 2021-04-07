package org.apache.ignite.examples.cq;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

public class Servers {
    public static void main(String[] args) {
        Ignite i1 = Ignition.start(Cnf.getCfg("I1", false));
        Ignite i2 = Ignition.start(Cnf.getCfg("I2", false));
    }
}
