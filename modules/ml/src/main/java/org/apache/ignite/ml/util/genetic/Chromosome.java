package org.apache.ignite.ml.util.genetic;

import org.jetbrains.annotations.NotNull;

public class Chromosome implements Comparable {
    private Double[] genes;
    private Double fitness = Double.NaN;

    public Chromosome(int size) {
        this.genes = new Double[size];
    }

    public Chromosome(Double[] doubles) {
        genes = doubles;
    }

    public Double[] toDoubleArray(){
        return genes;
    }

    public Double getFitness() {
        return fitness;
    }

    public void setFitness(Double fitness) {
        this.fitness = fitness;
    }

    public int size() {
        return genes.length;
    }

    public double getGene(int i) {
        return genes[i];
    }

    public void set(int i, double v) {
        genes[i] = v;
    }

    /**
     * Creates chromosome copy.
     */
    public Chromosome copy(){
        Chromosome copy = new Chromosome(genes);
        copy.fitness = fitness;
        return copy;
    }

    @Override public int compareTo(@NotNull Object o) {
        double delta = getFitness() - ((Chromosome)o).getFitness();
        if (delta > 0) return 1;
        else if (delta == 0) return 0;
        else return -1;
    }
}
