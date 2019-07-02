package org.apache.ignite.ml.util.genetic;

import java.util.Arrays;
import java.util.function.Function;

public class Population {
    private Chromosome[] chromosomes;

    public Population(int size) {
        this.chromosomes = new Chromosome[size];
    }

    public Chromosome getChromosome(int idx){
        return chromosomes[idx];
    }

    public double calculateFitnessForChromosome(int idx, Function<Chromosome, Double> fitnessFunction){
        double fitness = fitnessFunction.apply(chromosomes[idx]);
        chromosomes[idx].setFitness(fitness);
        return fitness;
    }


    public void calculateFitnessForAll(Function<Chromosome, Double> fitnessFunction) {
        for (int i = 0; i < chromosomes.length; i++)
            calculateFitnessForChromosome(i, fitnessFunction);
    }

    public void set(int idx, Chromosome chromosome) {
        chromosomes[idx] = chromosome;
    }

    public Chromosome get(Integer idx) {
        return chromosomes[idx];
    }

    public Chromosome[] selectBestKChromosome(int k) {
        Chromosome[] cp = Arrays.copyOf(chromosomes, chromosomes.length);
        Arrays.sort(cp);
        return Arrays.copyOfRange(cp, cp.length - k, cp.length);
    }

    public double getTotalFitness() {
        double totalFitness = 0.0;

        for (int i = 0; i < chromosomes.length; i++)
            totalFitness+=chromosomes[i].getFitness();

        return totalFitness;
    }

    public double getAverageFitness() {
        double totalFitness = 0.0;

        for (int i = 0; i < chromosomes.length; i++)
            totalFitness+=chromosomes[i].getFitness();

        return totalFitness/chromosomes.length;
    }

    public int size() {
        return chromosomes.length;
    }
}
