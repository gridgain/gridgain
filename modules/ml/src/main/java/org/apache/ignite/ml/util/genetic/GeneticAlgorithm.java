package org.apache.ignite.ml.util.genetic;

import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Stream;


public class GeneticAlgorithm {
    private int populationSize = 20;
    private int amountOfEliteChromosomes = 4;
    private int amountOfGenerations = 10;
    private boolean elitism = true;
    private double uniformRate = 0.5;
    private Population population;
    private Function<Chromosome, Double> fitnessFunction;

    public GeneticAlgorithm() {}

    public Population initializePopulation(List<Double[]> rndParamSets) {
        populationSize = rndParamSets.size();
        population = new Population(populationSize);
        for (int i = 0; i < populationSize; i++)
            population.set(i, new Chromosome(rndParamSets.get(i)));

        return population;
    }

    public void run() {
        if (population != null) {
            population.calculateFitnessForAll(fitnessFunction);
            int i = 0;
            while (i < amountOfGenerations) {
                Population newPopulation = new Population(populationSize);

                selectEliteChromosomes(newPopulation);

                crossingover(newPopulation);

                mutate();

                // update fitness for new population
                for (int j = amountOfEliteChromosomes; j < populationSize; j++)
                    newPopulation.calculateFitnessForChromosome(j, fitnessFunction);

                population = newPopulation;

                i++;
            }
        }
    }

    private int selectEliteChromosomes(Population newPopulation) {
        int elitismOff = 0;
        if (elitism) {
            Chromosome[] elite = population.selectBestKChromosome(amountOfEliteChromosomes);
            elitismOff = amountOfEliteChromosomes;
            for (int i = 0; i < elite.length; i++)
                newPopulation.set(i, elite[i]);
        }
        return elitismOff;
    }


    private void crossingover(Population newPopulation) {
        for (int j = amountOfEliteChromosomes; j < populationSize; j++) {
            Chromosome ch1 = tournamentSelection();
            Chromosome ch2 = tournamentSelection();
            Chromosome newCh = crossover(ch1, ch2);
            newPopulation.set(j, newCh);
        }
    }

    // fake mutate
    private void mutate() {
        //for (int j = amountOfEliteChromosomes; j < SIZE_OF_POPULATION; j++)
            //mutateOne(newPopulation.get(j));
    }

    private Chromosome crossover(Chromosome firstParent, Chromosome secondParent) {
        if (firstParent.size() != secondParent.size())
            throw new RuntimeException("Different length of hyper-parameter vectors!");
        Chromosome child = new Chromosome(firstParent.size());
        for (int i = 0; i < firstParent.size(); i++) {
            if (Math.random() < uniformRate)
                child.set(i, firstParent.get(i));
            else
                child.set(i, secondParent.get(i));
        }
        return child;
    }

    // fake tournament
    private Chromosome tournamentSelection() {
        Random rnd = new Random(); // TODO: need to seed
        int idx = rnd.nextInt(populationSize);
        return population.get(idx);
    }

    public void setFitnessFunction(Function<Chromosome, Double> fitnessFunction) {
        this.fitnessFunction = fitnessFunction;
    }

    public double[] getTheBestSolution() {
        Double[] boxed = population.selectBestKChromosome(1)[0].toDoubleArray();
        return Stream.of(boxed).mapToDouble(Double::doubleValue).toArray();
    }

    public GeneticAlgorithm setAmountOfEliteChromosomes(int amountOfEliteChromosomes) {
        this.amountOfEliteChromosomes = amountOfEliteChromosomes;
        return this;
    }

    public GeneticAlgorithm setAmountOfGenerations(int amountOfGenerations) {
        this.amountOfGenerations = amountOfGenerations;
        return this;
    }

    public GeneticAlgorithm setElitism(boolean elitism) {
        this.elitism = elitism;
        return this;
    }

    public GeneticAlgorithm setUniformRate(double uniformRate) {
        this.uniformRate = uniformRate;
        return this;
    }
}
