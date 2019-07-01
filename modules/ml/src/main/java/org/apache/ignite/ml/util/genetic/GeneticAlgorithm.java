package org.apache.ignite.ml.util.genetic;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;


public class GeneticAlgorithm {
    /** Population size. */
    private int populationSize = 20;

    /** Amount of elite chromosomes. */
    private int amountOfEliteChromosomes = 4;

    /** Amount of generations. */
    private int amountOfGenerations = 10;

    /** Elitism. */
    private boolean elitism = true;

    /** Uniform rate. */
    private double uniformRate = 0.5;

    /** Seed. */
    private long seed = 1234L;

    /** Crossingover probability. */

    private double crossingoverProbability = 0.9;

    /** Mutation probability. */
    private double mutationProbability = 0.1;

    /** Random generator. */
    private Random rnd = new Random(seed);

    /** Population. */
    private Population population;

    /** Fitness function. */
    private Function<Chromosome, Double> fitnessFunction;

    /** Mutation operator. */
    private BiFunction<Integer, Double, Double> mutationOperator;

    private CrossoverStrategy crossoverStrategy = CrossoverStrategy.ONE_POINT;

    public GeneticAlgorithm() {}

    public Population initializePopulation(List<Double[]> rndParamSets) {
        // validate that population size should be even
        // elite chromosome should be even too or we should handle odd case especially

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
            while (stopCriteriaIsReached(i)) {
                Population newPopulation = new Population(populationSize);

                selectEliteChromosomes(newPopulation);

                crossingover(newPopulation);

                mutate(newPopulation);

                // update fitness for new population
                for (int j = amountOfEliteChromosomes; j < populationSize; j++)
                    newPopulation.calculateFitnessForChromosome(j, fitnessFunction);

                population = newPopulation;

                i++;
            }
        }
    }

    private boolean stopCriteriaIsReached(int iterationNumber) {
        return iterationNumber < amountOfGenerations;
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
        // index in this cycle doesn't affect on parent selection
        for (int j = amountOfEliteChromosomes; j < populationSize; j+=2) {
            Chromosome ch1 = tournamentSelection();
            Chromosome ch2 = tournamentSelection();

            if(rnd.nextDouble() < crossingoverProbability) {
                List<Chromosome> twoChildren = crossover(ch1, ch2);
                newPopulation.set(j, twoChildren.get(0));
                newPopulation.set(j+1, twoChildren.get(1));
            } else {
                newPopulation.set(j, ch1);
                newPopulation.set(j+1, ch2);
            }
        }
    }


    /**
     * Applies mutation operator to each chromosome in population with mutation probability.
     *
     * @param newPopulation New population.
     */
    private void mutate(Population newPopulation) {
        for (int j = amountOfEliteChromosomes; j < populationSize; j++) {
            Chromosome possibleMutant = newPopulation.get(j);
            for (int geneIdx = 0; geneIdx < possibleMutant.size(); geneIdx++) {
                if(rnd.nextDouble() < mutationProbability) {
                    Double gene = possibleMutant.getGene(geneIdx);
                    Double newGeneValue = mutationOperator.apply(geneIdx, gene);
                    possibleMutant.set(geneIdx, newGeneValue);
                }
            }
        }
    }


    private List<Chromosome> crossover(Chromosome firstParent, Chromosome secondParent) {
        if (firstParent.size() != secondParent.size())
            throw new RuntimeException("Different length of hyper-parameter vectors!");
        switch (crossoverStrategy) {
            case UNIFORM: return uniformStrategy(firstParent, secondParent);
            case ONE_POINT: return onePointStrategy(firstParent, secondParent);
            default:
                throw new UnsupportedOperationException("This strategy "
                    + crossoverStrategy.name() + " is not supported yet.");

        }

    }

    private List<Chromosome> onePointStrategy(Chromosome firstParent, Chromosome secondParent) {
        int size = firstParent.size();

        Chromosome child1 = new Chromosome(size);
        Chromosome child2 = new Chromosome(size);
        int locusPnt = rnd.nextInt(size);

        for (int i = 0; i < locusPnt; i++) {
            child1.set(i, firstParent.getGene(i));
            child2.set(i, secondParent.getGene(i));
        }


        for (int i = locusPnt; i < size; i++) {
            child1.set(i, secondParent.getGene(i));
            child2.set(i, firstParent.getGene(i));
        }

        List<Chromosome> res = new ArrayList<>();
        res.add(child1);
        res.add(child2);

        return res;
    }

    private List<Chromosome> uniformStrategy(Chromosome firstParent, Chromosome secondParent) {
        int size = firstParent.size();

        Chromosome child1 = new Chromosome(size);
        Chromosome child2 = new Chromosome(size);

        for (int i = 0; i < firstParent.size(); i++) {
            if (rnd.nextDouble() < uniformRate) {
                child1.set(i, firstParent.getGene(i));
                child2.set(i, secondParent.getGene(i));
            }
            else {
                child1.set(i, secondParent.getGene(i));
                child2.set(i, firstParent.getGene(i));
            }
        }

        List<Chromosome> res = new ArrayList<>();
        res.add(child1);
        res.add(child2);

        return res;
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

    public GeneticAlgorithm setMutationOperator(
        BiFunction<Integer, Double, Double> mutationOperator) {
        this.mutationOperator = mutationOperator;
        return this;
    }

}
