package org.apache.ignite.ml.util.genetic;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;


public class GeneticAlgorithm {
    /** Population size. */
    private int populationSize = 50;

    /** Amount of elite chromosomes. */
    private int amountOfEliteChromosomes = 2;

    /** Amount of generations. */
    private int amountOfGenerations = 10;

    /** Elitism. */
    private boolean elitism = true;

    /** Uniform rate. */
    private double uniformRate = 0.5;

    /** Seed. */
    private long seed = 123L;

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

    /** Crossover strategy. */
    private CrossoverStrategy crossoverStrategy = CrossoverStrategy.UNIFORM;

    /** Selection strategy. */
    private SelectionStrategy selectionStrategy = SelectionStrategy.ROULETTE_WHEEL;

    /**
     * Forms the initial population.
     *
     * @param rawDataForPopulationFormation Rnd parameter sets.
     */
    public Population initializePopulation(List<Double[]> rawDataForPopulationFormation) {
        // validate that population size should be even
        // elite chromosome should be even too or we should handle odd case especially

        populationSize = rawDataForPopulationFormation.size();
        population = new Population(populationSize);
        for (int i = 0; i < populationSize; i++)
            population.set(i, new Chromosome(rawDataForPopulationFormation.get(i)));

        return population;
    }

    /**
     * The main method for genetic algorithm.
     */
    public void run() {
        if (population != null) {
            population.calculateFitnessForAll(fitnessFunction);
            int i = 0;
            while (stopCriteriaIsReached(i)) {
                Population newPopulation = new Population(populationSize);

                newPopulation = selectEliteChromosomes(newPopulation);

                Population parents = selectionParents();

                newPopulation = crossingover(parents, newPopulation);

                newPopulation = mutate(newPopulation);

                // update fitness for new population
                for (int j = amountOfEliteChromosomes; j < populationSize; j++)
                    newPopulation.calculateFitnessForChromosome(j, fitnessFunction);

                population = newPopulation;

                i++;
            }
        }
    }

    /**
     * The common method of parent population building with different selection strategies.
     */
    private Population selectionParents() {
        switch (selectionStrategy) {
            case ROULETTE_WHEEL: return selectParentsByRouletteWheel();
            default:
                throw new UnsupportedOperationException("This strategy "
                    + selectionStrategy.name() + " is not supported yet.");
        }
    }

    /**
     * Form the parent population via wheel-roulette algorithm.
     * For more information, please have a look http://www.edc.ncl.ac.uk/highlight/rhjanuary2007g02.php/.
     */
    private Population selectParentsByRouletteWheel() {
        double totalFitness = population.getTotalFitness();
        double[] sectors = new double[population.size()];

        for (int i = 0; i < population.size(); i++)
            sectors[i] = population.getChromosome(i).getFitness()/totalFitness;

        Population parentPopulation = new Population(population.size());

        for (int i = 0; i < parentPopulation.size(); i++) {
            double rouletteVal = rnd.nextDouble();
            double accumulatedSectorLen = 0.0;
            int selectedChromosomeIdx = Integer.MIN_VALUE;
            int sectorIdx = 0;

            while (selectedChromosomeIdx == Integer.MIN_VALUE && sectorIdx < sectors.length) {
                accumulatedSectorLen += sectors[sectorIdx];
                if(rouletteVal < accumulatedSectorLen)
                    selectedChromosomeIdx = sectorIdx;
                sectorIdx++;
            }

            if(selectedChromosomeIdx == Integer.MIN_VALUE)
                selectedChromosomeIdx = rnd.nextInt(population.size()); // or get last

            parentPopulation.set(i, population.getChromosome(selectedChromosomeIdx));
        }

        return parentPopulation;
    }

    /**
     * Simple stop criteria condition based on max amount of generations.
     *
     * @param iterationNum Iteration number.
     */
    private boolean stopCriteriaIsReached(int iterationNum) {
        return iterationNum < amountOfGenerations;
    }

    /**
     * Selects and injects the best chromosomes to form the elite of new population.
     *
     * @param newPopulation New population.
     */
    private Population selectEliteChromosomes(Population newPopulation) {
        if (elitism) {
            Chromosome[] elite = population.selectBestKChromosome(amountOfEliteChromosomes);
            for (int i = 0; i < elite.length; i++)
                newPopulation.set(i, elite[i]);
        }
        return newPopulation;
    }

    private Population crossingover(Population parents, Population newPopulation) {
        // because parent population is less than new population on amount of elite chromosome
        for (int j = 0; j < populationSize - amountOfEliteChromosomes; j+=2) {
            Chromosome ch1 = parents.get(j);
            Chromosome ch2 = parents.get(j + 1);

            if(rnd.nextDouble() < crossingoverProbability) {
                List<Chromosome> twoChildren = crossover(ch1, ch2);
                newPopulation.set(amountOfEliteChromosomes + j, twoChildren.get(0));
                newPopulation.set(amountOfEliteChromosomes + j + 1, twoChildren.get(1));
            } else {
                newPopulation.set(amountOfEliteChromosomes + j, ch1);
                newPopulation.set(amountOfEliteChromosomes + j + 1, ch2);
            }
        }
        return newPopulation;
    }


    /**
     * Applies mutation operator to each chromosome in population with mutation probability.
     *
     * @param newPopulation New population.
     */
    private Population mutate(Population newPopulation) {
        for (int j = amountOfEliteChromosomes; j < populationSize; j++) {
            Chromosome possibleMutant = newPopulation.get(j);
            for (int geneIdx = 0; geneIdx < possibleMutant.size(); geneIdx++) {
                if(rnd.nextDouble() < mutationProbability) {
                    Double gene = possibleMutant.getGene(geneIdx);
                    Double newGeneVal = mutationOperator.apply(geneIdx, gene);
                    possibleMutant.set(geneIdx, newGeneVal);
                }
            }
        }
        return newPopulation;
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
