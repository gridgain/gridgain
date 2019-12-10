import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;

public class Runner {
    public static void main(String[] args) {
        // Run me with
        // mvn exec:java -D"exec.mainClass"="Runner"
        IgniteConfiguration cfg = new IgniteConfiguration();
        Ignition.start(cfg);
    }
}
