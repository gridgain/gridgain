package org.gridgain.action;

import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertTrue;

/**
 * Action controller annotation processor.
 */
public class ActionControllerAnnotationProcessorTest {
    /**
     * Should find the tests action controllers.
     */
    @Test
    public void findActionMethods() {
        Map<String, ActionMethod> methods = ActionControllerAnnotationProcessor.findActionMethods("org.gridgain.action");

        assertTrue(methods.containsKey("TestActionController.action"));
        assertTrue(methods.containsKey("TestAction.action"));
    }

    @ActionController
    private static class TestActionController {
        public static CompletableFuture<Boolean> action(boolean flag) {
            return CompletableFuture.completedFuture(flag);
        }
    }

    @ActionController("TestAction")
    private static class TestActionControllerWithName {
        public static CompletableFuture<Boolean> action(boolean flag) {
            return CompletableFuture.completedFuture(flag);
        }
    }
}