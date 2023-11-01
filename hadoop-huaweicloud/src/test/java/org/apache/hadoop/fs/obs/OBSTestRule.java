package org.apache.hadoop.fs.obs;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class OBSTestRule implements TestRule {
    @Override
    public Statement apply(final Statement statement,
        final Description description) {
        return new OBSStatement(statement, description);
    }

    class OBSStatement extends Statement {
        private final Statement base;
        private final Description description;

        public OBSStatement(Statement base, Description description) {
            this.base = base;
            this.description = description;
        }

        @Override
        public void evaluate() throws Throwable {
            String methodName = description.getMethodName();
            String beforeMsg = String.format("Begin to run testcase %s",
                methodName);
            String afterMsg = String.format("Finish run testcase %s success",
                methodName);
            System.out.println(beforeMsg);
            base.evaluate();
            System.out.println(afterMsg);
        }
    }
}
