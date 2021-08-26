package org.apache.hadoop.fs.obs.input;

import org.apache.hadoop.fs.obs.OBSConstants;

/**
 * Set of classes to support output streaming into blocks which are then
 * uploaded as to OBS as a single PUT, or as part of a multipart request.
 */
public final class InputPolicys {

    /**
     * Create a factory.
     *
     * @param name factory name -the option from {@link OBSConstants}.
     * @return the factory, ready to be initialized.
     * @throws IllegalArgumentException if the name is unknown.
     */
    public static InputPolicyFactory createFactory(final String name) {
        switch (name) {
            case OBSConstants.READAHEAD_POLICY_PRIMARY:
                return new BasicInputPolicyFactory();
            case OBSConstants.READAHEAD_POLICY_ADVANCE:
                return new ExtendInputPolicyFactory();
            default:
                throw new IllegalArgumentException("Unsupported block buffer" + " \"" + name + '"');
        }
    }
}
