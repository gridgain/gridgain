package org.apache.ignite.internal.processors.tracing;

/**
 * List of tags that can be used to decorate spans.
 */
public class TraceTags {
    /** Tag parts default delimiter. */
    private static final String TAG_PARTS_DELIMITER = ".";

    /**
     * List of basic tags. Can be combined together to get more composite tags.
     * Don't forget to add new tags here and use constant reference instead of raw string creation.
     * Frequently used composite tags can be also declared here.
     */

    /** */
    public static final String NODE = "node";
    /** */
    public static final String ID = "id";
    /** */
    public static final String ORDER = "id";
    /** */
    public static final String EVENT = "event";
    /** */
    public static final String NAME = "name";
    /** */
    public static final String TYPE = "type";
    /** */
    public static final String INITIAL = "initial";
    /** */
    public static final String RESULT = "result";
    /** */
    public static final String ERROR = "error";
    /** */
    public static final String EXCHANGE = "exchange";
    /** */
    public static final String CONSISTENT_ID = "consistent.id";
    /** */
    public static final String TOPOLOGY_VERSION = "topology.version";
    /** */
    public static final String MAJOR = "major";
    /** */
    public static final String MINOR = "minor";
    /** */
    public static final String EVENT_NODE = tag(EVENT, NODE);
    /** */
    public static final String NODE_ID = tag(NODE, ID);

    /** */
    private TraceTags() {};

    /**
     * @param tagParts String parts of composite tag.
     * @return Composite tag with given parts joined using delimiter.
     */
    public static String tag(String... tagParts) {
        return String.join(TAG_PARTS_DELIMITER, tagParts);
    }
}
