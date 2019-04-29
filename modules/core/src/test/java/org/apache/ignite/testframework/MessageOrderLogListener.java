/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.testframework;

import java.util.Iterator;
import org.apache.ignite.internal.util.GridConcurrentLinkedHashSet;

/**
 * Checks the order of the messages in the log.
 */
public class MessageOrderLogListener extends LogListener {
    /** */
    private final MessageGroup matchesGrp;

    /** */
    public MessageOrderLogListener(MessageGroup matchesGrp) {
        this.matchesGrp = matchesGrp;
    }

    /** {@inheritDoc} */
    @Override public boolean check() {
        return matchesGrp.check();
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        matchesGrp.reset();
    }

    /** {@inheritDoc} */
    @Override public void accept(String s) {
        matchesGrp.accept(s);
    }

    /**
     * Allows to unify messages into groups. Messages in non-ordered groups can be analyzed independently
     * from each other.
     */
    public static class MessageGroup extends LogListener {
        /** */
        private final boolean ordered;

        /** */
        private final String containedStr;

        /** */
        private final GridConcurrentLinkedHashSet<MessageGroup> groups;

        /** */
        private final GridConcurrentLinkedHashSet<MessageGroup> matched = new GridConcurrentLinkedHashSet<>();

        /** */
        private volatile boolean checked;

        /** */
        private volatile String actualStr;

        /** */
        private volatile int lastAcceptedIdx;

        /**
         * Default constructor.
         *
         * @param ordered whether to check order of messages in this group.
         */
        public MessageGroup(boolean ordered) {
            this(ordered, null);
        }

        /** */
        private MessageGroup(boolean ordered, String s) {
            this.ordered = ordered;

            containedStr = s;

            groups = s == null ? new GridConcurrentLinkedHashSet<>() : null;
        }

        /** */
        public MessageGroup add(String s) {
            return add(new MessageGroup(false, s));
        }

        /** */
        public MessageGroup add(MessageGroup grp) {
            groups.add(grp);

            return this;
        }

        /** {@inheritDoc} */
        @Override public boolean check() {
            if (checked)
                return true;

            if (groups != null) {
                for (MessageGroup group : groups) {
                    if (!matched.contains(group) || !group.check())
                        return false;
                }

                return true;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            checked = false;

            actualStr = null;

            lastAcceptedIdx = 0;

            if (groups != null) {
                for (MessageGroup group : groups)
                    group.reset();
            }

            for (Iterator<MessageGroup> iter = matched.iterator(); iter.hasNext();) {
                iter.next();

                iter.remove();
            }
        }

        /** {@inheritDoc} */
        @Override public void accept(String s) {
            internalAccept(s);
        }

        /** */
        private boolean internalAccept(String s) {
            if (checked)
                return false;
            else if (containedStr != null && s.matches(containedStr)) {
                checked = true;

                actualStr = s;

                return true;
            }
            else {
                if (groups != null) {
                    int i = 0;

                    for (MessageGroup group : groups) {
                        if (i < lastAcceptedIdx && ordered) {
                            i++;

                            continue;
                        }

                        if (group.internalAccept(s)) {
                            matched.add(group);

                            lastAcceptedIdx = i;

                            return true;
                        }

                        i++;
                    }
                }

                return false;
            }
        }

        /** */
        public String getActualStr() {
            return actualStr;
        }
    }
}
