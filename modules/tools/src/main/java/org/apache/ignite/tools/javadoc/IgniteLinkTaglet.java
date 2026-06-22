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

package org.apache.ignite.tools.javadoc;

import com.sun.source.doctree.DocTree;
import com.sun.source.doctree.TextTree;
import com.sun.source.doctree.UnknownInlineTagTree;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.lang.model.element.Element;
import javax.lang.model.element.PackageElement;

/**
 * Represents {@ignitelink Class} tag. This tag can
 * be used as replacement of {@link Class} tag that references to the Ignite class that is not in classpath.
 * Class and its arguments should have fully qualified names.
 */
@SuppressWarnings("Since15")
public class IgniteLinkTaglet implements jdk.javadoc.doclet.Taglet {
    /** */
    private static final String NAME = "ignitelink";

    /**
     * Return the name of this custom tag.
     */
    @Override public String getName() {
        return NAME;
    }

    /**
     * Given the {@link DocTree} representation of this custom tag, return its HTML representation.
     * <p>
     * Input: org.apache.ignite.grid.spi.indexing.h2.GridH2IndexingSpi#setIndexCustomFunctionClasses(Class[])
     * <p>
     * Output: &lt;a href="../../../../../org/apache/ignite/grid/spi/indexing/h2/GridH2IndexingSpi.html#
     * setIndexCustomFunctionClasses(java.lang.Class...)"&gt;
     * &lt;code&gt;GridH2IndexingSpi.setIndexCustomFunctionClasses(java.lang.Class[])&lt;/code&gt;&lt;/a&gt;
     *
     * @param tags Tags to render.
     * @param element Documented element; its package defines the relative path depth.
     */
    @Override public String toString(List<? extends DocTree> tags, Element element) {
        StringBuilder sb = new StringBuilder();

        for (DocTree tag : tags) {
            if (!(tag instanceof UnknownInlineTagTree))
                continue;

            String text = tagText((UnknownInlineTagTree)tag);

            if (text.isEmpty())
                continue;

            // Relative path from the documented element's package up to the javadoc root.
            StringBuilder path = new StringBuilder();

            String pkg = enclosingPackage(element);

            if (!pkg.isEmpty()) {
                for (int i = 0, n = pkg.split("\\.").length; i < n; i++)
                    path.append("../");
            }

            String[] tokens = text.split("#");

            int lastIdx = tokens[0].lastIndexOf('.');

            String simpleClsName = lastIdx != -1 && lastIdx + 1 < tokens[0].length() ?
                tokens[0].substring(lastIdx + 1) : tokens[0];

            String fullyQClsName = tokens[0].replace(".", "/");

            sb.append("<a href=\"").append(path).append(fullyQClsName).append(".html")
                .append(tokens.length > 1 ? ("#" + tokens[1].replace("[]", "...")) : "")
                .append("\"><code>").append(simpleClsName).append(tokens.length > 1 ? ("." + tokens[1]) : "")
                .append("</code></a>");
        }

        return sb.toString();
    }

    /**
     * @param tag Inline tag.
     * @return Concatenated text content of the tag.
     */
    private static String tagText(UnknownInlineTagTree tag) {
        StringBuilder sb = new StringBuilder();

        for (DocTree content : tag.getContent()) {
            if (content instanceof TextTree)
                sb.append(((TextTree)content).getBody());
            else
                sb.append(content);
        }

        return sb.toString().trim();
    }

    /**
     * @param element Documented element.
     * @return Qualified name of the element's package, or an empty string.
     */
    private static String enclosingPackage(Element element) {
        for (Element e = element; e != null; e = e.getEnclosingElement()) {
            if (e instanceof PackageElement)
                return ((PackageElement)e).getQualifiedName().toString();
        }

        return "";
    }

    @Override public Set<Location> getAllowedLocations() {
        return EnumSet.allOf(Location.class);
    }

    /**
     * Will return true since this is an inline tag.
     *
     * @return true since this is an inline tag.
     */
    @Override public boolean isInlineTag() {
        return true;
    }

    /**
     * Register this Taglet.
     *
     * @param tagletMap the map to register this tag to.
     */
    public static void register(Map<String, IgniteLinkTaglet> tagletMap) {
        IgniteLinkTaglet tag = new IgniteLinkTaglet();

        jdk.javadoc.doclet.Taglet t = tagletMap.get(tag.getName());

        if (t != null)
            tagletMap.remove(tag.getName());

        tagletMap.put(tag.getName(), tag);
    }
}