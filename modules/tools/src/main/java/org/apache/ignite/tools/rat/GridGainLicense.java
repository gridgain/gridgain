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

package org.apache.ignite.tools.rat;

import java.util.regex.Pattern;
import org.apache.rat.analysis.RatHeaderAnalysisException;
import org.apache.rat.analysis.license.FullTextMatchingLicense;
import org.apache.rat.api.Document;
import org.apache.rat.api.MetaData;

/**
 * Looks for documents contain the OASIS copyright claim plus derivative work clause.
 * Perhaps need to match more.
 */
public class GridGainLicense extends FullTextMatchingLicense {
    /** */
    private Pattern copyrightPattern;

    /** */
    private boolean copyrightMatch;

    /**
     *
     */
    public GridGainLicense() {
        super(MetaData.RAT_LICENSE_FAMILY_CATEGORY_DATUM_ASL,
            new MetaData.Datum("http://org/apache/rat/meta-data#LicenseFamilyName", "Ignite Apache License 2.0"),
            "No modifications allowed",
            "");
    }

    /**
     * Sets copyrigth regexp.
     *
     * @param copyright Copyright regexp.
     */
    public void setCopyright(String copyright) {
        copyrightPattern = Pattern.compile(copyright);
    }

    /** {@inheritDoc} */
    @Override public boolean match(Document subject, String line) throws RatHeaderAnalysisException {
        if (copyrightPattern == null)
            return super.match(subject, line);
        else {
            boolean res = false;

            if (copyrightMatch)
                res = super.match(subject, line);
            else
                copyrightMatch = copyrightPattern.matcher(line).matches();

            return res;
        }
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        copyrightMatch = false;
        super.reset();
    }
}
