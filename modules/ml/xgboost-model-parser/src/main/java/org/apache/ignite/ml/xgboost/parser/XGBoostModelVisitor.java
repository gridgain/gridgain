/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.ml.xgboost.parser;

import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link XGBoostModelParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface XGBoostModelVisitor<T> extends ParseTreeVisitor<T> {
    /**
     * Visit a parse tree produced by {@link XGBoostModelParser#xgValue}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    public T visitXgValue(XGBoostModelParser.XgValueContext ctx);

    /**
     * Visit a parse tree produced by {@link XGBoostModelParser#xgHeader}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    public T visitXgHeader(XGBoostModelParser.XgHeaderContext ctx);

    /**
     * Visit a parse tree produced by {@link XGBoostModelParser#xgNode}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    public T visitXgNode(XGBoostModelParser.XgNodeContext ctx);

    /**
     * Visit a parse tree produced by {@link XGBoostModelParser#xgLeaf}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    public T visitXgLeaf(XGBoostModelParser.XgLeafContext ctx);

    /**
     * Visit a parse tree produced by {@link XGBoostModelParser#xgTree}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    public T visitXgTree(XGBoostModelParser.XgTreeContext ctx);

    /**
     * Visit a parse tree produced by {@link XGBoostModelParser#xgModel}.
     * @param ctx the parse tree
     * @return the visitor result
     */
    public T visitXgModel(XGBoostModelParser.XgModelContext ctx);
}