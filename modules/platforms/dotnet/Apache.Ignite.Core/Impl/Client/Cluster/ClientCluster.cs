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

#nullable disable

namespace Apache.Ignite.Core.Impl.Client.Cluster
{
    using System;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Ignite client cluster implementation.
    /// </summary>
    internal class ClientCluster : ClientClusterGroup, IClientCluster
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="ignite">Ignite.</param>
        public ClientCluster(IgniteClient ignite)
            : base(ignite)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public void SetActive(bool isActive)
        {
            DoOutInOp<object>(ClientOp.ClusterChangeState, ctx => ctx.Stream.WriteBool(isActive), null);
        }

        /** <inheritdoc /> */
        public Task SetActiveAsync(bool isActive)
        {
            return DoOutInOpAsync<object>(ClientOp.ClusterChangeState, ctx => ctx.Stream.WriteBool(isActive), null);
        }

        /** <inheritdoc /> */
        public bool IsActive()
        {
            return DoOutInOp(ClientOp.ClusterIsActive, null, ctx => ctx.Stream.ReadBool());
        }

        /** <inheritdoc /> */
        public Task<bool> IsActiveAsync()
        {
            return DoOutInOpAsync(ClientOp.ClusterIsActive, null, ctx => ctx.Stream.ReadBool());
        }

        /** <inheritdoc /> */
        public bool DisableWal(string cacheName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(cacheName, "cacheName");

            return DoOutInOp(ClientOp.ClusterChangeWalState, ctx => WriteWalState(ctx, cacheName, false),
                ctx => ctx.Stream.ReadBool());
        }

        /** <inheritdoc /> */
        public Task<bool> DisableWalAsync(string cacheName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(cacheName, "cacheName");

            return DoOutInOpAsync(ClientOp.ClusterChangeWalState, ctx => WriteWalState(ctx, cacheName, false),
                ctx => ctx.Stream.ReadBool());
        }

        /** <inheritdoc /> */
        public bool EnableWal(string cacheName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(cacheName, "cacheName");

            return DoOutInOp(ClientOp.ClusterChangeWalState, ctx => WriteWalState(ctx, cacheName, true),
                ctx => ctx.Stream.ReadBool());
        }

        /** <inheritdoc /> */
        public Task<bool> EnableWalAsync(string cacheName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(cacheName, "cacheName");

            return DoOutInOpAsync(ClientOp.ClusterChangeWalState, ctx => WriteWalState(ctx, cacheName, true),
                ctx => ctx.Stream.ReadBool());
        }

        /** <inheritdoc /> */
        public bool IsWalEnabled(string cacheName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(cacheName, "cacheName");

            return DoOutInOp(ClientOp.ClusterGetWalState, ctx => ctx.Writer.WriteString(cacheName), ctx => ctx.Stream.ReadBool());
        }

        /** <inheritdoc /> */
        public Task<bool> IsWalEnabledAsync(string cacheName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(cacheName, "cacheName");

            return DoOutInOpAsync(ClientOp.ClusterGetWalState, ctx => ctx.Writer.WriteString(cacheName),
                ctx => ctx.Stream.ReadBool());
        }

        /// <summary>
        /// Writes the change WAL state request.
        /// </summary>
        private static void WriteWalState(ClientRequestContext ctx, string cacheName, bool enable)
        {
            ctx.Writer.WriteString(cacheName);
            ctx.Writer.WriteBoolean(enable);
        }
    }
}
