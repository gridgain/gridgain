/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.rest.handlers.property;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedChangeableProperty;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandlerAdapter;
import org.apache.ignite.internal.processors.rest.request.GridRestPropertyRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.PROPERTY_GET;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.PROPERTY_LIST;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.PROPERTY_SET;

/**
 * REST command handler for distributed property management.
 * Supports {@link GridRestCommand#PROPERTY_LIST}, {@link GridRestCommand#PROPERTY_GET},
 * {@link GridRestCommand#PROPERTY_SET}.
 */
public class GridPropertyCommandHandler extends GridRestCommandHandlerAdapter {
    /** Supported commands. */
    private static final Collection<GridRestCommand> SUPPORTED_COMMANDS =
        U.sealList(PROPERTY_LIST, PROPERTY_GET, PROPERTY_SET);

    /**
     * @param ctx Kernal context.
     */
    public GridPropertyCommandHandler(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRestCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRestResponse> handleAsync(GridRestRequest req) {
        assert req instanceof GridRestPropertyRequest : req;

        GridRestPropertyRequest req0 = (GridRestPropertyRequest)req;
        GridRestResponse res;

        try {
            res = new GridRestResponse();

            switch (req0.command()) {
                case PROPERTY_LIST:
                    res.setResponse(listProperties());

                    break;

                case PROPERTY_GET:
                    res.setResponse(getProperty(req0));

                    break;

                case PROPERTY_SET:
                    res.setResponse(setProperty(req0));

                    break;

                default:
                    throw new IgniteCheckedException("Unsupported command: " + req0.command());
            }
        }
        catch (Exception e) {
            res = new GridRestResponse(GridRestResponse.STATUS_FAILED, errorMessage(e));
        }

        return new GridFinishedFuture<>(res);
    }

    /** @return Descriptors of every registered distributed property. */
    private List<GridPropertyCommandResponse> listProperties() {
        return ctx.distributedConfiguration().properties().stream()
            .map(GridPropertyCommandHandler::toDto)
            .collect(Collectors.toList());
    }

    /**
     * @return Descriptor for the named property.
     * @throws IgniteCheckedException If {@code name} is missing or the property is not registered.
     */
    private GridPropertyCommandResponse getProperty(GridRestPropertyRequest req) throws IgniteCheckedException {
        return toDto(requireProperty(req.name()));
    }

    /**
     * Parse and propagate a new value cluster-wide for the named property.
     *
     * @return Descriptor reflecting the value the cluster now holds.
     * @throws IgniteCheckedException If a parameter is missing, the property is unknown,
     *         the value fails to parse, or propagation fails.
     */
    private GridPropertyCommandResponse setProperty(GridRestPropertyRequest req) throws IgniteCheckedException {
        if (req.value() == null)
            throw new IgniteCheckedException(missingParameter("val"));

        DistributedChangeableProperty<Serializable> prop = requireProperty(req.name());

        prop.propagate(prop.parse(req.value()));

        return toDto(prop);
    }

    /**
     * Look up a property by name, throwing a clear error if missing or unknown.
     *
     * @param name Property name (may be null).
     * @return The property; never null on return.
     * @throws IgniteCheckedException If {@code name} is null or the property is not registered.
     */
    private DistributedChangeableProperty<Serializable> requireProperty(String name) throws IgniteCheckedException {
        if (name == null)
            throw new IgniteCheckedException(missingParameter("name"));

        DistributedChangeableProperty<Serializable> prop = ctx.distributedConfiguration().property(name);

        if (prop == null)
            throw new IgniteCheckedException("Unknown distributed property: " + name);

        return prop;
    }

    /** Build a response DTO from a property. */
    private static GridPropertyCommandResponse toDto(DistributedChangeableProperty<?> prop) {
        GridPropertyCommandResponse dto = new GridPropertyCommandResponse();

        dto.setName(prop.getName());

        Object val = prop.get();

        dto.setValue(val == null ? null : val.toString());
        dto.setType(val == null ? null : val.getClass().getSimpleName());

        return dto;
    }
}
