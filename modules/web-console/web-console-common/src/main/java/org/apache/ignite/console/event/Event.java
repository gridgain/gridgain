package org.apache.ignite.console.event;

import org.springframework.context.ApplicationEvent;

/**
 * Generic application event with payload
 */
public class Event<T> extends ApplicationEvent {
    /** Type. */
    private Type type;

    /** Payload. */
    private T payload;

    /**
     * @param type Type.
     * @param payload Payload.
     */
    public Event(Type type, T payload) {
        super(new Object());

        this.type = type;
        this.payload = payload;
    }

    /**
     * @return Type.
     */
    public Type getType() {
        return type;
    }

    /**
     * @return Payload.
     */
    public T getPayload() {
        return payload;
    }

    /**
     * Event type
     */
    public enum Type {
        /** */
        ACCOUNT_CREATE,

        /** */
        ACCOUNT_CREATE_BY_ADMIN,

        /** */
        ACCOUNT_UPDATE,

        /** */
        ACCOUNT_DELETE,

        /** */
        PASSWORD_RESET,

        /** */
        PASSWORD_CHANGED,

        /** */
        RESET_ACTIVATION_TOKEN,

        /** */
        ACTIVITY_UPDATE
    }
}
