package org.apache.ignite;

import org.apache.ignite.internal.util.typedef.G;

public class StartServer {
    public static void main(String[] args) {
        G.start("modules/spring/server_base.xml");
    }
}
