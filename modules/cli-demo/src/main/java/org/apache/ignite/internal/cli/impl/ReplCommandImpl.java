/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.cli.impl;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.ignite.internal.cli.ReplCommandSpec;
import org.fusesource.jansi.AnsiConsole;
import org.jline.console.SystemRegistry;
import org.jline.console.impl.SystemRegistryImpl;
import org.jline.keymap.KeyMap;
import org.jline.reader.Binding;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.MaskingCallback;
import org.jline.reader.Parser;
import org.jline.reader.Reference;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.widget.TailTipWidgets;
import picocli.CommandLine;
import picocli.shell.jline3.PicocliCommands;

/**
 *
 */
public class ReplCommandImpl extends ReplCommandSpec {
    /** */
    private CommandFactory factory;

    public ReplCommandImpl(CommandFactory factory) {
        this.factory = factory;
    }

    private static Path workDir() {
        return Paths.get(System.getProperty("user.dir"));
    }

    /** {@inheritDoc} */
    @Override public void run() {
        AnsiConsole.systemInstall();

        try {
            try (Terminal terminal = TerminalBuilder.builder().build()) {
                factory.setWriter(terminal.writer());

                // set up picocli commands
                CommandLine cmd = new CommandLine(new InteractiveCommandsImpl(terminal.writer()), factory);

                PicocliCommands picocliCommands = new PicocliCommands(ReplCommandImpl::workDir, cmd) {
                    @Override public Object invoke(CommandSession ses, String cmd, Object... args) throws Exception {
                        return execute(ses, cmd, (String[])args);
                    }
                };

                Parser parser = new DefaultParser();

                SystemRegistry sysReg = new SystemRegistryImpl(parser, terminal, ReplCommandImpl::workDir, null);

                sysReg.setCommandRegistries(picocliCommands);

                LineReader reader = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .completer(sysReg.completer())
                    .parser(parser)
                    .variable(LineReader.LIST_MAX, 50)   // max tab completion candidates
                    .build();

                TailTipWidgets widgets = new TailTipWidgets(
                    reader,
                    sysReg::commandDescription,
                    5,
                    TailTipWidgets.TipType.COMPLETER);

                widgets.enable();
                KeyMap<Binding> keyMap = reader.getKeyMaps().get("main");
                keyMap.bind(new Reference("tailtip-toggle"), KeyMap.alt("s"));

                String prompt = "ignitectl> ";
                String rightPrompt = null;

                // start the shell and process input until the user quits with Ctrl-D
                String line;

                while (true) {
                    try {
                        sysReg.cleanUp();

                        line = reader.readLine(prompt, rightPrompt, (MaskingCallback)null, null);

                        sysReg.execute(line);
                    }
                    catch (UserInterruptException ignore) {
                        // Ignore
                    }
                    catch (EndOfFileException e) {
                        return;
                    }
                    catch (Exception e) {
                        sysReg.trace(e);
                    }
                }
            }
        }
        catch (Throwable t) {
            t.printStackTrace();
        }
        finally {
            AnsiConsole.systemUninstall();
        }
    }
}
