/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.gridgain.internal.h2.jdbc;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;

import javax.xml.transform.Result;
import javax.xml.transform.Source;

import org.gridgain.internal.h2.message.TraceObject;
import org.gridgain.internal.h2.value.Value;

/**
 * Represents a SQLXML value.
 *
 * <p>The XML-processing entry points ({@link #getSource(Class)} and
 * {@link #setResult(Class)}) are replaced with throw-stubs. The
 * original H2 implementation parsed and transformed user-supplied XML
 * through unhardened {@link javax.xml.parsers.DocumentBuilderFactory},
 * {@link javax.xml.stream.XMLInputFactory}, and
 * {@link javax.xml.transform.TransformerFactory} instances, which is
 * an XXE attack surface. GridGain does not use the H2 JDBC layer for
 * SQLXML values, so the XML-processing methods are unreachable in
 * production; removing the XML factory wiring eliminates the surface
 * entirely.</p>
 *
 * <p>The string-based getters and setters ({@link #getString()},
 * {@link #setString(String)}) and the binary/character stream methods
 * remain functional because they do not perform XML parsing.</p>
 */
public class JdbcSQLXML extends JdbcLob implements SQLXML {

    /**
     * INTERNAL
     */
    public JdbcSQLXML(JdbcConnection conn, Value value, State state, int id) {
        super(conn, value, state, TraceObject.SQLXML, id);
    }

    @Override
    void checkReadable() throws SQLException, IOException {
        checkClosed();
        // The original H2 implementation handled state == SET_CALLED here by
        // serialising a DOMResult through an unhardened TransformerFactory.
        // setResult() is now a throw-stub, so SET_CALLED can no longer be
        // reached from XML processing. We keep the no-op path so that the
        // string/stream setters continue to behave identically.
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        return super.getBinaryStream();
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
        return super.getCharacterStream();
    }

    @Override
    public <T extends Source> T getSource(Class<T> sourceClass) throws SQLException {
        // GridGain does not parse XML through the H2 JDBC layer. Returning
        // a Source would require constructing a DocumentBuilderFactory /
        // XMLInputFactory / TransformerFactory, each of which is an XXE
        // sink unless explicitly hardened — the factories are gone.
        throw new SQLFeatureNotSupportedException(
            "JdbcSQLXML.getSource is not supported. "
            + "Use getString() / getBinaryStream() / getCharacterStream() instead.");
    }

    @Override
    public String getString() throws SQLException {
        try {
            debugCodeCall("getString");
            checkReadable();
            return value.getString();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    @Override
    public OutputStream setBinaryStream() throws SQLException {
        try {
            debugCodeCall("setBinaryStream");
            checkEditable();
            state = State.SET_CALLED;
            return new BufferedOutputStream(setClobOutputStreamImpl());
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    @Override
    public Writer setCharacterStream() throws SQLException {
        try {
            debugCodeCall("setCharacterStream");
            checkEditable();
            state = State.SET_CALLED;
            return setCharacterStreamImpl();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    @Override
    public <T extends Result> T setResult(Class<T> resultClass) throws SQLException {
        // See getSource above. setResult would require constructing a
        // SAXTransformerFactory / XMLOutputFactory, both XXE-prone
        // unless hardened.
        throw new SQLFeatureNotSupportedException(
            "JdbcSQLXML.setResult is not supported. "
            + "Use setString() / setBinaryStream() / setCharacterStream() instead.");
    }

    @Override
    public void setString(String value) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCodeCall("getSource", value);
            }
            checkEditable();
            completeWrite(conn.createClob(new StringReader(value), -1));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

}
