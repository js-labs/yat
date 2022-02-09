/*
 * Copyright (C) 2022 Sergey Zubarev, info@js-labs.org
 *
 * This file is a part of YAT.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.jsl.yat.server;

import org.jsl.collider.TimerQueue;
import org.jsl.collider.Session;
import org.jsl.collider.StreamDefragger;
import org.jsl.collider.RetainableByteBuffer;
import org.jsl.collider.Util;
import org.jsl.yat.Protocol;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SessionImpl implements Session.Listener
{
    private static final Logger s_logger = Server.s_logger;

    private final Session m_session;
    private final Server m_server;
    private final TimerQueue m_timerQueue;
    private final RequestManager m_requestManager;
    private final StreamDefragger m_streamDefragger;

    private final AtomicReference<TimerHandler> m_timerHandler;
    private final AtomicInteger m_bytesReceived;

    // Most messages are processed in context of the Server class,
    // logging logic requires a string builder, but Server process
    // requests multi thread and having a single StringBuilder in the
    // Server would require a lock. We will have a StringBuilder instance
    // for each connected session, and use it when process message.
    private final StringBuilder m_stringBuilder;
    private final Protocol.StringDecoder m_stringDecoder;
    private final SimpleDateFormat m_simpleDateFormat;

    private class TimerHandler implements TimerQueue.Task
    {
        private int m_bytesReceived;

        @Override
        public long run()
        {
            final int bytesReceived = SessionImpl.this.m_bytesReceived.get();
            if (m_bytesReceived == bytesReceived)
            {
                if (s_logger.isLoggable(Level.INFO))
                {
                    s_logger.info(m_session.getRemoteAddress().toString()
                            + ": connection timeout, close connection");
                }
                m_timerHandler.set(null);
                m_session.closeConnection();
            }
            m_bytesReceived = bytesReceived;
            return 0;
        }
    }

    public SessionImpl(Session session, Server server, TimerQueue timerQueue, RequestManager requestManager)
    {
        if (s_logger.isLoggable(Level.INFO))
        {
            s_logger.info(session.getRemoteAddress().toString()
                    + ": connection accepted (timeout=" + Protocol.SOCKET_TIMEOUT + ")");
        }

        m_session = session;
        m_server = server;
        m_timerQueue = timerQueue;
        m_requestManager = requestManager;

        m_streamDefragger = new StreamDefragger(Protocol.Message.SIZE_HEADER_SIZE)
        {
            @Override
            protected int validateHeader(ByteBuffer header)
            {
                return Protocol.Message.getSize(header);
            }
        };
        m_timerHandler = new AtomicReference<>();
        m_bytesReceived = new AtomicInteger();
        m_stringBuilder = new StringBuilder();
        m_stringDecoder = new Protocol.StringDecoder();
        m_simpleDateFormat = new SimpleDateFormat(Protocol.DATE_FORMAT);

        final TimerHandler timerHandler = new TimerHandler();
        m_timerHandler.set(timerHandler);
        timerQueue.schedule(timerHandler, Protocol.SOCKET_TIMEOUT, TimeUnit.SECONDS);
    }

    private interface MessagePrinter
    {
        void print(ByteBuffer msg, StringBuilder stringBuilder);
    }

    private void logMessage(ByteBuffer msg, MessagePrinter messagePrinter)
    {
        m_stringBuilder.append(m_session.getRemoteAddress().toString());
        m_stringBuilder.append(": received ");
        messagePrinter.print(msg, m_stringBuilder);
        s_logger.fine(m_stringBuilder.toString());
        m_stringBuilder.setLength(0);;
    }

    private void onMessage(RetainableByteBuffer msg)
    {
        final ByteBuffer byteBuffer = msg.getNioByteBuffer();
        final short messageId = Protocol.Message.getId(byteBuffer);
        if (messageId == Protocol.Ping.ID)
        {
            // do nothing
        }
        else if (messageId == Protocol.RegisterRequest.ID)
        {
            if (s_logger.isLoggable(Level.FINE))
                logMessage(byteBuffer, Protocol.RegisterRequest::print);
            if (m_requestManager.checkRequestInterval(m_session, m_timerQueue, Protocol.RegisterRequest.ID, (short)0))
                m_server.handleRegisterRequest(m_session, m_stringBuilder);
            // tracker device does not use long-term connections for requests
            m_session.closeConnection();
        }
        else if (messageId == Protocol.TrackerLinkRequest.ID)
        {
            if (s_logger.isLoggable(Level.FINE))
                logMessage(byteBuffer, Protocol.TrackerLinkRequest::print);
            if (m_requestManager.checkRequestInterval(m_session, m_timerQueue, Protocol.TrackerLinkRequest.ID, (short)0))
                m_server.handleTrackerLinkRequest(m_session, byteBuffer, m_timerQueue, m_stringBuilder);
            // tracker device does not use long-term connections for requests
            m_session.closeConnection();
        }
        else if (messageId == Protocol.MonitorLinkRequest.ID)
        {
            if (s_logger.isLoggable(Level.FINE))
                logMessage(byteBuffer, Protocol.MonitorLinkRequest::print);
            if (m_requestManager.checkRequestInterval(m_session, m_timerQueue, Protocol.MonitorLinkRequest.ID, Protocol.StreamOpenRequest.ID))
                m_server.handleMonitorLinkRequest(m_session, byteBuffer, m_stringBuilder);
        }
        else if (messageId == Protocol.StreamOpenRequest.ID)
        {
            if (s_logger.isLoggable(Level.FINE))
                logMessage(byteBuffer, Protocol.StreamOpenRequest::print);
            if (m_requestManager.checkRequestInterval(m_session, m_timerQueue, Protocol.StreamOpenRequest.ID, (short)0))
                m_server.handleStreamOpenRequest(m_session, byteBuffer, m_stringBuilder, m_stringDecoder, m_simpleDateFormat);
        }
        else if (messageId == Protocol.ResyncRequest.ID)
        {
            final int remaining = byteBuffer.remaining();
            if (remaining < Protocol.ResyncRequest.getMessageSize(0))
            {
                m_stringBuilder.append(m_session.getRemoteAddress().toString());
                m_stringBuilder.append(": invalid <ResyncRequest> received, close connection\n");
                Util.hexDump(byteBuffer, m_stringBuilder, 4);
                s_logger.warning(m_stringBuilder.toString());
                m_stringBuilder.setLength(0);
                m_session.closeConnection();
            }
            else
            {
                final int messages = Protocol.ResyncRequest.getMessages(byteBuffer);
                final int messageSize = Protocol.ResyncRequest.getMessageSize(messages);
                if (remaining == messageSize)
                {
                    if (s_logger.isLoggable(Level.FINE))
                        logMessage(byteBuffer, Protocol.ResyncRequest::print);
                    m_server.handleResyncRequest(m_session, byteBuffer, m_stringBuilder);
                }
                else
                {
                    m_stringBuilder.append(m_session.getRemoteAddress().toString());
                    m_stringBuilder.append(": invalid <ResyncRequest> received, close connection");
                    Util.hexDump(byteBuffer, m_stringBuilder, 4);
                    s_logger.warning(m_stringBuilder.toString());
                    m_stringBuilder.setLength(0);
                    m_session.closeConnection();
                }
            }
        }
        else if (messageId == Protocol.Tracking.ID)
        {
            m_server.handleTracking(msg, m_session.getRemoteAddress(),
                m_session.getCollider(), m_stringBuilder, m_stringDecoder, m_simpleDateFormat);
        }
        else
        {
            m_stringBuilder.append(m_session.getRemoteAddress().toString());
            m_stringBuilder.append(": unexpected message received\n");
            Util.hexDump(byteBuffer, m_stringBuilder, 5);
            m_stringBuilder.append("close connection");
            s_logger.warning(m_stringBuilder.toString());
            m_stringBuilder.setLength(0);
            m_session.closeConnection();
        }
    }

    @Override
    public void onDataReceived(RetainableByteBuffer data)
    {
        final int remaining = data.remaining();
        m_bytesReceived.addAndGet(remaining);
        if (remaining == 0)
        {
            if (s_logger.isLoggable(Level.WARNING))
            {
                s_logger.warning(m_session.getRemoteAddress().toString()
                        + ": internal error: zero size message received");
            }
        }
        else
        {
            RetainableByteBuffer msg = m_streamDefragger.getNext(data);
            while (msg != null)
            {
                if (msg == StreamDefragger.INVALID_HEADER)
                {
                    s_logger.warning(m_session.getRemoteAddress().toString()
                            + ": invalid message received, close connection.");
                    m_session.closeConnection();
                    return;
                }
                onMessage(msg);
                msg = m_streamDefragger.getNext();
            }
        }
    }

    @Override
    public void onConnectionClosed()
    {
        s_logger.info(m_session.getRemoteAddress().toString() + ": connection closed" );

        boolean interrupted = false;

        final TimerHandler timerHandler = m_timerHandler.getAndSet(null);
        if (timerHandler != null)
        {
            try
            {
                m_timerQueue.cancel(timerHandler);
            }
            catch (final InterruptedException ex)
            {
                s_logger.warning(m_session.getRemoteAddress().toString() + ": " + ex);
                interrupted = true;
            }
        }

        m_streamDefragger.close();
        m_server.onConnectionClosed(m_session);

        if (interrupted)
            Thread.currentThread().interrupt();
    }
}
