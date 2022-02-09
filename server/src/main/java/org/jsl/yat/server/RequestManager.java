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

import org.jsl.collider.Session;
import org.jsl.collider.TimerQueue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

class RequestManager implements TimerQueue.Task
{
    private static final Logger s_logger = Server.s_logger;

    private static class RequestInfo
    {
        final long time;
        short nextAllowedMessageId;

        public RequestInfo(long time, short nextAllowedMessageId)
        {
            this.time = time;
            this.nextAllowedMessageId = nextAllowedMessageId;
        }
    }

    private final ReentrantLock m_lock;
    private final HashMap<InetAddress, RequestInfo> m_requestByAddr;
    private final ArrayList<InetAddress> m_list;

    public RequestManager()
    {
        m_lock = new ReentrantLock();
        m_requestByAddr = new HashMap<>();
        m_list = new ArrayList<>();
    }

    public boolean checkRequestInterval(Session session, TimerQueue timerQueue,
        short messageId, short nextAllowedMessageId)
    {
        // allow only one request form the address per REQUEST_MIN_INTERVAL
        final SocketAddress socketAddress = session.getRemoteAddress();
        if (socketAddress instanceof InetSocketAddress)
        {
            final InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
            final InetAddress inetAddress = inetSocketAddress.getAddress();

            final long currentTime = System.currentTimeMillis();
            boolean scheduleTimer = false;
            boolean ret;
            m_lock.lock();
            try
            {
                RequestInfo requestInfo = m_requestByAddr.get(inetAddress);
                if (requestInfo == null)
                {
                    requestInfo = new RequestInfo(currentTime, nextAllowedMessageId);
                    scheduleTimer = m_list.isEmpty();
                    m_requestByAddr.put(inetAddress, requestInfo);
                    m_list.add(inetAddress);
                    ret = true;
                }
                else if (requestInfo.nextAllowedMessageId == messageId)
                {
                    requestInfo.nextAllowedMessageId = 0;
                    ret = true;
                }
                else
                {
                    // check time
                    final long diff = (currentTime - requestInfo.time);
                    ret = (diff >= TimeUnit.SECONDS.toMillis(Server.REQUEST_MIN_INTERVAL));
                }
            }
            finally
            {
                m_lock.unlock();
            }

            if (scheduleTimer)
            {
                if (s_logger.isLoggable(Level.INFO))
                    s_logger.info("RequestManager: start timer @ " + Server.REQUEST_MIN_INTERVAL + " seconds");
                timerQueue.schedule(this, Server.REQUEST_MIN_INTERVAL, TimeUnit.SECONDS);
            }

            if (!ret)
                s_logger.warning("Exceeded number or requests from " + socketAddress);

            return ret;
        }
        else
            s_logger.warning("Address is not instance of class 'InetSocketAddr'");
        return false;
    }

    @Override
    public long run()
    {
        final long currentTime = System.currentTimeMillis();
        m_lock.lock();
        try
        {
            Iterator<InetAddress> iterator = m_list.iterator();
            int requests = 0;
            while (iterator.hasNext())
            {
                final InetAddress inetAddress = iterator.next();
                final RequestInfo requestInfo = m_requestByAddr.get(inetAddress);
                final long diff = (requestInfo.time - currentTime);
                if (diff <= 0)
                {
                    m_requestByAddr.remove(inetAddress);
                    iterator.remove();
                }
                else
                {
                    if (s_logger.isLoggable(Level.FINE))
                        s_logger.fine("RequestManager: " + requests + " requests removed, " + diff);
                    return diff;
                }
                requests++;
            }

            if (s_logger.isLoggable(Level.FINE))
            {
                s_logger.fine("RequestManager: " + requests
                        + " requests removed (" + m_requestByAddr.size() + " pending requests)");
            }
        }
        finally
        {
            m_lock.unlock();
        }

        return 0;
    }
}
