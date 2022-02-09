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

import org.jsl.collider.RetainableByteBuffer;
import org.jsl.collider.Session;
import org.jsl.yat.Protocol;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TrackingDevice
{
    private static final Logger s_logger = Server.s_logger;

    private final ReentrantLock m_lock;
    private final ArrayList<Session> m_subscribers;
    private final TreeSet<Long> m_receivedMessages;

    private SocketAddress m_lastAddr;

    private long m_batteryLevelTime;
    private short m_batteryLevel;

    private long m_networkNameTime;
    private ByteBuffer m_networkName;

    private final TreeMap<Long, LocationInfo> m_locations;

    private long m_trackingStoppedTime;

    private static class LocationInfo
    {
        public final double latitude;
        public final double longitude;
        public final double altitude;

        public LocationInfo(double latitude, double longitude, double altitude)
        {
            this.latitude = latitude;
            this.longitude = longitude;
            this.altitude = altitude;
        }
    }

    public TrackingDevice()
    {
        m_lock = new ReentrantLock();
        m_receivedMessages = new TreeSet<>();
        m_subscribers = new ArrayList<>();
        m_locations = new TreeMap<>();
    }

    public void handleStreamOpenRequest(Session session,
        StringBuilder stringBuilder, Protocol.StringDecoder stringDecoder, DateFormat dateFormat)
    {
        ByteBuffer byteBuffer;
        m_lock.lock();
        try
        {
            for (Session s : m_subscribers)
            {
                if (s == session)
                    return;
            }
            m_subscribers.add(session);

            int messageSize = Protocol.Message.HEADER_SIZE;
            if (m_batteryLevelTime != 0)
                messageSize += Protocol.Tracking.getBatteryLevelSize();

            if (m_networkNameTime != 0)
                messageSize += Protocol.Tracking.getNetworkNameSize(m_networkName);

            if (!m_locations.isEmpty())
                messageSize += Protocol.Tracking.getLocationSize();

            if (m_trackingStoppedTime != 0)
                messageSize += Protocol.Tracking.getTrackingStoppedSize();

            byteBuffer = ByteBuffer.allocate(messageSize);
            byteBuffer.order(Protocol.BYTE_ORDER);

            int pos = Protocol.Tracking.init_SM(byteBuffer, (short)messageSize);
            if (m_batteryLevelTime != 0)
                pos = Protocol.Tracking.addBatteryLevel(byteBuffer, pos, m_batteryLevelTime, m_batteryLevel);

            if (m_networkNameTime != 0)
                pos = Protocol.Tracking.addNetworkName(byteBuffer, pos, m_networkNameTime, m_networkName);

            if (!m_locations.isEmpty())
            {
                final Map.Entry<Long, LocationInfo> lastEntry = m_locations.lastEntry();
                final LocationInfo locationInfo = lastEntry.getValue();
                pos = Protocol.Tracking.addLocation(byteBuffer, pos, lastEntry.getKey(),
                    locationInfo.latitude, locationInfo.longitude, locationInfo.altitude);
            }

            if (m_trackingStoppedTime != 0)
                Protocol.Tracking.addTrackingStopped(byteBuffer, pos, m_trackingStoppedTime);
        }
        finally
        {
            m_lock.unlock();
        }

        if (s_logger.isLoggable(Level.INFO))
        {
            stringBuilder.append(session.getRemoteAddress().toString());
            stringBuilder.append(": send ");
            Protocol.Tracking.print(byteBuffer, Protocol.Tracking.Variant.ServerToMonitor, stringBuilder, stringDecoder, dateFormat);
            s_logger.info(stringBuilder.toString());
            stringBuilder.setLength(0);
        }

        session.sendData(byteBuffer);
    }

    public void handleTracking(ByteBuffer trackingTS,
        StringBuilder stringBuilder, Protocol.StringDecoder stringDecoder, DateFormat dateFormat)
    {
        Session [] subscribers = null;
        RetainableByteBuffer trackingSM = null;

        final long messageSN = Protocol.Tracking.getMessageSN(trackingTS);
        int brokenFields = 0;
        m_lock.lock();
        try
        {
            m_receivedMessages.add(messageSN);

            boolean batteryLevelUpdated = false;
            boolean networkNameUpdated = false;
            boolean trackingStoppedUpdated = false;
            final ArrayList<Long> newLocations = new ArrayList<>();

            final int limit = trackingTS.limit();
            int it = Protocol.Tracking.getFieldIterator_TS(trackingTS);
            while (it < limit)
            {
                final int fieldSize = Protocol.Tracking.getFieldSize(trackingTS, it);
                final int remaining = (limit - it);
                if (fieldSize > remaining)
                {
                    // FIXME
                    break;
                }

                final short fieldId = Protocol.Tracking.getFieldId(trackingTS, it);
                if (fieldId == Protocol.Tracking.FIELD_BATTERY_LEVEL)
                {
                    if (fieldSize < Protocol.Tracking.getBatteryLevelSize())
                        brokenFields++;
                    else
                    {
                        final long batteryLevelTime = Protocol.Tracking.getBatteryLevelTime(trackingTS, it);
                        // we can receive old messages in resync, have to check time to process only the last
                        if (batteryLevelTime > m_batteryLevelTime)
                        {
                            m_batteryLevelTime = batteryLevelTime;
                            m_batteryLevel = Protocol.Tracking.getBatteryLevel(trackingTS, it);
                            batteryLevelUpdated = true;

                            if ((m_trackingStoppedTime != 0) && (m_trackingStoppedTime < batteryLevelTime))
                                m_trackingStoppedTime = 0;
                        }
                    }
                }
                else if (fieldId == Protocol.Tracking.FIELD_NETWORK_NAME)
                {
                    if (fieldSize < Protocol.Tracking.NETWORK_NAME_HEADER_SIZE)
                        brokenFields++;
                    else
                    {
                        final long networkNameTime = Protocol.Tracking.getNetworkNameTime(trackingTS, it);
                        // we can receive old messages in resync, have to check time to keep the last value only
                        if (networkNameTime > m_networkNameTime)
                        {
                            m_networkNameTime = networkNameTime;
                            m_networkName = Protocol.Tracking.getNetworkName(trackingTS, it);
                            networkNameUpdated = true;

                            if ((m_trackingStoppedTime != 0) && (m_trackingStoppedTime < networkNameTime))
                                m_trackingStoppedTime = 0;
                        }
                    }
                }
                else if (fieldId == Protocol.Tracking.FIELD_LOCATION)
                {
                    if (fieldSize < Protocol.Tracking.getLocationSize())
                        brokenFields++;
                    else
                    {
                        final long locationTime = Protocol.Tracking.getLocationTime(trackingTS, it);
                        final LocationInfo locationInfo = new LocationInfo(
                                Protocol.Tracking.getLocationLatitude(trackingTS, it),
                                Protocol.Tracking.getLocationLongitude(trackingTS, it),
                                Protocol.Tracking.getLocationAltitude(trackingTS, it));
                        if (m_locations.put(locationTime, locationInfo) == null)
                            newLocations.add(locationTime);

                        if ((m_trackingStoppedTime != 0) && (m_trackingStoppedTime < locationTime))
                            m_trackingStoppedTime = 0;
                    }
                }
                else if (fieldId == Protocol.Tracking.FIELD_TRACKING_STOPPED)
                {
                    if (fieldSize < Protocol.Tracking.getTrackingStoppedSize())
                        brokenFields++;
                    else
                    {
                        final long trackingStoppedTime = Protocol.Tracking.getTrackingStoppedTime(trackingTS, it);
                        if ((trackingStoppedTime > m_trackingStoppedTime)
                            && (trackingStoppedTime > m_batteryLevelTime)
                            && (trackingStoppedTime > m_networkNameTime)
                            && (!m_locations.isEmpty() && (trackingStoppedTime > m_locations.lastKey())))
                        {
                            m_trackingStoppedTime = trackingStoppedTime;
                            trackingStoppedUpdated = true;
                        }
                    }
                }
                /* else unknown field?! */
                it += fieldSize;
            }

            if (!m_subscribers.isEmpty())
            {
                int messageSize = Protocol.Message.HEADER_SIZE;
                if (batteryLevelUpdated)
                    messageSize += Protocol.Tracking.getBatteryLevelSize();
                if (networkNameUpdated)
                    messageSize += Protocol.Tracking.getNetworkNameSize(m_networkName);
                if (newLocations.size() > 0)
                    messageSize += (Protocol.Tracking.getLocationSize() * newLocations.size());
                if (trackingStoppedUpdated)
                    messageSize += Protocol.Tracking.getTrackingStoppedSize();

                trackingSM = RetainableByteBuffer.allocate(messageSize);
                trackingSM.order(Protocol.BYTE_ORDER);
                final ByteBuffer byteBuffer = trackingSM.getNioByteBuffer();
                int pos = Protocol.Tracking.init_SM(byteBuffer, (short)messageSize);

                if (batteryLevelUpdated)
                    pos = Protocol.Tracking.addBatteryLevel(byteBuffer, pos, m_batteryLevelTime, m_batteryLevel);

                if (networkNameUpdated)
                    pos = Protocol.Tracking.addNetworkName(byteBuffer, pos, m_networkNameTime, m_networkName);

                for (Long locationTime : newLocations)
                {
                    final LocationInfo locationInfo = m_locations.get(locationTime);
                    pos = Protocol.Tracking.addLocation(byteBuffer, pos, locationTime,
                            locationInfo.latitude, locationInfo.longitude, locationInfo.altitude);
                }

                if (trackingStoppedUpdated)
                    Protocol.Tracking.addTrackingStopped(byteBuffer, pos, m_trackingStoppedTime);

                // copy subscribers to the local array
                // to send data later not under the lock
                subscribers = new Session[m_subscribers.size()];
                m_subscribers.toArray(subscribers);
            }
        }
        finally
        {
            m_lock.unlock();
        }

        if (brokenFields > 0)
        {
            if (s_logger.isLoggable(Level.WARNING))
            {
                stringBuilder.append("invalid <tracking> message received\n");
                Protocol.Tracking.print(trackingTS, Protocol.Tracking.Variant.TrackerToServer, stringBuilder, stringDecoder, dateFormat);
                s_logger.warning(stringBuilder.toString());
                stringBuilder.setLength(0);
            }
        }

        if (subscribers != null)
        {
            if (s_logger.isLoggable(Level.FINE))
            {
                stringBuilder.append(subscribers[0].getRemoteAddress().toString());
                stringBuilder.append(": send ");
                final ByteBuffer byteBuffer = trackingSM.getNioByteBuffer().duplicate();
                Protocol.Tracking.print(byteBuffer, Protocol.Tracking.Variant.ServerToMonitor, stringBuilder, stringDecoder, dateFormat);
                for (int idx=1; idx<subscribers.length; idx++)
                {
                    stringBuilder.append(subscribers[idx].getRemoteAddress().toString());
                    stringBuilder.append('\n');
                }
                s_logger.fine(stringBuilder.toString());
                stringBuilder.setLength(0);
            }

            for (Session s : subscribers)
                s.sendData(trackingSM);

            trackingSM.release();
        }

        s_logger.info("m_trackingStoppedTime=" + m_trackingStoppedTime);
    }

    public void handleTracking(ByteBuffer byteBuffer,
        StringBuilder stringBuilder, Protocol.StringDecoder stringDecoder, DateFormat dateFormat, SocketAddress sourceAddr)
    {
        handleTracking(byteBuffer, stringBuilder, stringDecoder, dateFormat);
        m_lastAddr = sourceAddr;
    }

    public void handleResyncRequest(Session session, ByteBuffer resyncRequest, StringBuilder stringBuilder)
    {
        int messages = Protocol.ResyncRequest.getMessages(resyncRequest);
        if (messages > 0)
        {
            int pos = Protocol.ResyncRequest.getDataPos(resyncRequest);
            long messageSN = resyncRequest.getLong(pos);

            final ByteBuffer resyncReply = ByteBuffer.allocate(Protocol.ResyncReply.getSize(255, 0));
            resyncReply.order(Protocol.BYTE_ORDER);

            m_lock.lock();
            try
            {
                int ackMessages = 0;

                if (m_receivedMessages.contains(messageSN))
                    ackMessages++;

                if (--messages > 0)
                {
                    pos += (Long.SIZE / Byte.SIZE);
                    for (; messages>0; messages--)
                    {
                        final int diff = resyncRequest.getInt(pos);
                        pos += (Integer.SIZE / Byte.SIZE);
                        messageSN -= diff;
                        if (m_receivedMessages.contains(messageSN))
                            ackMessages++;
                    }
                }

                messages = Protocol.ResyncRequest.getMessages(resyncRequest);
                pos = Protocol.ResyncRequest.getDataPos(resyncRequest);
                messageSN = resyncRequest.getLong(pos);

                final int requestMessages = (messages - ackMessages);
                int apos = Protocol.ResyncReply.init(resyncReply, ackMessages, requestMessages);

                int rpos = apos;
                if (ackMessages > 0)
                {
                    rpos += (Long.SIZE / Byte.SIZE);
                    if (ackMessages > 1)
                        rpos += (Integer.SIZE / Byte.SIZE) * (ackMessages - 1);
                }

                long lastAckMessageSN = 0;
                long lastRequestMessageSN = 0;

                if (m_receivedMessages.contains(messageSN))
                {
                    resyncReply.putLong(apos, messageSN);
                    lastAckMessageSN = messageSN;
                    apos += (Long.SIZE / Byte.SIZE);
                }
                else
                {
                    resyncReply.putLong(rpos, messageSN);
                    lastRequestMessageSN = messageSN;
                    rpos += (Long.SIZE / Byte.SIZE);
                }

                if (--messages > 0)
                {
                    pos += (Long.SIZE / Byte.SIZE);
                    for (; messages>0; messages--)
                    {
                        int diff = resyncRequest.getInt(pos);
                        pos += (Integer.SIZE / Byte.SIZE);
                        messageSN -= diff;

                        if (m_receivedMessages.contains(messageSN))
                        {
                            if (lastAckMessageSN == 0)
                            {
                                resyncReply.putLong(apos, messageSN);
                                lastAckMessageSN = messageSN;
                                apos += (Long.SIZE / Byte.SIZE);
                            }
                            else
                            {
                                diff = (int) (lastAckMessageSN - messageSN);
                                resyncReply.putInt(apos, diff);
                                apos += (Integer.SIZE / Byte.SIZE);
                            }
                        }
                        else
                        {
                            if (lastRequestMessageSN == 0)
                            {
                                resyncReply.putLong(rpos, messageSN);
                                lastRequestMessageSN = messageSN;
                                rpos += (Long.SIZE / Byte.SIZE);
                            }
                            else
                            {
                                diff = (int) (lastRequestMessageSN - messageSN);
                                resyncReply.putInt(rpos, diff);
                                rpos += (Integer.SIZE / Byte.SIZE);
                            }
                        }
                    }
                }
            }
            finally
            {
                m_lock.unlock();
            }

            if (s_logger.isLoggable(Level.FINE))
            {
                stringBuilder.append(session.getRemoteAddress().toString());
                stringBuilder.append(": send ");
                Protocol.ResyncReply.print(resyncReply, stringBuilder);
                s_logger.fine(stringBuilder.toString());
                stringBuilder.setLength(0);
            }

            session.sendData(resyncReply);
        }
    }

    public void onConnectionClosed(Session session)
    {
        m_lock.lock();
        try
        {
            Iterator<Session> iterator = m_subscribers.iterator();
            while (iterator.hasNext())
            {
                final Session s = iterator.next();
                if (s == session)
                {
                    iterator.remove();
                    return;
                }
            }
        }
        finally
        {
            m_lock.unlock();
        }
        s_logger.warning("Internal error: no open stream for "
                + session.getRemoteAddress().toString());
    }
}
