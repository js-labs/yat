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

import org.jsl.collider.*;
import org.jsl.yat.Protocol;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Server extends ThreadPool.Runnable implements TimerQueue.Task
{
    public static final Logger s_logger = Logger.getLogger(Main.class.getName());

    public static final int REQUEST_MIN_INTERVAL = 2; // sec
    public static final int LINK_REQUEST_TIMEOUT = 60*100; // sec FIXME

    private static final String TRACKING_FILE_PREFIX = "tracking";

    private final String m_storagePath;

    private final Random m_rand;
    private final ReentrantLock m_lock;
    private final HashMap<UUID, TrackingDevice> m_trackingDevices;
    private final HashMap<Session, TrackingDevice> m_openStreams;
    private final ArrayList<LinkRequest> m_linkRequests;

    private ListItem m_head;
    private final AtomicReference<ListItem> m_tail;

    private FileOutputStream m_outputStream;

    private static class LinkRequest
    {
        public final int linkCode;
        public final UUID deviceId;
        public long timeout;

        LinkRequest(int linkCode, UUID deviceId, long timeout)
        {
            this.linkCode = linkCode;
            this.deviceId = deviceId;
            this.timeout = timeout;
        }
    }

    private static class ListItem
    {
        public volatile ListItem next;
        public RetainableByteBuffer data;
        public final int pos;
        public final int limit;
        public SocketAddress sourceAddr;

        ListItem(RetainableByteBuffer data, SocketAddress sourceAddr)
        {
            this.data = data;
            this.pos = data.position();
            this.limit = data.limit();
            this.sourceAddr = sourceAddr;
            data.retain();
        }
    }

    private static class TrackingUpdatesFile
    {
        public final Path path;
        public final FileTime lastModifiedTime;

        TrackingUpdatesFile(Path path, FileTime lastModifiedTime)
        {
            this.path = path;
            this.lastModifiedTime = lastModifiedTime;
        }
    }

    private void loadTrackingUpdates(ArrayList<TrackingUpdatesFile> files,
        StringBuilder stringBuilder, Protocol.StringDecoder stringDecoder, DateFormat dateFormat)
    {
        files.sort(Comparator.comparing(o -> o.lastModifiedTime));
        int trackingUpdates = 0;
        for (TrackingUpdatesFile file : files)
        {
            try
            {
                final FileInputStream inputStream = new FileInputStream(file.path.toString());
                final FileChannel fileChannel = inputStream.getChannel();
                final ByteBuffer byteBuffer = ByteBuffer.allocate(1024*16);
                byteBuffer.order(Protocol.BYTE_ORDER);
                while (fileChannel.read(byteBuffer) > 0)
                {
                    byteBuffer.flip();
                    final int limit = byteBuffer.limit();
                    int pos = 0;
                    for (;;)
                    {
                        final int remaining = (limit - pos);
                        short messageSize;
                        if ((remaining < Protocol.Message.SIZE_HEADER_SIZE)
                             || (remaining < (messageSize = Protocol.Message.getSize(byteBuffer, pos))))
                        {
                            for (int dst=0, src=pos; dst<remaining; dst++, src++)
                                byteBuffer.put(dst, byteBuffer.get(src));
                            byteBuffer.limit(byteBuffer.capacity());
                            byteBuffer.position(remaining);
                            break;
                        }

                        byteBuffer.limit(pos + messageSize);
                        byteBuffer.position(pos);

                        final UUID uuid = Protocol.Tracking.getDeviceId(byteBuffer);
                        final TrackingDevice trackingDevice = m_trackingDevices.get(uuid);
                        if (trackingDevice != null)
                        {
                            trackingDevice.handleTracking(byteBuffer, stringBuilder, stringDecoder, dateFormat);
                            trackingUpdates++;
                        }

                        pos += messageSize;
                        byteBuffer.limit(limit);
                    }
                }
            }
            catch (final IOException ex)
            {
                s_logger.info(ex.toString());
            }
        }

        if (s_logger.isLoggable(Level.INFO))
        {
            final StringBuilder sb = new StringBuilder();
            sb.append(trackingUpdates);
            sb.append(" tracking updates (");
            final int c = files.size();
            for (int idx=0; idx<c; idx++)
            {
                if (idx > 0)
                    sb.append(", ");
                sb.append(files.get(idx).path);
            }
            sb.append(')');
            s_logger.info(sb.toString());
        }
    }

    public boolean open(StringBuilder stringBuilder, Protocol.StringDecoder stringDecoder, DateFormat dateFormat)
    {
        final Path storagePath = Paths.get(m_storagePath);
        final ArrayList<TrackingUpdatesFile> trackingUpdateFiles = new ArrayList<>();
        try
        {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(storagePath))
            {
                for (Path p : stream)
                {
                    final Path fileName = p.getFileName();
                    UUID uuid;
                    try
                    {
                        uuid = UUID.fromString(fileName.toString());
                    }
                    catch (final IllegalArgumentException ex)
                    {
                        uuid = null;
                    }

                    if (uuid == null)
                    {
                        if (fileName.toString().startsWith(TRACKING_FILE_PREFIX))
                        {
                            try
                            {
                                final BasicFileAttributes fileAttributes = Files.readAttributes(p, BasicFileAttributes.class);
                                final FileTime lastModifiedTime = fileAttributes.lastModifiedTime();
                                trackingUpdateFiles.add(new TrackingUpdatesFile(p, lastModifiedTime));
                            }
                            catch (final IOException ex)
                            {
                                s_logger.warning(ex.toString());
                            }
                        }
                        else
                            s_logger.warning("Unknown file '" + p + "'");
                    }
                    else
                        m_trackingDevices.put(uuid, new TrackingDevice());
                }
            }
            catch (final DirectoryIteratorException ex)
            {
                s_logger.warning(ex.toString());
            }
        }
        catch (final IOException ex)
        {
            s_logger.warning(ex.toString());
            return false;
        }
        s_logger.info(m_trackingDevices.size() + " tracking devices");

        loadTrackingUpdates(trackingUpdateFiles, stringBuilder, stringDecoder, dateFormat);

        final Calendar calendar = new GregorianCalendar();
        calendar.setTime(new Date(System.currentTimeMillis()));
        final int year = calendar.get(Calendar.YEAR);
        final int month = calendar.get(Calendar.MONTH) + 1;
        final int day = calendar.get(Calendar.DAY_OF_MONTH);
        final String fileName = String.format("%s-%04d-%02d-%02d", TRACKING_FILE_PREFIX, year, month, day);
        final Path p = Paths.get(m_storagePath, fileName);

        try
        {
            m_outputStream = new FileOutputStream(p.toString(), true);
        }
        catch (final FileNotFoundException ex)
        {
            s_logger.warning(ex.toString());
            return false;
        }

        s_logger.info("Write tracking updates to file " + p);
        return true;
    }

    public void handleRegisterRequest(Session session, StringBuilder stringBuilder)
    {
        final UUID deviceId = UUID.randomUUID();
        final Path fileName = Paths.get(m_storagePath, deviceId.toString());
        try
        {
            final FileWriter fileWriter = new FileWriter(fileName.toString());
            fileWriter.close();

            final TrackingDevice trackingDevice = new TrackingDevice();
            m_lock.lock();
            try
            {
                m_trackingDevices.put(deviceId, trackingDevice);
            }
            finally
            {
                m_lock.unlock();
            }

            final int messageSize = Protocol.RegisterReply.getMessageSize();
            final ByteBuffer registerReply = ByteBuffer.allocate(messageSize);
            registerReply.order(Protocol.BYTE_ORDER);
            Protocol.RegisterReply.init(registerReply, deviceId);
            session.sendData(registerReply);

            if (s_logger.isLoggable(Level.INFO))
            {
                stringBuilder.append(session.getRemoteAddress().toString());
                stringBuilder.append(": send ");
                Protocol.RegisterReply.print(registerReply, stringBuilder);
                s_logger.info(stringBuilder.toString());
                stringBuilder.setLength(0);
            }
        }
        catch (final IOException ex)
        {
            if (s_logger.isLoggable(Level.WARNING))
                s_logger.warning("Failed to create file '" + fileName + "'");
            // There are some problems on our side,
            // let's just close a connection in this case
        }
    }

    public void handleTrackerLinkRequest(Session session,
        ByteBuffer byteBuffer, TimerQueue timerQueue, StringBuilder stringBuilder)
    {
        final long timeout = (System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(LINK_REQUEST_TIMEOUT));
        final UUID deviceId = Protocol.TrackerLinkRequest.getDeviceId(byteBuffer);
        int linkCode = 0;
        boolean startTimer = false;
        m_lock.lock();
        try
        {
            final TrackingDevice trackingDevice = m_trackingDevices.get(deviceId);
            if (trackingDevice != null)
            {
                LinkRequest linkRequest = null;
                for (LinkRequest lr : m_linkRequests)
                {
                    if (lr.deviceId.equals(deviceId))
                    {
                        linkRequest = lr;
                        break;
                    }
                }

                if (linkRequest == null)
                {
                    linkCode = (Math.abs(m_rand.nextInt()) % 100000);
                    linkRequest = new LinkRequest(linkCode, deviceId, timeout);
                    startTimer = m_linkRequests.isEmpty();
                    m_linkRequests.add(linkRequest);
                }
                else
                {
                    // extend the request timeout
                    linkCode = linkRequest.linkCode;
                    linkRequest.timeout = timeout;
                }
            }
            /* else unknown tracking device will be handled later */
        }
        finally
        {
            m_lock.unlock();
        }

        if (linkCode == 0)
        {
            if (s_logger.isLoggable(Level.WARNING))
            {
                s_logger.warning(session.getRemoteAddress().toString()
                        + ": invalid tracking device identifier " + deviceId);
            }
        }
        else
        {
            final int messageSize = Protocol.TrackerLinkReply.getMessageSize();
            final ByteBuffer trackerLinkReply = ByteBuffer.allocate(messageSize);
            trackerLinkReply.order(Protocol.BYTE_ORDER);
            Protocol.TrackerLinkReply.init(trackerLinkReply, linkCode);

            if (s_logger.isLoggable(Level.INFO))
            {
                stringBuilder.append(session.getRemoteAddress().toString());
                stringBuilder.append(": send ");
                Protocol.TrackerLinkReply.print(trackerLinkReply, stringBuilder);
                s_logger.fine(stringBuilder.toString());
                stringBuilder.setLength(0);
            }

            session.sendData(trackerLinkReply);

            if (startTimer)
                timerQueue.schedule(this, LINK_REQUEST_TIMEOUT, TimeUnit.SECONDS);
        }
    }

    public void handleMonitorLinkRequest(Session session, ByteBuffer monitorLinkRequest, StringBuilder stringBuilder)
    {
        final int linkCode = Protocol.MonitorLinkRequest.getLinkCode(monitorLinkRequest);
        UUID deviceId = null;
        m_lock.lock();
        try
        {
            final Iterator<LinkRequest> iterator = m_linkRequests.iterator();
            while (iterator.hasNext())
            {
                final LinkRequest linkRequestInfo = iterator.next();
                if (linkRequestInfo.linkCode == linkCode)
                {
                    // does it make sense to restart timer?
                    deviceId = linkRequestInfo.deviceId;
                    iterator.remove();
                    break;
                }
            }
        }
        finally
        {
            m_lock.unlock();
        }

        final int messageSize = Protocol.MonitorLinkReply.getMessageSize();
        final ByteBuffer monitorLinkReply = ByteBuffer.allocate(messageSize);
        monitorLinkReply.order(Protocol.BYTE_ORDER);

        if (deviceId == null)
        {
            if (s_logger.isLoggable(Level.WARNING))
            {
                s_logger.warning(session.getRemoteAddress().toString()
                    + ": received invalid link request code " + linkCode);
            }
            Protocol.MonitorLinkReply.init(monitorLinkReply, 0, 0);
        }
        else
        {
            Protocol.MonitorLinkReply.init(monitorLinkReply,
                    deviceId.getMostSignificantBits(),
                    deviceId.getLeastSignificantBits());
        }

        if (s_logger.isLoggable(Level.INFO))
        {
            stringBuilder.append(session.getRemoteAddress().toString());
            stringBuilder.append(": send ");
            Protocol.MonitorLinkReply.print(monitorLinkReply, stringBuilder);
            s_logger.info(stringBuilder.toString());
            stringBuilder.setLength(0);
        }

        session.sendData(monitorLinkReply);
    }

    public void handleStreamOpenRequest(Session session, ByteBuffer streamOpenRequest,
        StringBuilder stringBuilder, Protocol.StringDecoder stringDecoder, DateFormat dateFormat)
    {
        final UUID deviceId = Protocol.StreamOpenRequest.getDeviceId(streamOpenRequest);
        TrackingDevice trackingDevice;
        m_lock.lock();
        try
        {
            trackingDevice = m_trackingDevices.get(deviceId);
            if (trackingDevice != null)
                m_openStreams.put(session, trackingDevice);
        }
        finally
        {
            m_lock.unlock();
        }

        if (trackingDevice != null)
            trackingDevice.handleStreamOpenRequest(session, stringBuilder, stringDecoder, dateFormat);
    }

    public void handleResyncRequest(Session session, ByteBuffer resyncRequest, StringBuilder stringBuilder)
    {
        final UUID deviceId = Protocol.ResyncRequest.getDeviceId(resyncRequest);
        TrackingDevice trackingDevice;
        m_lock.lock();
        try
        {
            trackingDevice = m_trackingDevices.get(deviceId);
        }
        finally
        {
            m_lock.unlock();
        }

        if (trackingDevice == null)
        {
            s_logger.warning(session.getRemoteAddress().toString() +
                    ": unexpected device id " + deviceId);
        }
        else
            trackingDevice.handleResyncRequest(session, resyncRequest, stringBuilder);
    }

    public void handleTracking(RetainableByteBuffer data, SocketAddress sourceAddr, Collider collider,
        StringBuilder stringBuilder, Protocol.StringDecoder stringDecoder, DateFormat dateFormat)
    {
        final ByteBuffer byteBuffer = data.getNioByteBuffer().duplicate();

        stringBuilder.append(sourceAddr.toString());
        stringBuilder.append(": received\n");
        Protocol.hexDump(byteBuffer, stringBuilder, 16);
        s_logger.fine(stringBuilder.toString());
        stringBuilder.setLength(0);

        if (s_logger.isLoggable(Level.FINE))
        {
            stringBuilder.append(sourceAddr.toString());
            stringBuilder.append(": received ");
            Protocol.Tracking.print(byteBuffer, Protocol.Tracking.Variant.TrackerToServer, stringBuilder, stringDecoder, dateFormat);
            s_logger.fine(stringBuilder.toString());
            stringBuilder.setLength(0);
        }

        final UUID uuid = Protocol.Tracking.getDeviceId(byteBuffer);
        TrackingDevice trackingDevice;

        m_lock.lock();
        try
        {
            trackingDevice = m_trackingDevices.get(uuid);
        }
        finally
        {
            m_lock.unlock();
        }

        if (trackingDevice == null)
        {
            if (s_logger.isLoggable(Level.WARNING))
            {
                stringBuilder.append(sourceAddr.toString());
                stringBuilder.append(": invalid device id ");
                stringBuilder.append(uuid);
                s_logger.log(Level.WARNING, stringBuilder.toString());
                stringBuilder.setLength(0);
            }
        }
        else
        {
            trackingDevice.handleTracking(byteBuffer, stringBuilder, stringDecoder, dateFormat, sourceAddr);

            final ListItem listItem = new ListItem(data, sourceAddr);
            final ListItem tail = m_tail.getAndSet(listItem);
            if (tail == null)
            {
                m_head = listItem;
                final ThreadPool threadPool = collider.getThreadPool();
                threadPool.execute(this);
            }
            else
                tail.next = listItem;
        }
    }

    public void onConnectionClosed(Session session)
    {
        TrackingDevice trackingDevice;
        m_lock.lock();
        try
        {
            trackingDevice = m_openStreams.remove(session);
        }
        finally
        {
            m_lock.unlock();
        }
        if (trackingDevice != null)
            trackingDevice.onConnectionClosed(session);
    }

    @Override
    public long run()
    {
        // link requests timeout callback
        // remove all expired requests and find the next with minimum timeout
        final long currentTime = System.currentTimeMillis();
        long nextTimeout = Long.MAX_VALUE;
        int requestsCanceled = 0;
        m_lock.lock();
        try
        {
            Iterator<LinkRequest> iterator = m_linkRequests.iterator();
            while (iterator.hasNext())
            {
                final LinkRequest linkRequest = iterator.next();
                if (linkRequest.timeout <= currentTime)
                {
                    requestsCanceled++;
                    iterator.remove();
                }
                else if (linkRequest.timeout < nextTimeout)
                    nextTimeout = linkRequest.timeout;
            }
        }
        finally
        {
            m_lock.unlock();
        }

        if (nextTimeout == Long.MAX_VALUE)
            nextTimeout = 0;
        else
            nextTimeout = (nextTimeout - currentTime);

        if (s_logger.isLoggable(Level.INFO))
        {
            s_logger.info("Cancelled " + requestsCanceled
                + " link requests, next timer in " + nextTimeout);
        }

        return nextTimeout;
    }

    @Override
    public void runInThreadPool()
    {
        // persist all received tracking updates
        final FileChannel fileChannel = m_outputStream.getChannel();
        ListItem listItem = m_head;
        for (;;)
        {
            // have to duplicate ByteBuffer here
            final ByteBuffer byteBuffer = listItem.data.getNioByteBuffer().asReadOnlyBuffer();
            byteBuffer.limit(listItem.limit);
            byteBuffer.position(listItem.pos);

            try { fileChannel.write(byteBuffer); }
            catch (final IOException ex) { s_logger.warning(ex.toString()); }

            listItem.data.release();
            listItem.data = null;
            listItem.sourceAddr = null;

            ListItem next = listItem.next;
            if (next == null)
            {
                try { m_outputStream.flush(); }
                catch (final IOException ex) { s_logger.warning(ex.toString()); }

                m_head = null;
                if (m_tail.compareAndSet(listItem, null))
                {
                    // one can think doing flush here is a nice idea,
                    // but it is wrong, because after CAS another thread
                    // can start write something to the file,
                    // thus we should flush before the CAS
                    break;
                }
                while ((next = listItem.next) == null);
            }
            listItem = next;
        }
    }

    private static class ShutdownHook extends Thread
    {
        private final Collider m_collider;
        private final TimerQueue m_timerQueue;
        private final Thread m_colliderThread;

        public ShutdownHook(Collider collider, TimerQueue timerQueue, Thread colliderThread)
        {
            m_collider = collider;
            m_timerQueue = timerQueue;
            m_colliderThread = colliderThread;
        }

        public void run()
        {
            s_logger.info("Shutdown hook");
            m_timerQueue.stop();
            m_collider.stop();
            try
            {
                m_colliderThread.join();
            }
            catch (final InterruptedException ex)
            {
                s_logger.warning(ex.toString());
            }
        }
    }

    private class AcceptorImpl extends Acceptor
    {
        private final TimerQueue m_timerQueue;
        private final RequestManager m_requestManager;

        AcceptorImpl(int listenPort, TimerQueue timerQueue)
        {
            super(listenPort);
            m_timerQueue = timerQueue;
            m_requestManager = new RequestManager();
        }

        @Override
        public Session.Listener createSessionListener(Session session)
        {
            return new SessionImpl(session, Server.this, m_timerQueue, m_requestManager);
        }

        @Override
        public void onAcceptorStarted(Collider collider, int localPort)
        {
            s_logger.info("Server acceptor started at port " + localPort);
        }
    }

    private class DatagramListenerImpl extends DatagramListener
    {
        private final StringBuilder m_stringBuilder;
        private final Protocol.StringDecoder m_stringDecoder;
        private final DateFormat m_dateFormat;
        private final Collider m_collider;

        DatagramListenerImpl(InetSocketAddress addr,
            StringBuilder stringBuilder, Protocol.StringDecoder stringDecoder, DateFormat dateFormat, Collider collider)
        {
            super(addr);
            m_stringBuilder = stringBuilder;
            m_stringDecoder = stringDecoder;
            m_dateFormat = dateFormat;
            m_collider = collider;
        }

        @Override
        public void onDataReceived(RetainableByteBuffer data, SocketAddress sourceAddr)
        {
            final ByteBuffer byteBuffer = data.getNioByteBuffer();
            final int remaining = byteBuffer.remaining();
            if (remaining >= Protocol.Message.HEADER_SIZE)
            {
                final short messageSize = Protocol.Message.getSize(byteBuffer);
                if (messageSize == remaining)
                {
                    final short messageId = Protocol.Message.getId(byteBuffer);
                    if (messageId == Protocol.Tracking.ID)
                    {
                        handleTracking(data, sourceAddr, m_collider, m_stringBuilder, m_stringDecoder, m_dateFormat);
                        return;
                    }

                    if (s_logger.isLoggable(Level.WARNING))
                    {
                        m_stringBuilder.append("Unexpected message ");
                        m_stringBuilder.append(messageId);
                    }
                }
                else
                {
                    if (s_logger.isLoggable(Level.WARNING))
                        m_stringBuilder.append("Invalid message");
                }
            }
            else
            {
                if (s_logger.isLoggable(Level.WARNING))
                {
                    m_stringBuilder.append("Invalid message");
                    Util.hexDump(byteBuffer, m_stringBuilder, 4);
                }
            }

            if (s_logger.isLoggable(Level.WARNING))
            {
                m_stringBuilder.append('\n');
                Util.hexDump(byteBuffer, m_stringBuilder, 4);
                m_stringBuilder.append(" received from ");
                m_stringBuilder.append(sourceAddr.toString());
                s_logger.warning(m_stringBuilder.toString());
                m_stringBuilder.setLength(0);
            }
        }
    }

    public Server(String storagePath)
    {
        m_storagePath = storagePath;
        m_rand = new Random();
        m_lock = new ReentrantLock();
        m_trackingDevices = new HashMap<>();
        m_openStreams = new HashMap<>();
        m_linkRequests = new ArrayList<>();
        m_tail = new AtomicReference<>();
    }

    public void run(int portNumber)
    {
        s_logger.info("YAT-server starting at " + portNumber);
        Collider collider;
        try
        {
            final Collider.Config config = new Collider.Config();
            config.byteOrder = Protocol.BYTE_ORDER;
            collider = Collider.create(config);
        }
        catch (final IOException ex)
        {
            s_logger.warning(ex.toString());
            System.exit(-1);
            return;
        }

        // Use stringBuilder, stringDecoder and simpleDateFormat
        // to load stored tracking updates and for income messages later
        final StringBuilder stringBuilder = new StringBuilder();
        final Protocol.StringDecoder stringDecoder = new Protocol.StringDecoder();
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Protocol.DATE_FORMAT);
        if (open(stringBuilder, stringDecoder, simpleDateFormat))
        {
            final TimerQueue timerQueue = new TimerQueue(collider.getThreadPool());
            final AcceptorImpl acceptor = new AcceptorImpl(portNumber, timerQueue);
            final InetSocketAddress listenAddr = new InetSocketAddress(portNumber);
            final DatagramListenerImpl datagramListener = new DatagramListenerImpl(listenAddr, stringBuilder, stringDecoder, simpleDateFormat, collider);
            try
            {
                collider.addAcceptor(acceptor);
                collider.addDatagramListener(datagramListener);
                Runtime.getRuntime().addShutdownHook(new ShutdownHook(collider, timerQueue, Thread.currentThread()));
                collider.run();
            }
            catch (final IOException ex)
            {
                s_logger.warning(ex.toString());
            }

            try { m_outputStream.close(); }
            catch (final IOException ex) { s_logger.warning(ex.toString()); }
        }
        else
            System.exit(-1);
        s_logger.info("Done");
    }
}
