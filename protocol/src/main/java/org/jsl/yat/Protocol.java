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

package org.jsl.yat;

import java.nio.ByteOrder;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.security.InvalidParameterException;
import java.text.DateFormat;
import java.util.UUID;

public class Protocol
{
    public static final String CHARSET = "UTF-8";
    public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

    public static final byte [] SERVER_ADDR = {(byte)127, 0, 0, 1};
    public static final int SERVER_PORT = 80;

    public static final int SOCKET_TIMEOUT = 15; // sec
    public static final int SOCKET_PING_INTERVAL = 5; // sec

    private static final int SIZEOF_BYTE = 1;
    private static final int SIZEOF_SHORT = (Short.SIZE / Byte.SIZE);
    private static final int SIZEOF_INT = (Integer.SIZE / Byte.SIZE);
    private static final int SIZEOF_LONG = (Long.SIZE / Byte.SIZE);
    private static final int SIZEOF_DOUBLE = (Double.SIZE / Byte.SIZE);

    public static final ByteOrder BYTE_ORDER = ByteOrder.BIG_ENDIAN;

    private static final char [] HD = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

    public static void hexDump(ByteBuffer byteBuffer, StringBuilder sb, int maxLines)
    {
        int pos = byteBuffer.position();
        int limit = byteBuffer.limit();
        if (pos == limit)
        {
            sb.append("<empty>");
            return;
        }

        int c = ((limit - pos) / 16);
        if (c > maxLines)
            limit = (pos + (maxLines * 16));

        /*        "0000: 00 01 02 03 04 05 06 07 08 09 0A 0B 0C 0D 0E 0F | ................" */
        sb.append("       0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F [");
        sb.append(pos);
        sb.append(", ");
        sb.append(limit);
        sb.append("]\n");

        while (pos < limit)
        {
            int p = (pos - byteBuffer.position());
            sb.append(HD[(p >> 12) & 0x0F]);
            sb.append(HD[(p >> 8) & 0x0F]);
            sb.append(HD[(p >> 4) & 0x0F]);
            sb.append(HD[p & 0x0F]);
            sb.append(": ");

            int n = Math.min((limit - pos), 16);
            p = pos;
            for (c=n; c>0; c--, p++)
            {
                final int v = (((int)byteBuffer.get(p)) & 0xFF);
                sb.append(HD[(v >> 4)]);
                sb.append(HD[v & 0x0F]);
                sb.append(' ');
            }

            for (c=(16-n); c>0; c--)
                sb.append("   ");

            sb.append("| ");

            p = pos;
            for (c=n; c>0; c--, p++)
            {
                final int v = byteBuffer.get(p);
                final char vc = ((v >= 32) ? (char)v : '.');
                sb.append(vc);
            }

            sb.append('\n');
            pos += n;
        }
    }

    public static String hexDump(ByteBuffer byteBuffer)
    {
        final StringBuilder sb = new StringBuilder();
        hexDump(byteBuffer, sb, 2);
        return sb.toString();
    }

    public static void hexDumpSingleLine(ByteBuffer byteBuffer, StringBuilder sb, int from, int to)
    {
        sb.append("bytes(");
        int pos = from;

        int c = (byteBuffer.get(pos) & 0xFF);
        sb.append(HD[(c >> 4) & 0x0F]);
        sb.append(HD[c & 0x0F]);
        pos++;

        for (; pos<to; pos++)
        {
            c = (byteBuffer.get(pos) & 0xFF);
            sb.append(", ");
            sb.append(HD[(c >> 4) & 0x0F]);
            sb.append(HD[c & 0x0F]);
        }
        sb.append(')');
    }

    public static class StringDecoder
    {
        public static String ERROR = StringDecoder.class.getName();

        private final CharsetDecoder m_decoder;
        private CharBuffer m_buffer;

        public StringDecoder()
        {
            m_decoder = Charset.forName(CHARSET).newDecoder();
            m_buffer = CharBuffer.allocate(128);
        }

        public String decode(ByteBuffer byteBuffer)
        {
            final int r = byteBuffer.remaining();
            if (r == 0)
                return "";

            int n = (int) (r * m_decoder.averageCharsPerByte());
            if (m_buffer.capacity() < n)
                m_buffer = CharBuffer.allocate(n);

            for (;;)
            {
                final CoderResult cr = m_decoder.decode(byteBuffer, m_buffer, true);
                if (cr.isUnderflow())
                {
                    m_decoder.flush(m_buffer);
                    m_decoder.reset();
                    m_buffer.flip();
                    final String str = m_buffer.toString();
                    m_buffer.clear();
                    return str;
                }

                if (cr.isOverflow())
                {
                    n *= 2;
                    final CharBuffer cb = CharBuffer.allocate(n);
                    m_buffer.flip();
                    cb.put(m_buffer);
                    m_buffer = cb;
                    continue;
                }

                return ERROR;
            }
        }
    }

    public static class Message
    {
        public static final int SIZE_HEADER_SIZE = SIZEOF_SHORT;
        public static final int HEADER_SIZE = (SIZE_HEADER_SIZE + SIZEOF_SHORT);

        public static int create(ByteBuffer byteBuffer, short id, int extSize)
        {
            if (extSize > (Short.MAX_VALUE - HEADER_SIZE))
                throw new InvalidParameterException();
            final int messageSize = (HEADER_SIZE + extSize);
            final int pos = byteBuffer.position();
            byteBuffer.putShort((short)messageSize);
            byteBuffer.putShort(id);
            return pos;
        }

        public static int init(ByteBuffer byteBuffer, short messageSize, short messageId)
        {
            int pos = byteBuffer.position();
            byteBuffer.limit(pos + messageSize);
            byteBuffer.putShort(pos, messageSize);
            pos += SIZEOF_SHORT;
            byteBuffer.putShort(pos, messageId);
            pos += SIZEOF_SHORT;
            return pos;
        }

        public static short getSize(ByteBuffer byteBuffer, int pos)
        {
            return byteBuffer.getShort(pos);
        }

        public static short getSize(ByteBuffer byteBuffer)
        {
            return getSize(byteBuffer, byteBuffer.position());
        }

        public static short getId(ByteBuffer msg)
        {
            final int pos = (msg.position() + SIZEOF_SHORT);
            return msg.getShort(pos);
        }
    }

    public static class Ping
    {
        public static final short ID = 1;

        public static short getMessageSize()
        {
            return Message.HEADER_SIZE;
        }

        public static void init(ByteBuffer byteBuffer)
        {
            final short messageSize = getMessageSize();
            Message.init(byteBuffer, messageSize, ID);
        }
    }

    public static class RegisterRequest
    {
        public static final short ID = 5;

        public static int getMessageSize()
        {
            return Message.HEADER_SIZE;
        }

        public static void init(ByteBuffer byteBuffer)
        {
            final int messageSize = getMessageSize();
            Message.init(byteBuffer, (short)messageSize, ID);
        }

        public static void print(ByteBuffer byteBuffer, StringBuilder sb)
        {
            sb.append("<RegisterRequest> (size=");
            sb.append(Message.getSize(byteBuffer));
            sb.append(") {}\n");
        }
    }

    public static class RegisterReply
    {
        /* long : device id 1
         * long : device id 2
         */
        public static final short ID = 6;

        public static int getMessageSize()
        {
            return (Message.HEADER_SIZE + SIZEOF_LONG + SIZEOF_LONG);
        }

        public static void init(ByteBuffer byteBuffer, UUID deviceId)
        {
            final int messageSize = getMessageSize();
            int pos = Message.init(byteBuffer, (short)messageSize, ID);
            byteBuffer.putLong(pos, deviceId.getMostSignificantBits());
            pos += SIZEOF_LONG;
            byteBuffer.putLong(pos, deviceId.getLeastSignificantBits());
        }

        public static long getDeviceId1(ByteBuffer byteBuffer)
        {
            final int pos = byteBuffer.position();
            return byteBuffer.getLong(pos + Message.HEADER_SIZE);
        }

        public static long getDeviceId2(ByteBuffer byteBuffer)
        {
            final int pos = byteBuffer.position();
            return byteBuffer.getLong(pos + Message.HEADER_SIZE + SIZEOF_LONG);
        }

        public static void print(ByteBuffer byteBuffer, StringBuilder sb)
        {
            sb.append("<RegisterReply> (size=");
            sb.append(Message.getSize(byteBuffer));
            sb.append(") = {\n");

            final long deviceId1 = getDeviceId1(byteBuffer);
            final long deviceId2 = getDeviceId2(byteBuffer);
            final UUID deviceId = new UUID(deviceId1, deviceId2);
            sb.append("   <device id> = ");
            sb.append(deviceId);
            sb.append("\n}\n");
        }
    }

    public static class TrackerLinkRequest
    {
        /* long : device id 1
         * long : device id 2
         */
        public static final short ID = 7;

        public static int getMessageSize()
        {
            return (Message.HEADER_SIZE + SIZEOF_LONG + SIZEOF_LONG);
        }

        public static void init(ByteBuffer byteBuffer, long deviceId1, long deviceId2)
        {
            final int messageSize = getMessageSize();
            int pos = Message.init(byteBuffer, (short)messageSize, ID);
            byteBuffer.putLong(pos, deviceId1);
            pos += SIZEOF_LONG;
            byteBuffer.putLong(pos, deviceId2);
        }

        public static UUID getDeviceId(ByteBuffer byteBuffer)
        {
            int pos = (byteBuffer.position() + Message.HEADER_SIZE);
            final long deviceId1 = byteBuffer.getLong(pos);
            pos += SIZEOF_LONG;
            final long deviceId2 = byteBuffer.getLong(pos);
            return new UUID(deviceId1, deviceId2);
        }

        public static void print(ByteBuffer byteBuffer, StringBuilder sb)
        {
            sb.append("<TrackerLinkRequest> (size=");
            sb.append(Message.getSize(byteBuffer));
            sb.append(") = {\n");

            final UUID uuid = getDeviceId(byteBuffer);
            sb.append("   <device id> = ");
            sb.append(uuid);
            sb.append("\n}\n");
        }
    }

    public static class TrackerLinkReply
    {
        /* int : link code */
        public static final short ID = 8;

        public static int getMessageSize()
        {
            return (Message.HEADER_SIZE + SIZEOF_INT);
        }

        public static void init(ByteBuffer byteBuffer, int linkCode)
        {
            final int messageSize = getMessageSize();
            int pos = Message.init(byteBuffer, (short)messageSize, ID);
            byteBuffer.putInt(pos, linkCode);
        }

        public static int getLinkCode(ByteBuffer byteBuffer)
        {
            final int pos = byteBuffer.position() + Message.HEADER_SIZE;
            return byteBuffer.getInt(pos);
        }

        public static void print(ByteBuffer byteBuffer, StringBuilder sb)
        {
            sb.append("<TrackerLinkReply> (size=");
            sb.append(Message.getSize(byteBuffer));
            sb.append(") = {\n");

            sb.append("   <link code> = ");
            sb.append(getLinkCode(byteBuffer));
            sb.append("\n}\n");
        }
    }

    public static class MonitorLinkRequest
    {
        /* int : link code */
        public static final short ID = 9;

        public static int getMessageSize()
        {
            return (Message.HEADER_SIZE + SIZEOF_INT);
        }

        public static void init(ByteBuffer byteBuffer, int linkCode)
        {
            final int messageSize = getMessageSize();
            final int pos = Message.init(byteBuffer, (short)messageSize, ID);
            byteBuffer.putInt(pos, linkCode);
        }

        public static int getLinkCode(ByteBuffer byteBuffer)
        {
            final int pos = byteBuffer.position();
            return byteBuffer.getInt(pos + Message.HEADER_SIZE);
        }

        public static void print(ByteBuffer byteBuffer, StringBuilder sb)
        {
            sb.append("<MonitorLinkRequest> (size=");
            sb.append(Message.getSize(byteBuffer));
            sb.append(") = {\n");

            sb.append("   <link code> = ");
            sb.append(getLinkCode(byteBuffer));
            sb.append("\n}\n");
        }
    }

    public static class MonitorLinkReply
    {
        /* long : device id 1
         * long : device id 2
         */
        public static final short ID = 10;

        public static int getMessageSize()
        {
            return (Message.HEADER_SIZE + SIZEOF_LONG + SIZEOF_LONG);
        }

        public static void init(ByteBuffer byteBuffer, long deviceId1, long deviceId2)
        {
            final int messageSize = getMessageSize();
            int pos = Message.init(byteBuffer, (short)messageSize, ID);
            byteBuffer.putLong(pos, deviceId1);
            pos += SIZEOF_LONG;
            byteBuffer.putLong(pos, deviceId2);
        }

        public static UUID getDeviceId(ByteBuffer byteBuffer)
        {
            final long deviceId1 = getDeviceId1(byteBuffer);
            final long deviceId2 = getDeviceId2(byteBuffer);
            return new UUID(deviceId1, deviceId2);
        }

        public static long getDeviceId1(ByteBuffer byteBuffer)
        {
            final int pos = (byteBuffer.position() + Message.HEADER_SIZE);
            return byteBuffer.getLong(pos);
        }

        public static long getDeviceId2(ByteBuffer byteBuffer)
        {
            final int pos = (byteBuffer.position() + Message.HEADER_SIZE + SIZEOF_LONG);
            return byteBuffer.getLong(pos);
        }

        public static void print(ByteBuffer byteBuffer, StringBuilder sb)
        {
            sb.append("<MonitorLinkReply> (size=");
            sb.append(Message.getSize(byteBuffer));
            sb.append(") = {\n");

            sb.append("   <device id> = ");
            sb.append(getDeviceId(byteBuffer));
            sb.append("\n}\n");
        }
    }

    public static class StreamOpenRequest extends Message
    {
        /* long : device id 1
         * long : device id 2
         */
        public static final short ID = 11;

        public static short getMessageSize()
        {
            return Message.HEADER_SIZE + SIZEOF_LONG + SIZEOF_LONG;
        }

        public static void init(ByteBuffer byteBuffer, long deviceId1, long deviceId2)
        {
            final short messageSize = getMessageSize();
            final int pos = Message.init(byteBuffer, messageSize, ID);
            byteBuffer.putLong(pos, deviceId1);
            byteBuffer.putLong(pos + SIZEOF_LONG, deviceId2);
        }

        public static UUID getDeviceId(ByteBuffer byteBuffer)
        {
            final int pos = (byteBuffer.position() + Message.HEADER_SIZE);
            final long deviceId1 = byteBuffer.getLong(pos);
            final long deviceId2 = byteBuffer.getLong(pos + SIZEOF_LONG);
            return new UUID(deviceId1, deviceId2);
        }

        public static void print(ByteBuffer byteBuffer, StringBuilder sb)
        {
            sb.append("<StreamOpenRequest> (size=");
            sb.append(Message.getSize(byteBuffer));
            sb.append(") = {\n");

            sb.append("   <device id> = ");
            sb.append(getDeviceId(byteBuffer));
            sb.append("\n}\n");
        }
    }

    public static class ResyncRequest
    {
        /* To be sent by the tracker with a list of messages sent to the server by UDP.
         *
         * long : device id 1
         * long : device id 2
         * byte : number of messages
         * byte : padding
         * long : sequence number of first message
         * int[] : offset relative to the previous sequence number
         */
        public static final short ID = 12;

        public static int getMessageSize(int messages)
        {
            return (Message.HEADER_SIZE + SIZEOF_LONG + SIZEOF_LONG
                    + SIZEOF_BYTE + SIZEOF_BYTE + SIZEOF_LONG + SIZEOF_INT*messages);
        }

        public static int init(ByteBuffer byteBuffer, long deviceId1, long deviceId2, int messages)
        {
            final int messageSize = getMessageSize(messages);
            int pos = Message.init(byteBuffer, (short)messageSize, ID);
            byteBuffer.putLong(pos, deviceId1);
            pos += SIZEOF_LONG;
            byteBuffer.putLong(pos, deviceId2);
            pos += SIZEOF_LONG;
            byteBuffer.put(pos, (byte)messages);
            pos += SIZEOF_BYTE;
            byteBuffer.put(pos, (byte)0);
            pos += SIZEOF_BYTE;
            return pos;
        }

        public static UUID getDeviceId(ByteBuffer byteBuffer)
        {
            int pos = (byteBuffer.position() + Message.HEADER_SIZE);
            final long deviceId1 = byteBuffer.getLong(pos);
            pos += SIZEOF_LONG;
            final long deviceId2 = byteBuffer.getLong(pos);
            return new UUID(deviceId1, deviceId2);
        }

        public static int getMessages(ByteBuffer byteBuffer)
        {
            int pos = (byteBuffer.position() + Message.HEADER_SIZE);
            pos += (SIZEOF_LONG + SIZEOF_LONG);
            return byteBuffer.get(pos);
        }

        public static int getDataPos(ByteBuffer byteBuffer)
        {
            return (byteBuffer.position() + Message.HEADER_SIZE
                    + SIZEOF_LONG + SIZEOF_LONG + SIZEOF_BYTE + SIZEOF_BYTE);
        }

        public static void print(ByteBuffer byteBuffer, StringBuilder sb)
        {
            sb.append("<ResyncRequest> (size=");
            sb.append(Message.getSize(byteBuffer));
            sb.append(") = {\n");

            sb.append("   <device id> = ");
            sb.append(getDeviceId(byteBuffer));
            sb.append('\n');

            int messages = getMessages(byteBuffer);
            sb.append("   <messages> = [");
            sb.append(messages);
            sb.append("] {");

            int pos = (byteBuffer.position() + Message.HEADER_SIZE);
            pos += (SIZEOF_LONG + SIZEOF_LONG + SIZEOF_BYTE + SIZEOF_BYTE);
            if (messages > 0)
            {
                long messageSN = byteBuffer.getLong(pos);
                sb.append(messageSN);
                if (--messages > 0)
                {
                    pos += SIZEOF_LONG;
                    for (; messages>0; messages--)
                    {
                        sb.append(", ");
                        messageSN -= byteBuffer.getInt(pos);
                        sb.append(messageSN);
                        pos += SIZEOF_INT;
                    }
                }
            }
            sb.append("}\n");
            sb.append("}\n");
        }
    }

    public static class ResyncReply
    {
        /* To be sent by the server to ack received messages and request lost messages.
         * byte : number of ack messages
         * byte : number of request messages
         * long : first ack message id
         * int[] : offset relative to the previous id
         * long : first requested message id
         * int[] : offset relative to the previous id
         */

        public static final short ID = 13;

        public static int getSize(int ackMessages, int requestMessages)
        {
            int messageSize = (Message.HEADER_SIZE + SIZEOF_BYTE + SIZEOF_BYTE);
            if (ackMessages > 0)
                messageSize += (SIZEOF_LONG + SIZEOF_INT*ackMessages);
            if (requestMessages > 0)
                messageSize += (SIZEOF_LONG + SIZEOF_INT*requestMessages);
            return messageSize;
        }

        public static int init(ByteBuffer byteBuffer, int ackMessages, int requestMessages)
        {
            final int messageSize = getSize(ackMessages, requestMessages);
            int pos = Message.init(byteBuffer, (short)messageSize, ID);
            byteBuffer.put(pos, (byte)ackMessages);
            pos += SIZEOF_BYTE;
            byteBuffer.put(pos, (byte)requestMessages);
            pos += SIZEOF_BYTE;
            return pos;
        }

        public static int getAckMessages(ByteBuffer byteBuffer)
        {
            final int pos = (byteBuffer.position() + Message.HEADER_SIZE);
            return byteBuffer.get(pos);
        }

        public static int getRequestMessages(ByteBuffer byteBuffer)
        {
            final int pos = (byteBuffer.position() + Message.HEADER_SIZE + SIZEOF_BYTE);
            return byteBuffer.get(pos);
        }

        public static int getDataPos(ByteBuffer byteBuffer)
        {
            return (byteBuffer.position() + Message.HEADER_SIZE + SIZEOF_BYTE + SIZEOF_BYTE);
        }

        private static int print(ByteBuffer byteBuffer, StringBuilder sb, int messages, int pos)
        {
            long messageTime = byteBuffer.getLong(pos);
            sb.append(messageTime);
            pos += SIZEOF_LONG;
            if (--messages > 0)
            {
                for (; messages>0; messages--)
                {
                    sb.append(", ");
                    messageTime -= byteBuffer.getInt(pos);
                    sb.append(messageTime);
                    pos += SIZEOF_INT;
                }
            }
            return pos;
        }

        public static void print(ByteBuffer byteBuffer, StringBuilder sb)
        {
            sb.append("<ResyncReply> (size=");
            sb.append(Message.getSize(byteBuffer));
            sb.append(") = {\n");

            final int ackMessages = getAckMessages(byteBuffer);
            sb.append("   <ack messages> = [");
            sb.append(ackMessages);
            sb.append("] {");

            int pos = (byteBuffer.position() + Message.HEADER_SIZE + SIZEOF_BYTE + SIZEOF_BYTE);
            if (ackMessages > 0)
                pos = print(byteBuffer, sb, ackMessages, pos);

            sb.append("}\n");

            final int requestMessages = getRequestMessages(byteBuffer);
            sb.append("   <request messages> = [");
            sb.append(requestMessages);
            sb.append("] {");

            if (requestMessages > 0)
                print(byteBuffer, sb, requestMessages, pos);

            sb.append("}\n");
            sb.append("}\n");
        }
    }

    public static class Tracking extends Message
    {
        public static final short ID = 16;

        /* tracker -> server message variant
         * has a source device identifier and message sequence number
         *    long : device id 1
         *    long : device id 2
         *    long : message sequence number
         */
        public enum Variant
        {
            TrackerToServer,
            ServerToMonitor
        }

        // tracker app -> server
        public static final int TS_EMPTY_SIZE = Message.HEADER_SIZE +
                                                SIZEOF_LONG + // device id 1
                                                SIZEOF_LONG + // device id 2
                                                SIZEOF_LONG;  // message sequence number

        // server -> monitor device has just a message header

        public static final int FIELD_ID_SIZE = SIZEOF_SHORT;

        /* Each field has a header
         *    byte : length (yes, not greater than 255 bytes)
         *    byte : id
         */
        public static final byte FIELD_BATTERY_LEVEL = 0;    // long + short
        public static final byte FIELD_NETWORK_NAME = 1;     // long + value
        public static final byte FIELD_LOCATION = 2;         // long + double + double + double
        public static final byte FIELD_TRACKING_STOPPED = 3; // long

        public static final int NETWORK_NAME_HEADER_SIZE = (SIZEOF_BYTE + SIZEOF_BYTE + SIZEOF_LONG);

        // tracker app -> server
        public static void init_TS(ByteBuffer byteBuffer, long deviceId1, long deviceId2)
        {
            int pos = byteBuffer.position();
            byteBuffer.putShort(pos, (short)0); // size will be calculated later
            pos += SIZEOF_SHORT;
            byteBuffer.putShort(pos, ID);
            pos += SIZEOF_SHORT;
            byteBuffer.putLong(pos, deviceId1);
            pos += SIZEOF_LONG;
            byteBuffer.putLong(pos, deviceId2);
        }

        // server -> monitor app
        public static int init_SM(ByteBuffer byteBuffer, short size)
        {
            return Message.init(byteBuffer, size, ID);
        }

        public static void setSize_TS(ByteBuffer byteBuffer, short size)
        {
            int pos = byteBuffer.position();
            byteBuffer.putShort(pos, size);
        }

        public static UUID getDeviceId(ByteBuffer byteBuffer)
        {
            int pos = (byteBuffer.position() + HEADER_SIZE);
            final long deviceId1 = byteBuffer.getLong(pos);
            pos += SIZEOF_LONG;
            final long deviceId2 = byteBuffer.getLong(pos);
            return new UUID(deviceId1, deviceId2);
        }

        public static void setMessageSN(ByteBuffer byteBuffer, long messageSN)
        {
            final int pos = (byteBuffer.position() + HEADER_SIZE + SIZEOF_LONG*2);
            byteBuffer.putLong(pos, messageSN);
        }

        public static long getMessageSN(ByteBuffer byteBuffer)
        {
            final int pos = (byteBuffer.position() + HEADER_SIZE + SIZEOF_LONG*2);
            return byteBuffer.getLong(pos);
        }

        // tracker app -> server message variant
        public static int getFieldIterator_TS(ByteBuffer byteBuffer)
        {
            return byteBuffer.position() + TS_EMPTY_SIZE;
        }

        // server -> monitor app message variant
        public static int getFieldIterator_SM(ByteBuffer byteBuffer)
        {
            return byteBuffer.position() + Message.HEADER_SIZE;
        }

        public static int getFieldSize(ByteBuffer byteBuffer, int it)
        {
            return (byteBuffer.get(it) & 0xFF);
        }

        public static byte getFieldId(ByteBuffer byteBuffer, int it)
        {
            return byteBuffer.get(it + 1);
        }

        public static int getBatteryLevelSize()
        {
            return (SIZEOF_BYTE + SIZEOF_BYTE + SIZEOF_LONG + SIZEOF_SHORT);
        }

        public static int addBatteryLevel(ByteBuffer byteBuffer, int pos, long changeTime, short level)
        {
            final int fieldSize = getBatteryLevelSize();
            byteBuffer.put(pos, (byte) fieldSize);
            pos += SIZEOF_BYTE;
            byteBuffer.put(pos, FIELD_BATTERY_LEVEL);
            pos += SIZEOF_BYTE;
            byteBuffer.putLong(pos, changeTime);
            pos += SIZEOF_LONG;
            byteBuffer.putShort(pos, level);
            return (pos + SIZEOF_SHORT);
        }

        public static long getBatteryLevelTime(ByteBuffer byteBuffer, int it)
        {
            return byteBuffer.getLong(it + SIZEOF_BYTE + SIZEOF_BYTE);
        }

        public static short getBatteryLevel(ByteBuffer byteBuffer, int it)
        {
            return byteBuffer.getShort(it + SIZEOF_BYTE + SIZEOF_BYTE + SIZEOF_LONG);
        }

        public static int getNetworkNameSize(ByteBuffer networkName)
        {
            int size = (SIZEOF_BYTE + SIZEOF_BYTE + SIZEOF_LONG);
            if (networkName != null)
                size += networkName.remaining();
            return size;
        }

        public static long getNetworkNameTime(ByteBuffer byteBuffer, int it)
        {
            return byteBuffer.getLong(it + SIZEOF_BYTE + SIZEOF_BYTE);
        }

        public static ByteBuffer getNetworkName(ByteBuffer byteBuffer, int it)
        {
            final int size = (getFieldSize(byteBuffer, it) - NETWORK_NAME_HEADER_SIZE);
            final ByteBuffer networkName = ByteBuffer.allocate(size);
            networkName.order(Protocol.BYTE_ORDER);
            int pos = (it + NETWORK_NAME_HEADER_SIZE);
            for (int c=size; c>0; c--, pos++)
                networkName.put(byteBuffer.get(pos));
            networkName.flip();
            return networkName;
        }

        public static String getNetworkName(ByteBuffer byteBuffer, int it, StringDecoder sd)
        {
            final int fieldSize = getFieldSize(byteBuffer, it);
            final int pos = byteBuffer.position();
            final int limit = byteBuffer.limit();
            byteBuffer.limit(it + fieldSize);
            byteBuffer.position(it + NETWORK_NAME_HEADER_SIZE);
            final String networkName = sd.decode(byteBuffer);
            byteBuffer.limit(limit);
            byteBuffer.position(pos);
            return networkName;
        }

        public static int addNetworkName(ByteBuffer byteBuffer, int pos, long changeTime, ByteBuffer networkName)
        {
            final int networkNameSize = ((networkName == null) ? 0 : networkName.remaining());
            final int fieldSize = (SIZEOF_BYTE + SIZEOF_BYTE + SIZEOF_LONG + networkNameSize);
            byteBuffer.put(pos, (byte)fieldSize);
            pos += SIZEOF_BYTE;
            byteBuffer.put(pos, FIELD_NETWORK_NAME);
            pos += SIZEOF_BYTE;
            byteBuffer.putLong(pos, changeTime);
            pos += SIZEOF_LONG;
            if (networkNameSize > 0)
            {
                int src = networkName.position();
                final int srcEnd = (src + networkNameSize);
                for (; src<srcEnd; src++, pos++)
                    byteBuffer.put(pos, networkName.get(src));
            }
            return pos;
        }

        public static int getLocationSize()
        {
            return (SIZEOF_BYTE + SIZEOF_BYTE + SIZEOF_LONG + SIZEOF_DOUBLE * 3);
        }

        public static int addLocation(ByteBuffer byteBuffer, int pos,
             long locationTime, double latitude, double longitude, double altitude)
        {
            final int fieldSize = getLocationSize();
            byteBuffer.put(pos, (byte)fieldSize);
            pos += SIZEOF_BYTE;
            byteBuffer.put(pos, FIELD_LOCATION);
            pos += SIZEOF_BYTE;
            byteBuffer.putLong(pos, locationTime);
            pos += SIZEOF_LONG;
            byteBuffer.putDouble(pos, latitude);
            pos += SIZEOF_DOUBLE;
            byteBuffer.putDouble(pos, longitude);
            pos += SIZEOF_DOUBLE;
            byteBuffer.putDouble(pos, altitude);
            return (pos + SIZEOF_DOUBLE);
        }

        public static long getLocationTime(ByteBuffer byteBuffer, int it)
        {
            return byteBuffer.getLong(it + SIZEOF_BYTE + SIZEOF_BYTE);
        }

        public static double getLocationLatitude(ByteBuffer byteBuffer, int it)
        {
            return byteBuffer.getDouble(it + SIZEOF_BYTE + SIZEOF_BYTE + SIZEOF_LONG);
        }

        public static double getLocationLongitude(ByteBuffer byteBuffer, int it)
        {
            return byteBuffer.getDouble(it + SIZEOF_BYTE + SIZEOF_BYTE + SIZEOF_LONG + SIZEOF_DOUBLE);
        }

        public static double getLocationAltitude(ByteBuffer byteBuffer, int it)
        {
            return byteBuffer.getDouble(it + SIZEOF_BYTE + SIZEOF_BYTE + SIZEOF_LONG + SIZEOF_DOUBLE + SIZEOF_DOUBLE);
        }

        public static int getTrackingStoppedSize()
        {
            return (SIZEOF_BYTE + SIZEOF_BYTE + SIZEOF_LONG);
        }

        public static int addTrackingStopped(ByteBuffer byteBuffer, int pos, long trackingStoppedTime)
        {
            final int fieldSize = getTrackingStoppedSize();
            byteBuffer.put(pos, (byte)fieldSize);
            pos += SIZEOF_BYTE;
            byteBuffer.put(pos, FIELD_TRACKING_STOPPED);
            pos += SIZEOF_BYTE;
            byteBuffer.putLong(pos, trackingStoppedTime);
            return (pos + SIZEOF_LONG);
        }

        public static long getTrackingStoppedTime(ByteBuffer byteBuffer, int it)
        {
            return byteBuffer.getLong(it + SIZEOF_BYTE + SIZEOF_BYTE);
        }

        public static void print(ByteBuffer byteBuffer, Variant v, StringBuilder sb, StringDecoder sd, DateFormat df)
        {
            sb.append("<tracking> (size=");
            sb.append(Message.getSize(byteBuffer));
            sb.append(") = {\n");

            int it;
            if (v == Variant.TrackerToServer)
            {
                sb.append("   <device id> = ");
                sb.append(getDeviceId(byteBuffer));
                sb.append('\n');

                sb.append("   <message sequence number> = ");
                sb.append(getMessageSN(byteBuffer));
                sb.append('\n');

                it = getFieldIterator_TS(byteBuffer);
            }
            else if (v == Variant.ServerToMonitor)
                it = getFieldIterator_SM(byteBuffer);
            else
                return;

            final int limit = byteBuffer.limit();
            while (it < limit)
            {
                final int fieldSize = getFieldSize(byteBuffer, it);
                if ((it + fieldSize) > limit)
                {
                    sb.append("invalid message: field size less than remaining");
                    break;
                }

                final byte fieldId = getFieldId(byteBuffer, it);
                if (fieldId == FIELD_BATTERY_LEVEL)
                {
                    if (fieldSize < getBatteryLevelSize())
                    {
                        sb.append("invalid message: fieldSize=");
                        sb.append(fieldSize);
                        sb.append(" is less than <battery level> size\n");
                    }
                    else
                    {
                        sb.append("   <battery level> = ");
                        sb.append(df.format(getBatteryLevelTime(byteBuffer, it)));
                        sb.append(", ");
                        sb.append(getBatteryLevel(byteBuffer, it));
                        sb.append("%\n");
                    }
                }
                else if (fieldId == FIELD_NETWORK_NAME)
                {
                    if (fieldSize < NETWORK_NAME_HEADER_SIZE)
                    {
                        sb.append("invalid message: fieldSize=");
                        sb.append(fieldSize);
                        sb.append(" is less than <network name> size\n");
                    }
                    else
                    {
                        sb.append("   <network name> = ");
                        sb.append(df.format(getNetworkNameTime(byteBuffer, it)));
                        sb.append(", ");
                        final String networkName = getNetworkName(byteBuffer, it, sd);
                        if (networkName == StringDecoder.ERROR)
                        {
                            sb.append("failed to decode ");
                            final int pos = (it + SIZEOF_BYTE + SIZEOF_BYTE + SIZEOF_LONG);
                            hexDumpSingleLine(byteBuffer, sb, pos, it+fieldSize);
                        }
                        else
                        {
                            sb.append('\'');
                            sb.append(networkName);
                            sb.append('\'');
                        }
                        sb.append("\n");
                    }
                }
                else if (fieldId == FIELD_LOCATION)
                {
                    if (fieldSize < getLocationSize())
                    {
                        sb.append("invalid message: fieldSize=");
                        sb.append(fieldSize);
                        sb.append(" is less than <location> size\n");
                    }
                    else
                    {
                        sb.append("   <location> ");
                        sb.append(df.format(getLocationTime(byteBuffer, it)));
                        sb.append(", (");
                        sb.append(getLocationLatitude(byteBuffer, it));
                        sb.append(", ");
                        sb.append(getLocationLongitude(byteBuffer, it));
                        sb.append(", ");
                        sb.append(getLocationAltitude(byteBuffer, it));
                        sb.append(")\n");
                    }
                }
                else if (fieldId == FIELD_TRACKING_STOPPED)
                {
                    if (fieldSize < getTrackingStoppedSize())
                    {
                        sb.append("invalid message: fieldSize=");
                        sb.append(fieldSize);
                        sb.append(" is less than <tracking stopped> size\n");
                    }
                    else
                    {
                        sb.append("   <tracking stopped> ");
                        sb.append(df.format(getTrackingStoppedTime(byteBuffer, it)));
                        sb.append('\n');
                    }
                }
                else
                {
                    sb.append("   unknown field ");
                    sb.append(fieldId);
                    sb.append(" (size=");
                    sb.append(fieldSize);
                    sb.append(")\n");
                }

                it += fieldSize;
            }
            sb.append("}\n");
        }
    }
}
