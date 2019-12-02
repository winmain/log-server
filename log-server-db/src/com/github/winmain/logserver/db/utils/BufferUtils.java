package com.github.winmain.logserver.db.utils;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class BufferUtils {
    static public final Charset DefaultCharset = StandardCharsets.UTF_8;

    static public void putInt(ByteBuffer buf, int v) {
        if (v >= 0) {
            if (v < 0x80) {
                buf.put((byte) v);
                return;
            } else if (v < 0x4000) {
                buf.put((byte) ((v >> 8) | 0x80));
                buf.put((byte) (v & 0xff));
                return;
            } else if (v < 0x200000) {
                buf.put((byte) ((v >> 16) | 0xc0));
                buf.put((byte) (v >> 8));
                buf.put((byte) v);
                return;
            } else if (v < 0x10000000) {
                buf.put((byte) ((v >> 24) | 0xe0));
                buf.put((byte) (v >> 16));
                buf.put((byte) (v >> 8));
                buf.put((byte) v);
                return;
            }
        }
        buf.put((byte) 0xff);
        buf.put((byte) (v >> 24));
        buf.put((byte) (v >> 16));
        buf.put((byte) (v >> 8));
        buf.put((byte) v);
    }

    static public int getInt(ByteBuffer buf) {
        byte b = buf.get();
        if ((b & 0x80) == 0) {
            return b;
        } else if ((b & 0xc0) == 0x80) {
            return ((b & 0x3f) << 8) | (buf.get() & 0xff);
        } else if ((b & 0xe0) == 0xc0) {
            return ((b & 0x1f) << 16) | ((buf.get() & 0xff) << 8) | (buf.get() & 0xff);
        } else if ((b & 0xf0) == 0xe0) {
            return ((b & 0x0f) << 24) | ((buf.get() & 0xff) << 16) | ((buf.get() & 0xff) << 8) | (buf.get() & 0xff);
        } else if (b != (byte) 0xff) {
            throw new RuntimeException("Invalid int prefix: " + (int) b);
        } else {
            return (buf.get() << 24) | ((buf.get() & 0xff) << 16) | ((buf.get() & 0xff) << 8) | (buf.get() & 0xff);
        }
    }

    static public void putString(ByteBuffer buf, String s) {
        byte[] bytes = s.getBytes(DefaultCharset);
        putInt(buf, bytes.length);
        buf.put(bytes);
    }

    static public String getString(ByteBuffer buf) {
        int length = getInt(buf);
        int pos = buf.position();
        String result = new String(buf.array(), buf.arrayOffset() + pos, length, DefaultCharset);
        buf.position(pos + length);
        return result;
    }
}
