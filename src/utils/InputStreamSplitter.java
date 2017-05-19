package utils;

import java.io.IOException;
import java.io.InputStream;

/**
 * Splitter, позволяющий разделять поток разделителями из byte-array.
 */
public class InputStreamSplitter {
    private InputStream source;
    private byte[] startDelimiter;
    private int bufferStep;

    private byte[] buf = new byte[0];
    private int pos;
    private int end = 0;
    private int nextPos = 0;
    private int last = 0;
    private boolean hasNext = true;

    public InputStreamSplitter(InputStream source, byte[] startDelimiter, int bufferStep) {
        this.source = source;
        this.startDelimiter = startDelimiter;
        this.bufferStep = bufferStep;
    }

    public InputStreamSplitter(InputStream source, byte[] startDelimiter) {
        this(source, startDelimiter, 4096);
    }

    public boolean readNext() throws IOException {
        if (!hasNext && end == last)
            return false;
        pos = end;
        while (true) {
            end = indexOf(buf, startDelimiter, nextPos, last);
            if (end == -1) {
                if (!hasNext) {
                    // last piece
                    end = last;
                    nextPos = -1;
                    return true;
                }
                if (last == buf.length)
                    getMoreBuf();
                hasNext = readStream();
            } else {
                nextPos = end + startDelimiter.length;
                return true;
            }
        }
    }

    public byte[] getBuf() {
        return buf;
    }

    public int getStart() {
        return pos;
    }

    public int getEnd() {
        return end;
    }

    public int getLength() {
        return end - pos;
    }

    // ------------------------------- Private & protected methods -------------------------------

    private void getMoreBuf() {
        if (pos > 0) {
            // shift buffer contents to zero
            System.arraycopy(buf, pos, buf, 0, last - pos);
            last -= pos;
            nextPos -= pos;
            pos = 0;
        } else {
            // enlarge buffer
            byte[] newBuf = new byte[buf.length + bufferStep];
            System.arraycopy(buf, 0, newBuf, 0, last - pos);
            buf = newBuf;
        }
    }

    private boolean readStream() throws IOException {
        int toRead = buf.length - last;
        while (toRead > 0) {
            if (source.available() == 0)
                return false;
            toRead -= source.read(buf, last, toRead);
            last = buf.length - toRead;
        }
        return source.available() > 0;
    }

    private static int indexOf(byte[] array, byte[] target, int start, int end) {
        if (target.length == 0) {
            return 0;
        }

        int maxi = end - target.length + 1;
             outer:
        for (int i = start; i < maxi; i++) {
            for (int j = 0; j < target.length; j++) {
                if (array[i + j] != target[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

}
