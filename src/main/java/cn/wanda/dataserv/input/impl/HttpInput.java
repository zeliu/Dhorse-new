package cn.wanda.dataserv.input.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import cn.wanda.dataserv.config.el.expression.ExpressionUtils;
import cn.wanda.dataserv.core.Line;
import lombok.extern.log4j.Log4j;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.el.expression.ExpressionUtils;
import cn.wanda.dataserv.config.location.HttpInputLocation;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.input.AbstractInput;
import cn.wanda.dataserv.input.Input;
import cn.wanda.dataserv.input.InputException;
import cn.wanda.dataserv.utils.charset.CharSetUtils;

@Log4j
public class HttpInput extends AbstractInput implements Input {

    private final static char noLineDelim = 0x7F;
    private final static int defaultBufferSize = 80;

    private HttpInputLocation location;
    private char lineDelim;
    private String httpURL = "";
    private int bufferSize;

    private HttpInputStrategy strategy;

    @Override
    public void init() {

        log.info("init http input...");

        LocationConfig l = this.inputConfig.getLocation();

        if (!HttpInputLocation.class.isAssignableFrom(l.getClass())) {
            throw new InputException("input config type is not Http!");
        }

        location = (HttpInputLocation) l;

        this.lineDelim = location.getLineDelim();
        this.encoding = inputConfig.getEncode();
        this.httpURL = location.getHttp().getHttpURL();
        this.bufferSize = location.getBufferSize();

        this.httpURL = this.httpURL + ExpressionUtils.getOneValue(location.getSuffix());

        if (StringUtils.isBlank(httpURL)) {
            throw new InputException("http url is empty!");
        }

        if (lineDelim == noLineDelim) {
            this.strategy = new HttpNativeLineStrategy();
        } else {
            this.strategy = new HttpBufferedStrategy();
        }
        try {
            this.strategy.open();
        } catch (IOException e) {
            throw new InputException("input init failed!", e);
        }

        log.info("init http input finish.");
    }

    @Override
    public Line readLine() {
        try {
            Line line = strategy.readLine();
            if (line != null) {
                return line;
            } else {
                return Line.EOF;
            }
        } catch (Exception e) {
            throw new InputException(e);
        }

    }

    @Override
    public void close() {
        if (strategy != null) {
            this.strategy.close();
        }

    }

    interface HttpInputStrategy {
        int open() throws IOException;

        Line readLine() throws IOException;

        void close();
    }

    class HttpNativeLineStrategy implements HttpInputStrategy {

        private BufferedReader bs;

        @Override
        public int open() throws IOException {
            URL url = new URL(httpURL);
            bs = new BufferedReader(new InputStreamReader(url.openStream(),
                    CharSetUtils.getDecoderForName(encoding)));
            return 0;
        }

        @Override
        public Line readLine() throws IOException {
            String s = bs.readLine();
            if (s != null) {
                return new Line(s);
            } else {
                return null;
            }
        }

        @Override
        public void close() {
            try {
                IOUtils.closeStream(bs);
            } catch (Exception e) {
                log.warn(String.format(
                        "Close http input failed:%s,%s", e.getMessage(),
                        e.getCause()));
            }

        }
    }

    class HttpBufferedStrategy implements HttpInputStrategy {

        private InputStreamReader reader;

        private char cb[];
        private int nChars, nextChar;

        private static final int INVALIDATED = -2;
        private static final int UNMARKED = -1;
        private int markedChar = UNMARKED;
        private int readAheadLimit = 0; /* Valid only when markedChar > 0 */


        @Override
        public int open() throws IOException {
            URL url = new URL(httpURL);
            reader = new InputStreamReader(url.openStream(), CharSetUtils.getDecoderForName(encoding));
            cb = new char[bufferSize > 0 ? bufferSize : defaultBufferSize];
            nextChar = nChars = 0;
            return 0;
        }

        @Override
        public Line readLine() throws IOException {
            String s = this.readString();
            if (s == null) {
                return null;
            } else {
                return new Line(s);
            }
        }

        @Override
        public void close() {
            try {
                IOUtils.closeStream(reader);
            } catch (Exception e) {
                log.warn(String.format(
                        "Close http input failed:%s,%s", e.getMessage(),
                        e.getCause()), e);
            }
        }

        private String readString() throws IOException {

            StringBuffer s = null;
            int startChar;

            if (reader == null) {
                throw new InputException("Stream closed");
            }

            for (; ; ) {

                if (nextChar >= nChars)
                    fill();
                if (nextChar >= nChars) { /* EOF */
                    if (s != null && s.length() > 0)
                        return s.toString();
                    else
                        return null;
                }
                boolean eol = false;
                char c = 0;
                int i;

                charLoop:
                for (i = nextChar; i < nChars; i++) {
                    c = cb[i];
                    if (c == lineDelim) {
                        eol = true;
                        break charLoop;
                    }
                }

                startChar = nextChar;
                nextChar = i;

                if (eol) {
                    String str;
                    if (s == null) {
                        str = new String(cb, startChar, i - startChar);
                    } else {
                        s.append(cb, startChar, i - startChar);
                        str = s.toString();
                    }
                    nextChar++;
                    return str;
                }

                if (s == null)
                    s = new StringBuffer(defaultBufferSize);
                s.append(cb, startChar, i - startChar);
            }
        }

        private void fill() throws IOException {
            int dst;
            if (markedChar <= UNMARKED) {
                /* No mark */
                dst = 0;
            } else {
				/* Marked */
                int delta = nextChar - markedChar;
                if (delta >= readAheadLimit) {
					/* Gone past read-ahead limit: Invalidate mark */
                    markedChar = INVALIDATED;
                    readAheadLimit = 0;
                    dst = 0;
                } else {
                    if (readAheadLimit <= cb.length) {
						/* Shuffle in the current buffer */
                        System.arraycopy(cb, markedChar, cb, 0, delta);
                        markedChar = 0;
                        dst = delta;
                    } else {
						/* Reallocate buffer to accommodate read-ahead limit */
                        char ncb[] = new char[readAheadLimit];
                        System.arraycopy(cb, markedChar, ncb, 0, delta);
                        cb = ncb;
                        markedChar = 0;
                        dst = delta;
                    }
                    nextChar = nChars = delta;
                }
            }

            int n;
            do {
                n = reader.read(cb, dst, cb.length - dst);
            } while (n == 0);
            if (n > 0) {
                nChars = dst + n;
                nextChar = dst;
            }
        }

    }
}