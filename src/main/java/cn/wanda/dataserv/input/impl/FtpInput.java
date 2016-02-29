package cn.wanda.dataserv.input.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import cn.wanda.dataserv.config.el.expression.ExpressionUtils;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.input.Input;
import cn.wanda.dataserv.input.InputException;
import cn.wanda.dataserv.utils.FtpUtils;
import cn.wanda.dataserv.utils.charset.CharSetUtils;
import lombok.extern.log4j.Log4j;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.ftp.FTPClient;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.el.expression.ExpressionUtils;
import cn.wanda.dataserv.config.location.FtpInputLocation;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.input.AbstractInput;
import cn.wanda.dataserv.input.Input;
import cn.wanda.dataserv.input.InputException;
import cn.wanda.dataserv.utils.FtpUtils;
import cn.wanda.dataserv.utils.charset.CharSetUtils;

@Log4j
public class FtpInput extends AbstractInput implements Input {

    private final static String anonymoususer = "anonymous";
    private final static String anonymouspasswd = "anonymous";

    private FtpInputLocation location;

    private String ip;

    private int port;

    private String user;

    private String passwd;

    private String path;

    private FTPClient ftpClient;

    private BufferedReader reader;
    private String fileName;

    @Override
    public void init() {

        log.info("init ftp input...");

        LocationConfig l = this.inputConfig.getLocation();

        if (!FtpInputLocation.class.isAssignableFrom(l.getClass())) {
            throw new InputException("input config type is not Ftp!");
        }

        location = (FtpInputLocation) l;

        this.encoding = this.inputConfig.getEncode();
        this.ip = this.location.getFtp().getIp();
        this.port = Integer.parseInt(this.location.getFtp().getPort());
        this.user = this.location.getFtp().getUser();
        this.passwd = this.location.getFtp().getPasswd();
        String ftpPath = this.location.getFtp().getPath();
        String fileNameP = ExpressionUtils.getOneValue(this.location.getFileName());
        if (StringUtils.isBlank(fileNameP)) {
            throw new InputException("ftp file name is empty!");
        }

        this.path = FtpUtils.calcFilePath(ftpPath, fileNameP);

        this.fileName = FtpUtils.calcFileName(fileNameP);

        if (StringUtils.isBlank(this.fileName)) {
            throw new InputException("ftp input init failed: input path is a folder!");
        }

        if (StringUtils.isBlank(ip)) {
            throw new InputException("ftp url is empty!");
        }
        if (StringUtils.isBlank(user)) {
            this.user = anonymoususer;
            this.passwd = anonymouspasswd;
        }

        try {
            this.ftpClient = FtpUtils.connectServer(ip, port, user, passwd, path);
//			this.ftpClient.setBufferSize(65535);
        } catch (IOException e) {
            throw new InputException("ftp input init failed!", e);
        }

        try {
            InputStream is = this.ftpClient.retrieveFileStream(this.fileName);
            if (is == null) {
                throw new InputException("source file not exist.");
            }
            reader = new BufferedReader(new InputStreamReader(is, CharSetUtils.getDecoderForName(this.encoding)));
        } catch (UnsupportedEncodingException e) {
            throw new InputException("ftp input init failed!", e);
        } catch (IOException e) {
            throw new InputException("ftp input init failed!", e);
        }

        log.info("init ftp input finish.");
    }

    @Override
    public Line readLine() {
        try {
            String line = reader.readLine();
            if (line != null) {
                return new Line(line);
            } else {
                return Line.EOF;
            }
        } catch (IOException e) {
            throw new InputException(e);
        }
    }

    @Override
    public void close() {
        try {
            if (reader != null) {
                reader.close();
            }
            if (ftpClient != null) {
                ftpClient.logout();
                ftpClient.disconnect();
            }
        } catch (IOException e) {
            log.warn(String.format(
                    "Close ftp input failed:%s,%s", e.getMessage(),
                    e.getCause()));
        }
    }

}