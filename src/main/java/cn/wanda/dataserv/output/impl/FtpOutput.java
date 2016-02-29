package cn.wanda.dataserv.output.impl;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

import cn.wanda.dataserv.config.el.expression.ExpressionUtils;
import cn.wanda.dataserv.config.location.FtpOutputLocation;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.output.Output;
import cn.wanda.dataserv.utils.FtpUtils;
import lombok.extern.log4j.Log4j;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.ftp.FTPClient;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.output.AbstractOutput;
import cn.wanda.dataserv.output.OutputException;

@Log4j
public class FtpOutput extends AbstractOutput implements Output {

    private final static String anonymoususer = "anonymous";
    private final static String anonymouspasswd = "anonymous";
    private final static String defaultFileName = "0000001";

    private FtpOutputLocation location;

    private String ip;

    private int port;

    private String user;

    private String passwd;

    private String path;

    private FTPClient ftpClient;

    private BufferedWriter writer;
    private String fileName;

    @Override
    public void init() {

        log.info("init ftp output...");

        LocationConfig l = this.outputConfig.getLocation();

        if (!FtpOutputLocation.class.isAssignableFrom(l.getClass())) {
            throw new OutputException("output config type is not Ftp!");
        }

        location = (FtpOutputLocation) l;

        this.encoding = this.outputConfig.getEncode();
        this.ip = this.location.getFtp().getIp();
        this.port = Integer.parseInt(this.location.getFtp().getPort());
        this.user = this.location.getFtp().getUser();
        this.passwd = this.location.getFtp().getPasswd();
        String ftpPath = this.location.getFtp().getPath();
        String fileName = ExpressionUtils.getOneValue(this.location.getFileName());
        if (StringUtils.isBlank(fileName)) {
            throw new OutputException("ftp file name is empty!");
        }

        this.path = FtpUtils.calcFilePath(ftpPath, fileName);
        this.fileName = FtpUtils.calcFileName(fileName);

        if (StringUtils.isBlank(this.fileName)) {
            this.fileName = defaultFileName;
        }
        if (StringUtils.isBlank(ip)) {
            throw new OutputException("ftp url is empty!");
        }
        if (StringUtils.isBlank(user)) {
            this.user = anonymoususer;
            this.passwd = anonymouspasswd;
        }

        try {
            this.ftpClient = FtpUtils.connectServer(ip, port, user, passwd, path);
        } catch (IOException e) {
            throw new OutputException("ftp output init failed!", e);
        }

        try {
            //delete target file if exist
            ftpClient.deleteFile(this.fileName);
        } catch (IOException e) {
            throw new OutputException("ftp output init failed!", e);
        }

        try {
            writer = new BufferedWriter(new OutputStreamWriter(this.ftpClient.appendFileStream(this.fileName), encoding));
        } catch (UnsupportedEncodingException e) {
            throw new OutputException("ftp output init failed!", e);
        } catch (IOException e) {
            throw new OutputException("ftp output init failed!", e);
        }

        log.info("init ftp output finish.");
    }

    @Override
    public void close() {
        try {
            if (writer != null) {
                writer.close();
            }
            if (ftpClient != null) {
                ftpClient.logout();
                ftpClient.disconnect();
            }
        } catch (IOException e) {
            log.warn("Close Ftp output failed!", e);
        }
    }

    @Override
    public void writeLine(Line line) {
        try {
            writer.write(line.getLine());
            writer.newLine();
        } catch (IOException e) {
            throw new OutputException(e);
        }
    }

}