package cn.wanda.dataserv.input.impl;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import cn.wanda.dataserv.config.el.expression.ExpressionUtils;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.input.Input;
import cn.wanda.dataserv.input.InputException;
import cn.wanda.dataserv.utils.FtpUtils;
import cn.wanda.dataserv.utils.charset.CharSetUtils;
import lombok.extern.java.Log;
import lombok.extern.log4j.Log4j;

import org.apache.commons.lang.StringUtils;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.el.expression.ExpressionUtils;
import cn.wanda.dataserv.config.location.LocalFileLocation;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.input.AbstractInput;
import cn.wanda.dataserv.input.Input;
import cn.wanda.dataserv.input.InputException;
import cn.wanda.dataserv.utils.FtpUtils;
import cn.wanda.dataserv.utils.charset.CharSetUtils;

@Log4j
public class FileInput extends AbstractInput implements Input {

    public static final String URL = "url";
    private BufferedReader fr;
    private String filePath;

    public Line readLine() {
        try {
            String line = fr.readLine();
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
            if (fr != null) {
                fr.close();
            }
        } catch (IOException e) {
            log.warn(String.format(
                    "Close file input failed:%s,%s", e.getMessage(),
                    e.getCause()));
        }
    }

    @Override
    public void init() {
        //process params
        LocationConfig l = this.inputConfig.getLocation();

        if (!LocalFileLocation.class.isAssignableFrom(l.getClass())) {
            throw new InputException(
                    "input config type is not LocalFile!");
        }

        LocalFileLocation location = (LocalFileLocation) l;

        this.filePath = ExpressionUtils.getOneValue(location.getFilePath());
        if (StringUtils.isBlank(this.filePath)) {
            throw new InputException("filepath is empty.");
        }
        if (location.getFileResource() != null) {
            String prefix = location.getFileResource().getFilePathPrefix();
            this.filePath = FtpUtils.getFullPath(prefix, this.filePath);
        }
        this.encoding = this.inputConfig.getEncode();

        try {
            fr = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), CharSetUtils.getDecoderForName(this.encoding)));
        } catch (FileNotFoundException e) {
            throw new InputException(e);
        }
    }
}
