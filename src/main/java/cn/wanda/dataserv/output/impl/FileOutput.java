package cn.wanda.dataserv.output.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import cn.wanda.dataserv.config.el.expression.ExpressionUtils;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.output.Output;
import cn.wanda.dataserv.utils.FtpUtils;
import lombok.extern.log4j.Log4j;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.location.LocalFileLocation;
import cn.wanda.dataserv.output.AbstractOutput;
import cn.wanda.dataserv.output.OutputException;

@Log4j
public class FileOutput extends AbstractOutput implements Output {
    public static final String URL = "url";

    public static final String MD5_SUFFIX = ".md5";

    private BufferedWriter fw;
    private File targetFile = null;
    private String genMd5;

    @Override
    public void writeLine(Line line) {
        try {
            fw.write(line.getLine());
            fw.newLine();
        } catch (IOException e) {
            throw new OutputException(e);
        }
    }

    @Override
    public void post(boolean success) {
        //process params
        LocationConfig l = this.outputConfig.getLocation();
        LocalFileLocation location = (LocalFileLocation) l;

        String filePath = ExpressionUtils.getOneValue(location.getFilePath());
        if (StringUtils.isBlank(filePath)) {
            throw new OutputException("filepath is empty.");
        }
        if (location.getFileResource() != null) {
            String prefix = location.getFileResource().getFilePathPrefix();
            filePath = FtpUtils.getFullPath(prefix, filePath);
        }
        this.genMd5 = location.getGenMd5();

        this.targetFile = new File(filePath);

        if (success && "true".equalsIgnoreCase(this.genMd5)) {
            this.genMD5();
        }
    }

    @Override
    public void close() {
        try {
            if (fw != null) {
                fw.close();
            }

        } catch (IOException e) {
            log.warn("Close File output failed!", e);
        }

    }

    @Override
    public void init() {
        //process params
        LocationConfig l = this.outputConfig.getLocation();

        if (!LocalFileLocation.class.isAssignableFrom(l.getClass())) {
            throw new OutputException(
                    "output config type is not LocalFile!");
        }

        LocalFileLocation location = (LocalFileLocation) l;

        String filePath = ExpressionUtils.getOneValue(location.getFilePath());
        if (StringUtils.isBlank(filePath)) {
            throw new OutputException("filepath is empty.");
        }
        if (location.getFileResource() != null) {
            String prefix = location.getFileResource().getFilePathPrefix();
            filePath = FtpUtils.getFullPath(prefix, filePath);
        }
        this.encoding = this.outputConfig.getEncode();
        this.genMd5 = location.getGenMd5();

        this.targetFile = new File(filePath);

        if (!this.targetFile.exists()) {
            try {

                File parentFile = new File(targetFile.getAbsolutePath().substring(0, targetFile.getAbsolutePath().lastIndexOf(File.separator)));
                if (!parentFile.exists()) {
                    parentFile.mkdirs();
                }
                this.targetFile.createNewFile();
            } catch (IOException e) {
                throw new OutputException(e);
            }
        }

        try {
            fw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(targetFile), encoding));
        } catch (IOException e) {
            throw new OutputException(e);
        }
    }

    private void genMD5() {
        BufferedWriter md5Writer = null;
        try {
            if (!this.targetFile.exists()) {
                log.warn("target file:" + this.targetFile.getPath() + " does not exist.");
                return;
            }
            FileInputStream fis = new FileInputStream(targetFile);

            String MD5 = DigestUtils.md5Hex(fis);

            String md5FileName = this.targetFile.getPath() + MD5_SUFFIX;

            md5Writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(md5FileName), encoding));

            md5Writer.write(MD5 + " " + targetFile.getName());

            log.info("Gen MD5 : " + MD5);
        } catch (IOException e) {
            log.warn("Gen MD5 File failed! ", e);
        } finally {
            if (md5Writer != null) {
                try {
                    md5Writer.close();
                } catch (IOException e) {
                    log.debug(e.getMessage(), e);
                }
            }
        }
    }
}
