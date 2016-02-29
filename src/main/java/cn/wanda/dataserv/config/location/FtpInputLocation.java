package cn.wanda.dataserv.config.location;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.el.CompositeExpression;
import cn.wanda.dataserv.config.el.Expression;
import cn.wanda.dataserv.config.el.expression.ExpressionUtils;
import cn.wanda.dataserv.config.resource.FtpServer;
import cn.wanda.dataserv.core.ConfigParseException;
import cn.wanda.dataserv.input.InputException;
import cn.wanda.dataserv.utils.FtpUtils;
import cn.wanda.dataserv.utils.PathPatternMatcher;
import lombok.Data;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

@Data
public class FtpInputLocation extends LocationConfig {

    @Config(name = "ftp")
    private FtpServer ftp;

    /**
     * ftp文件名<br>
     * 使用时会和ftp中path拼接成完整路径<br>
     * 若以"/"结尾，则认为为目录，则会传输目录下所有文件<br>
     * 另，文件名可包含"*","?"以代替一个或多个字符
     */
    @Config(name = "file-name")
    private Expression fileName;

    @Override
    protected List<Object> doSplit() throws InputException, ConfigParseException {
        String fileNameString = ExpressionUtils.getOneValue(fileName);
        if (StringUtils.isBlank(fileNameString)) {
            throw new InputException("fileName is empty.");
        }
        if (fileNameString.endsWith("/")) {
            try {
                FTPClient ftpClient = FtpUtils.connectServer(ftp.getIp(),
                        Integer.parseInt(ftp.getPort()), ftp.getUser(),
                        ftp.getPasswd(),
                        FtpUtils.getFullPath(ftp.getPath(), fileNameString));
                FTPFile[] files = ftpClient.listFiles();

                List<Object> result = new ArrayList<Object>();
                for (FTPFile file : files) {
                    if (!file.isDirectory()) {
                        result.add(getFtpInputLocation(fileNameString + file.getName()));
                    }
                }
                return result;
            } catch (NumberFormatException e) {
                throw new ConfigParseException("Ftp input split failed!", e);
            } catch (IOException e) {
                throw new ConfigParseException("Ftp input split failed!", e);
            }
        } else if (PathPatternMatcher.isPattern(fileNameString)) {
            try {
                String filePath = FtpUtils.calcFilePath(ftp.getPath(), fileNameString);
                FTPClient ftpClient = FtpUtils.connectServer(ftp.getIp(),
                        Integer.parseInt(ftp.getPort()), ftp.getUser(),
                        ftp.getPasswd(), filePath);
                FTPFile[] files = ftpClient.listFiles();

                List<Object> result = new ArrayList<Object>();

                int offset = fileNameString.lastIndexOf(FtpUtils.separator);

                String fileNamePrefix = "";
                if (offset >= 0) {
                    fileNamePrefix = fileNameString.substring(0, offset) + FtpUtils.separator;
                }
                for (FTPFile file : files) {
                    if ((!file.isDirectory()) && (PathPatternMatcher.match(fileNameString, fileNamePrefix + file.getName()))) {
                        result.add(getFtpInputLocation(fileNamePrefix + file.getName()));
                    }
                }
                return result;
            } catch (NumberFormatException e) {
                throw new ConfigParseException("Ftp input split failed!", e);
            } catch (IOException e) {
                throw new ConfigParseException("Ftp input split failed!", e);
            }
        } else {
            return super.doSplit();
        }
    }

    private FtpInputLocation getFtpInputLocation(String fileName) {
        FtpInputLocation result = new FtpInputLocation();
        FtpServer f = new FtpServer();
        f.setIp(ftp.getIp());
        f.setPasswd(ftp.getPasswd());
        f.setPort(ftp.getPort());
        f.setUser(ftp.getUser());
        result.setFtp(f);
        result.setFileName(new CompositeExpression(null, true, fileName));
        return result;
    }
}