package cn.wanda.dataserv.utils;

import java.io.IOException;
import java.net.SocketException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;

public class FtpUtils {

    public final static String separator = "/";

    public static FTPClient connectServer(String ip, int port, String userName, String userPwd, String path) throws IOException {
        FTPClient ftpClient = new FTPClient();
        try {
            ftpClient.connect(ip, port);
            if (!ftpClient.login(userName, userPwd)) {
                throw new IOException("ftp login failed!");
            }
            if (StringUtils.isNotBlank(path)) {
                if (!ftpClient.changeWorkingDirectory(path)) {
                    ftpClient.mkd(path);
                    if (!ftpClient.changeWorkingDirectory(path)) {
                        throw new IOException("ftp change working dir failed! path:" + path);
                    }
                }
            }
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
            ftpClient.setDataTimeout(0);
            ftpClient.enterLocalPassiveMode();
            return ftpClient;
        } catch (SocketException e) {
            throw new IOException("ftp login failed!");
        } catch (IOException e) {
            throw new IOException("ftp login failed!");
        }
    }

    public static String calcFilePath(String ftpPath, String fileName) {

        String path = getFullPath(ftpPath, fileName);

        int lastSeparator = path.lastIndexOf(separator);

        if (lastSeparator < 0) {
            return "";
        }

        return path.substring(0, lastSeparator);
    }

    public static String calcFileName(String fileName) {

        int lastSeparator = fileName.lastIndexOf(separator);

        return fileName.substring(lastSeparator + 1);
    }

    public static String getFullPath(String path, String fileName) {
        if (StringUtils.isBlank(path)) {
            return fileName;
        }
        String prefix = path;
        String suffix = fileName;
        if (path.endsWith(separator)) {
            prefix = path.substring(0, path.lastIndexOf(separator));
        }
        if (fileName.startsWith(separator)) {
            suffix = fileName.substring(fileName.indexOf(separator) + 1);
        }
        return prefix + separator + suffix;
    }
}