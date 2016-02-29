package cn.wanda.dataserv.config.location;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.el.CompositeExpression;
import cn.wanda.dataserv.config.el.Expression;
import cn.wanda.dataserv.config.el.expression.ExpressionUtils;
import cn.wanda.dataserv.core.ConfigParseException;
import lombok.Data;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.annotation.RequiredType;
import cn.wanda.dataserv.config.resource.HdfsConf;
import cn.wanda.dataserv.input.InputException;
import cn.wanda.dataserv.utils.DFSUtils;
import cn.wanda.dataserv.utils.FtpUtils;
import cn.wanda.dataserv.utils.PathPatternMatcher;

@Data
public class HdfsInputLocation extends LocationConfig {

    @Config(name = "hdfs", require = RequiredType.OPTIONAL)
    private HdfsConf hdfs = new HdfsConf();

    @Config(name = "buffer-size", require = RequiredType.OPTIONAL)
    private Integer bufferSize = 4 * 1024;
    /**
     * ignore key if inputfile type is Sequeuence File
     */
    @Config(name = "ignore-key", require = RequiredType.OPTIONAL)
    private Boolean ignoreKey = true;

    /**
     * input file in hdfs<br>
     * 其中文件名部分可包含"*"或"?"以进行模糊匹配，文件夹部分不支持模糊匹配
     */
    @Config(name = "path")
    private Expression path;
    /**
     * default is org.apache.hadoop.io.compress.DefaultCodec<br>
     * support:<br>
     * org.apache.hadoop.io.compress.DefaultCodec<br>
     * org.apache.hadoop.io.compress.GzipCodec<br>
     * org.apache.hadoop.io.compress.BZip2Codec<br>
     * org.apache.hadoop.io.compress.LzopCodec<br>
     * org.apache.hadoop.io.compress.LzoCodec<br>
     * org.apache.hadoop.io.compress.LzmaCodec<br>
     * org.apache.hadoop.io.compress.QuickLzCodec<br>
     */
    @Config(name = "codec-class", require = RequiredType.OPTIONAL)
    private String codecClass = DFSUtils.DefaultCodec;
    /**
     * include:
     * txt,seq,txt_comp,rcfile
     */
    @Config(name = "file-type", require = RequiredType.OPTIONAL)
    private String fileType = DFSUtils.TXT;

    @Override
    public List<Object> doSplit() {
        try {
            String pathString = ExpressionUtils.getOneValue(path);
            if (StringUtils.isBlank(pathString)) {
                throw new InputException("path is empty.");
            }
            List<Object> result = new ArrayList<Object>();

            if (PathPatternMatcher.isPattern(getFileName(pathString))) {
                int offset = pathString.lastIndexOf(FtpUtils.separator);
                String pathPrefix = "";
                if (offset >= 0) {
                    pathPrefix = pathString.substring(0, offset) + FtpUtils.separator;
                }
                Configuration c = DFSUtils.getConf(pathPrefix, hdfs.getUgi(), hdfs);
                List<String> pathList = DFSUtils.getPathList(pathPrefix, c);
                for (String subPath : pathList) {
                    if (!PathPatternMatcher.match(getFileName(pathString), getFileName(subPath))) {
                        continue;
                    }
                    result.add(getDfsLocation(subPath));
                }
            } else {
                Configuration c = DFSUtils.getConf(pathString, hdfs.getUgi(), hdfs);
                List<String> pathList = DFSUtils.getPathList(pathString, c);
                for (String subPath : pathList) {
                    result.add(getDfsLocation(subPath));
                }
            }
            return result;
        } catch (IOException e) {
            throw new ConfigParseException("Hdfs input split failed!", e);
        } catch (URISyntaxException e) {
            throw new ConfigParseException("Hdfs input split failed!", e);
        }
    }

    private static String getFileName(String fullPath) {
        int offset = fullPath.lastIndexOf(FtpUtils.separator);

        return fullPath.substring(offset + 1);
    }

    private HdfsInputLocation getDfsLocation(String filePath) {
        HdfsInputLocation dfsLocation = new HdfsInputLocation();

        dfsLocation.setBufferSize(bufferSize);
        HdfsConf h = new HdfsConf();
        h.setUgi(hdfs.getUgi());
        h.setHadoopConf(hdfs.getHadoopConf());
        dfsLocation.setHdfs(h);
        dfsLocation.setPath(new CompositeExpression(null, true, filePath));
        dfsLocation.setFileType(fileType);

        return dfsLocation;
    }
}
