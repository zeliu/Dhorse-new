package cn.wanda.dataserv.config.resource;

import lombok.Data;
import cn.wanda.dataserv.config.annotation.Config;

/**
 * Created by liuze on 2015/10/30 0030.
 */

@Data
public class EXCELConf {



    @Config(name = "templete-file")
    private String templeteFile;

    @Config(name = "output-file")
    private String outputFile;

    @Config(name = "sheet-name")
    private String sheetName;

    @Config(name = "row-num")
    private String rowNum;

    @Config(name = "column-type")
    private String columnType;

    @Config(name = "field-delim")
    private String fieldDelim;



}
