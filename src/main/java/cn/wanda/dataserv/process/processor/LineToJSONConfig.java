package cn.wanda.dataserv.process.processor;

import java.util.List;

import cn.wanda.dataserv.config.annotation.Config;
import lombok.Data;

@Data
public class LineToJSONConfig {
    @Config
    private String type;
    @Config(name = "field-names")
    private List<String> fieldNames;
    @Config(name = "field-index")
    private List<String> fieldIndex;
    
}
