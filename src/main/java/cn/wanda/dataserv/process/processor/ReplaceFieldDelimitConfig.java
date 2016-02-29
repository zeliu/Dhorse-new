package cn.wanda.dataserv.process.processor;

import cn.wanda.dataserv.config.Resolvable;
import cn.wanda.dataserv.config.annotation.Config;
import cn.wanda.dataserv.config.annotation.Escape;
import cn.wanda.dataserv.core.Context;
import cn.wanda.dataserv.utils.escape.JavaEscape;
import lombok.Data;

/**
 * Created by songzhuozhuo on 2015/3/4
 */
@Data
public class ReplaceFieldDelimitConfig implements Resolvable {
    private String type;

    @Config(name = "field-delimit")
    private String fieldDelimit;

    private boolean resolved = false;

    public boolean resolved() {
        return this.resolved;
    }

    @Override
    public boolean resolve(Context context) {
        this.fieldDelimit = JavaEscape.unescapeJava(fieldDelimit);
        this.resolved = true;
        return true;
    }
}
