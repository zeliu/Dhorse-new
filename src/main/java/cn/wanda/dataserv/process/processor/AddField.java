package cn.wanda.dataserv.process.processor;

import cn.wanda.dataserv.config.FieldConfig;
import cn.wanda.dataserv.config.el.Expression;
import cn.wanda.dataserv.process.LineSchema;
import cn.wanda.dataserv.process.LineWrapper;
import lombok.extern.java.Log;

import cn.wanda.dataserv.process.ProcessorSupport;

@Log
public class AddField extends ProcessorSupport {


    private AddFieldConfig config;

    public AddField() {
        super();
    }

    @Override
    public void setConfig(Object config) {
        this.config = (AddFieldConfig) config;
    }

    @Override
    protected LineSchema processSchema(LineSchema lineSchema) {

        FieldConfig f = config.getSchema();
        String beforeName = config.getBefore();
        String afterName = config.getAfter();
        if (beforeName != null) {
            int pos = lineSchema.getIndex(beforeName);
            lineSchema.addField(pos, f);
            if (afterName != null) {
                log.warning("use before property and ignore after property");
            }
        } else if (afterName != null) {
            int pos = lineSchema.getIndex(afterName);
            lineSchema.addField(pos + 1, f);
        } else {
            lineSchema.addField(f);
        }
        return lineSchema;
    }

    @Override
    public LineWrapper processLine(LineWrapper line) {
        Expression expr = config.getExpr();
        String value = String.valueOf(expr.eval(line));

        String beforeName = config.getBefore();
        String afterName = config.getAfter();
        if (beforeName != null) {
            if (afterName != null) {
                log.warning("use before property and ignore after property");
            }
            int pos = schema.getIndex(beforeName);
            line.addField(pos, value);
        } else if (afterName != null) {
            int pos = schema.getIndex(afterName);
            line.addField(pos + 1, value);
        } else {
            line.addField(value);
        }

        return line;
    }

}
