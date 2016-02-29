package cn.wanda.dataserv.config.el.expression;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import cn.wanda.dataserv.config.el.FunctionExpression;
import cn.wanda.dataserv.config.el.StringConstant;
import cn.wanda.dataserv.core.Context;
import cn.wanda.dataserv.process.LineWrapper;
import org.apache.commons.lang.StringUtils;

public class RangeExpression implements FunctionExpression {

    private static final String FUNC_NAME = "range";

    private String _start;
    private String _end;
    private String _step;
    private String _formatlength;

    private boolean isResolved = false;

    private List<String> value = new ArrayList<String>();

    @Override
    public List value() {
        return this.value;
    }

    @Override
    public Object eval(LineWrapper line) {
        if (this.value.size() > 0) {
            return this.value.get(0);
        }
        return null;
    }

    @Override
    public boolean resolved() {
        return this.isResolved;
    }

    @Override
    public boolean resolve(Context context) {

        Double start = Double.parseDouble(_start);
        Double end = StringUtils.isBlank(this._end) ? start : Double.parseDouble(_end);
        Double step = StringUtils.isBlank(this._step) ? 1.0 : Double.parseDouble(_step);
        Integer formatLength = StringUtils.isBlank(this._formatlength) ? 0 : Integer.parseInt(this._formatlength);

        DecimalFormat nf = new DecimalFormat();
        nf.setDecimalSeparatorAlwaysShown(false);
        nf.setGroupingUsed(false);
        nf.setMinimumIntegerDigits(formatLength);

        for (; start <= end; start += step) {
            this.value.add(nf.format(start));
        }
        this.isResolved = true;
        return true;
    }

    @Override
    public String getName() {
        return FUNC_NAME;
    }

    @Override
    public void setArgs(String[] args) {
        if (args.length > 0 && StringUtils.isNotBlank(args[0])) {
            this._start = args[0];
        }
        if (args.length > 1 && StringUtils.isNotBlank(args[1])) {
            this._end = args[1];
        }
        if (args.length > 2 && StringUtils.isNotBlank(args[2])) {
            this._step = args[2];
        }
        if (args.length > 3 && StringUtils.isNotBlank(args[3])) {
            this._formatlength = args[3];
        }
    }

    @Override
    public List<Object> split() {
        List<Object> result = new ArrayList<Object>();
        for (String s : this.value) {
            result.add(new StringConstant(s));
        }
        return result;
    }

}