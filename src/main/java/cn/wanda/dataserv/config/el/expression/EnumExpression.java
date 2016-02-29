package cn.wanda.dataserv.config.el.expression;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import cn.wanda.dataserv.config.el.FunctionExpression;
import cn.wanda.dataserv.config.el.StringConstant;
import cn.wanda.dataserv.core.Context;
import cn.wanda.dataserv.process.LineWrapper;
import lombok.extern.log4j.Log4j;

import org.apache.commons.lang.StringUtils;


@Log4j
public class EnumExpression implements FunctionExpression {

    private static final String FUNC_NAME = "enum";

    private static final String RANGE_SYMBOL = "~";
    private static final String STEP_SYMBOL = "^";

    private boolean isResoleved;
    private List<String> value = new ArrayList<String>();

    private String[] _enums;

    private String _format;

    @Override
    public boolean resolve(Context context) {

        if (this._enums != null && this._enums.length == 0) {
            return false;
        }

        for (String s : _enums) {
            this.resolveRec(s);
        }

        this.isResoleved = true;

        return this.isResoleved;
    }

    private void resolveRec(String s) {

        int rangePos = s.indexOf(RANGE_SYMBOL);
        int stepPos = s.indexOf(STEP_SYMBOL);


        Integer formatLength = StringUtils.isBlank(this._format) ? 0 : Integer
                .parseInt(this._format);

        DecimalFormat nf = new DecimalFormat();
        nf.setDecimalSeparatorAlwaysShown(false);
        nf.setGroupingUsed(false);
        nf.setMinimumIntegerDigits(formatLength);

        // s is a const string
        if (rangePos == -1) {
            String result = s;
            try {
                Double d = Double.parseDouble(s);
                result = nf.format(d);
            } catch (Exception e) {
                log.debug("enum string:" + s + " is not a num.");
            }
            this.value.add(result);
            return;
        }

        // s is a range expression
        Double _start = Double.parseDouble(s.substring(0, rangePos));
        Double _end;
        if (stepPos == -1) {
            _end = Double.parseDouble(s.substring(rangePos
                    + RANGE_SYMBOL.length()));
        } else {
            _end = Double.parseDouble(s.substring(
                    rangePos + RANGE_SYMBOL.length(), stepPos));
        }
        Double _step = stepPos == -1 ? (double) 1 : Double.parseDouble(s
                .substring(stepPos + STEP_SYMBOL.length()));

        for (; _start < _end; _start += _step) {
            this.value.add(nf.format(_start));
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
        return this.isResoleved;
    }

    @Override
    public String getName() {
        return FUNC_NAME;
    }

    @Override
    public void setArgs(String[] args) {
        if (args.length > 1) {
            this._format = args[args.length - 1];

            this._enums = new String[args.length - 1];

            System.arraycopy(args, 0, this._enums, 0, this._enums.length);
        }
    }

    public static void main(String[] args) {
        DecimalFormat nf = new DecimalFormat();
        nf.setDecimalSeparatorAlwaysShown(false);
        nf.setMinimumIntegerDigits(4);
        nf.setGroupingUsed(false);
        Double a = 3.0;

        System.out.println(nf.format(a));
    }
}