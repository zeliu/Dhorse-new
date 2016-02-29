package cn.wanda.dataserv.config.el.expression;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cn.wanda.dataserv.config.el.FunctionExpression;
import cn.wanda.dataserv.config.el.StringConstant;
import cn.wanda.dataserv.core.Context;
import cn.wanda.dataserv.process.LineWrapper;
import lombok.extern.java.Log;
import lombok.extern.log4j.Log4j;

import org.apache.commons.lang.StringUtils;

import cn.wanda.dataserv.config.el.FunctionExpression;
import cn.wanda.dataserv.config.el.StringConstant;
import cn.wanda.dataserv.core.Context;
import cn.wanda.dataserv.process.LineWrapper;

/**
 * patterns letters seealso: simpleDateFormat<br>
 * time calc format: <code>$(time:var,offset)</code><br>
 * var eg:<code>/app/ecom/rigelci/uc/uctest/yyyyMMdd/HHmm</code><br>
 * var regex:<code>(yyyy|MM|dd|HH|mm|ss)</code><br>
 * offset eg:<code>1y-1d-1M+1H-30m</code><br>
 * offset regex:<code>([+|-]?)(\\d+)(y|M|d|H|m|s|w)</code><br>
 * tips: <code>1y-1d-1M+1H-30m</code> and <code>1y-1M-1d+1H-30m</code> has different result
 *
 * @author songqian
 * @see SimpleDateFormat
 */
@Log4j
public class TimeExpression implements FunctionExpression {

    private static final String FUNC_NAME = "time";

    private static final String RUNTIME_NAME = "runtime";
    private static final String DATETIME_NAME = "datatime";

    private boolean isResolved = false;

    private Calendar calendar;

    private static final String DIFF_PATTERN = "([+|-]?)(\\d+)(y|M|d|H|m|s|w)";

    private static final String TIME_PATTERN = "(yyyy|MM|dd|HH|mm|ss)";

    private static final String INPUT_PATTERN = "yyyyMMddHHmmss";

    /**
     * @see SimpleDateFormat
     */
    private String pattern;

    private String diff;

    private String value = "";

    /**
     * can spilt for multi value
     */
    @Override
    public List value() {
        List<String> result = new ArrayList<String>();
        result.add(value);
        return result;
    }

    @Override
    public Object eval(LineWrapper line) {
        if (StringUtils.isBlank(pattern)) {
            return null;
        } else {
            return value;
        }
    }

    @Override
    public boolean resolved() {
        return isResolved;
    }

    @Override
    public boolean resolve(Context context) {
        if (StringUtils.isBlank(this.pattern)) {
            log.info("input args is empty!");
            this.isResolved = true;
            return true;
        }
        Map runtime = (Map) context.get(RUNTIME_NAME);
        String dataTime = null;
        if (runtime != null && runtime.get(DATETIME_NAME) != null) {
            dataTime = runtime.get(DATETIME_NAME).toString();
        } else {
            //TO DO throw exception now, could get datatime from other way
//			throw new ExpressionException("no datatime in context for timeexpression!");
            SimpleDateFormat sdf = new SimpleDateFormat(INPUT_PATTERN);
            dataTime = sdf.format(new Date());
        }
        getDataTime(dataTime);

        this.value = this.pattern;
        calcCalendar();
        parse();

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
            this.pattern = args[0];
        }
        if (args.length > 1 && StringUtils.isNotBlank(args[1])) {
            this.diff = args[1];
        }
    }

    /**
     * @param dataTime : INPUT_PATTERN yyyyMMddHHmmss
     * @throws ParseException
     * @see SimpleDateFormat
     */
    private void getDataTime(String dataTime) {
        SimpleDateFormat sdf = new SimpleDateFormat(INPUT_PATTERN);
        try {
            Date date = sdf.parse(dataTime);
            this.calendar = Calendar.getInstance();
            this.calendar.setTime(date);
        } catch (ParseException e) {
            throw new ExpressionException("pattern of datatime for timeexpression should by : yyyyMMddHHmmss", e);
        }
    }

    private void calcCalendar() {
        if (StringUtils.isBlank(this.diff)) {
            return;
        }
        Pattern p = Pattern.compile(DIFF_PATTERN);
        Matcher m = p.matcher(this.diff);
        while (m.find()) {
            String op = m.group(1);
            String num = m.group(2);
            String unitString = m.group(3);
            int unit = Calendar.DAY_OF_MONTH;
            if ("y".equals(unitString)) {
                unit = Calendar.YEAR;
            } else if ("M".equals(unitString)) {
                unit = Calendar.MONTH;
            } else if ("d".equals(unitString)) {
                unit = Calendar.DAY_OF_MONTH;
            } else if ("H".equals(unitString)) {
                unit = Calendar.HOUR_OF_DAY;
            } else if ("m".equals(unitString)) {
                unit = Calendar.MINUTE;
            } else if ("s".equals(unitString)) {
                unit = Calendar.SECOND;
            } else if ("w".equals(unitString)) {
                unit = Calendar.WEEK_OF_YEAR;
            }

            if ("-".equals(op)) {
                this.calendar.add(unit, 0 - Integer.parseInt(num));
            } else {
                this.calendar.add(unit, Integer.parseInt(num));
            }
        }
    }

    private void parse() {
        if (StringUtils.isBlank(this.pattern)) {
            return;
        }
        Pattern p = Pattern.compile(TIME_PATTERN);
        Matcher m = p.matcher(this.pattern);
        while (m.find()) {
            String t = m.group();
            SimpleDateFormat sdf = new SimpleDateFormat(t);
            this.value = this.value.replaceFirst(t, sdf.format(calendar.getTime()));
        }

    }

    @Override
    public List<Object> split() {
        List<Object> result = new ArrayList<Object>();
        result.add(new StringConstant(this.value));
        return result;
    }
}