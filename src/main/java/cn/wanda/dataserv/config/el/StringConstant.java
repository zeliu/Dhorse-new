package cn.wanda.dataserv.config.el;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import cn.wanda.dataserv.process.LineWrapper;
import cn.wanda.dataserv.core.Context;
import cn.wanda.dataserv.process.LineWrapper;

public class StringConstant implements Expression {

    private String string;

    public StringConstant(String s) {
        this.string = s;
    }

    @Override
    public boolean resolve(Context context) {
        return true;
    }

    @Override
    public Object eval(LineWrapper line) {
        return string;
    }

    @Override
    public boolean resolved() {
        // always true
        return true;
    }

    @Override
    public List value() {
        // TODO Auto-generated method stub
        return Arrays.asList(new String[]{string});
    }

    @Override
    public List<Object> split() {
        List<Object> result = new ArrayList<Object>();
        result.add(new StringConstant(string));
        return result;
    }

}
