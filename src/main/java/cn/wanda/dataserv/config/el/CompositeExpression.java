package cn.wanda.dataserv.config.el;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import cn.wanda.dataserv.process.LineWrapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Transformer;

import cn.wanda.dataserv.config.Splittable;
import cn.wanda.dataserv.core.AssertUtils;
import cn.wanda.dataserv.core.Context;
import cn.wanda.dataserv.process.LineWrapper;

/**
 * 复合表达式对象，可以是表达式和字符串的组合
 *
 * @author haobowei
 */
public class CompositeExpression implements Expression, Splittable {
    List<Expression> exprList = new ArrayList<Expression>();
    private boolean resolve;
    private List<String> value;

    public CompositeExpression() {
    }

    /**
     * 给定一个已经赋值的复合表达式对象
     *
     * @param exprList2 表达式对象列表
     * @param resolved  是否resolved
     * @param valItem   值
     */
    public CompositeExpression(List<Expression> exprList2, boolean b,
                               String valItem) {
        this.exprList = exprList2;
        this.resolve = b;
        this.value = Arrays.asList(new String[]{valItem});
        ;
    }

    /**
     * 增加一个表达式对象
     *
     * @param e
     */
    public void add(Expression e) {
        this.exprList.add(e);
    }

    public boolean resolve(Context context) {
        this.resolve = true;
        for (Expression e : exprList) {
            resolve &= e.resolve(context);
        }
        if (resolve) {
            LinkedList<StringBuilder> queue = new LinkedList<StringBuilder>();
            queue.add(new StringBuilder());
            for (Expression e : exprList) {
                // cache qSize, it will increase in inner loop;
                int qSzie = queue.size();
                for (int i = 0; i < qSzie; i++) {
                    StringBuilder s = queue.peek();
                    List vals = e.value();
                    if (vals == null || vals.isEmpty()) {
                        continue;
                    } else {
                        queue.pop();
                        for (Object val : vals) {
                            StringBuilder s2 = new StringBuilder(s.toString());
                            queue.add(s2.append(val));
                        }
                    }
                }
            }
            List<String> exprList = new ArrayList<String>();
            CollectionUtils.collect(queue, new Transformer() {

                @Override
                public Object transform(Object arg0) {
                    return arg0.toString();
                }
            }, exprList);
            this.value = exprList;
        }
        return resolve;
    }

    @Override
    public boolean resolved() {
        return resolve;
    }

    @Override
    public Object eval(LineWrapper line) {
        StringBuffer sb = new StringBuffer();
        for (Expression e : exprList) {
            sb.append(e.eval(line));
        }
        return sb;
    }

    public boolean isMulti() {
        return exprList.size() > 1;
    }

    /**
     * 在已知只有一个表达式的情况下返回表达式
     *
     * @return
     */
    public Expression one() {
        AssertUtils.assertTrue(exprList.size() > 0,
                "composite expression cannot be empty");
        return exprList.get(0);
    }

    public List<Object> split() {
        List<Object> retVal = new ArrayList<Object>();
        for (String valItem : this.value) {
            retVal.add(new CompositeExpression(this.exprList, true, valItem));
        }
        return retVal;
    }

    @Override
    public List value() {
        return value;
    }
}
