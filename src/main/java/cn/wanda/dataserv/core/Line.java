package cn.wanda.dataserv.core;

import lombok.Data;

@Data
public class Line {
    private String line;

    public static final Line EOF = new Line();

    public Line() {
    }

    public Line(String s) {
        this.line = s;
    }
}
