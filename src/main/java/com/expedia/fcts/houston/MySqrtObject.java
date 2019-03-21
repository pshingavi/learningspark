package com.expedia.fcts.houston;

import java.io.Serializable;

public class MySqrtObject implements Serializable {
    private Integer x;
    private Double sqrtX;

    public MySqrtObject(Integer x) {
        this.x = x;
        this.sqrtX = Math.sqrt(x);
    }

    @Override
    public String toString() {
        return "Integer: " + x +
                " Sqrt: " + sqrtX;
    }
}
