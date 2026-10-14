package com.zongpicha.waterflink.model;

public enum WindowType {
    HOUR(1),
    DAY(2),
    THREE_DAY(3),
    WEEK(4);

    private final int code;

    WindowType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
