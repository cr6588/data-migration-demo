package com.sjdf.demo;

import lombok.Data;

import java.io.Serializable;

@Data
public class DirectTransferVo implements Serializable {
    private static final long serialVersionUID = 2051785871932596907L;
    private DbConVo in;
    private DbConVo out;
    private String table;
}
