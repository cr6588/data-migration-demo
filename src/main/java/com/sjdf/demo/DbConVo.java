package com.sjdf.demo;

import lombok.Data;

import java.io.Serializable;

@Data
public class DbConVo implements Serializable {
    private static final long serialVersionUID = -558605692302597300L;
    String url;
    String username;
    String password;
}
