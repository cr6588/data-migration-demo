package com.sjdf.demo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DbConVo implements Serializable {
    private static final long serialVersionUID = -558605692302597300L;
    String url;
    String username;
    String password;
}
