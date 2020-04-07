package com.haoxin;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/4/3 18:13
 */
@Controller
@RequestMapping("DsInfoSJservice")
public class DsInfoSJservice {

    @RequestMapping(value="webInfoSJService")
    public void webInfoSJService(@RequestBody String jsonstr, HttpServletRequest request, HttpServletResponse response){

        System.out.println("hello Jin来了");
    }
}
