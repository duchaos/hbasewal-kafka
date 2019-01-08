package com.shuidihuzhu.transfer.controller;

import com.shuidihuzhu.transfer.model.Config;
import com.shuidihuzhu.transfer.model.Response;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@Controller
@RequestMapping("/api/hbase-transfer/config")
public class ConfigController {
    @RequestMapping(value="/kafka", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody Response kafka(String flag){
        if(flag.equals("true")){
            Config.openKafka=true;
        }else if(flag.equals("false")){
            Config.openKafka=false;
        }
        return Response.makeSuccess(null);
    }

    @RequestMapping(value="/es", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody Response es(String flag){
        if(flag.equals("true")){
            Config.openEs=true;
        }else if(flag.equals("false")){
            Config.openEs=false;
        }
        return Response.makeSuccess(null);
    }

}