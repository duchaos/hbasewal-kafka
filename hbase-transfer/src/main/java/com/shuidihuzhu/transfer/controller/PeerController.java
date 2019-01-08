package com.shuidihuzhu.transfer.controller;

import com.shuidihuzhu.transfer.model.Response;
import com.shuidihuzhu.transfer.task.PeerRegisterTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@Controller
@RequestMapping("/api/hbase-transfer/peer")
public class PeerController {
    private Logger logger = LoggerFactory.getLogger(PeerController.class);

    @Autowired
    private PeerRegisterTask peerRegisterTask;

    @RequestMapping(value="/add", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody Response add(){
        try{
            peerRegisterTask.addPeer();
        }catch (Exception e){
            logger.error("add peer error.",e);
            return Response.getInstance(Response.SYSTEM_SERVER_ERROR);
        }
        return Response.makeSuccess(null);
    }

    @RequestMapping(value="/remove", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody Response remove(){
        try{
            peerRegisterTask.removePeer();
        }catch (Exception e){
            logger.error("add peer error.",e);
            return Response.getInstance(Response.SYSTEM_SERVER_ERROR);
        }
        return Response.makeSuccess(null);
    }

}