package com.ngdata.wal.controller;

import com.ngdata.wal.model.Response;
import com.ngdata.wal.task.PeerRegisterTask;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * @author duchao
 */
@Slf4j
@RestController
@RequestMapping("/peer/operation")
public class PeerController {

    private final PeerRegisterTask peerRegisterTask;

    public PeerController(PeerRegisterTask peerRegisterTask) {
        this.peerRegisterTask = peerRegisterTask;
    }

    @PostMapping(value = "/add")
    public Response add(String subscriptionName) {
        try {
            peerRegisterTask.addPeer(subscriptionName);
        } catch (Exception e) {
            log.error("add peer error.", e);
            return Response.getInstance(Response.SYSTEM_SERVER_ERROR);
        }
        return Response.makeSuccess(null);
    }

    @PostMapping(value = "/remove")
    public Response remove(String subscriptionName) {
        try {
            peerRegisterTask.removePeer(subscriptionName);
        } catch (Exception e) {
            log.error("remove peer error.", e);
            return Response.getInstance(Response.SYSTEM_SERVER_ERROR);
        }
        return Response.makeSuccess(null);
    }

    @PostMapping(value = "/enable")
    public Response enable(String subscriptionName) {
        try {
            peerRegisterTask.enablePeer(subscriptionName);
        } catch (Exception e) {
            log.error("enable peer error.", e);
            return Response.getInstance(Response.SYSTEM_SERVER_ERROR);
        }
        return Response.makeSuccess(null);
    }

    @PostMapping(value = "/disable")
    public Response disable(String subscriptionName) {
        try {
            peerRegisterTask.disablePeer(subscriptionName);
        } catch (Exception e) {
            log.error("disable peer error.", e);
            return Response.getInstance(Response.SYSTEM_SERVER_ERROR);
        }
        return Response.makeSuccess(null);
    }


}