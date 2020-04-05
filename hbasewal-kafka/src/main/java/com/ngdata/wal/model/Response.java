package com.ngdata.wal.model;


import com.alibaba.fastjson.JSON;
import lombok.Data;

/**
 * 服务端返回给客户端的结果
 *
 * @author duchao
 */
@Data
public class Response implements java.io.Serializable {

    private static final long serialVersionUID = -7353532565482245417L;

    public static final int SIGNATURE_SUCCESS = 0;
    public static final int SIGNATURE_FAILED = -1;
    public static final int SIGNATURE_DEVICEID_FAILED = -2;
    public static final int SIGNATURE_SIGN_MISSED = -3;
    public static final int SIGNATURE_DEVICE_NOT_REGISTER = -4;
    public static final int SIGNATURE_DEVICE_NOT_BIND = -5;
    public static final int SYSTEM_PARAM_ERROR = -6;
    public static final int SYSTEM_ERROR_REQUEST = -7;
    public static final int SYSTEM_SERVER_ERROR = -500;

    public static Response getInstance(int code) {
        switch (code) {
            case SIGNATURE_SUCCESS:
                return new Response(code, "Signature: verification success");
            case SIGNATURE_FAILED:
                return new Response(code, "Signature: verification failed");
            case SIGNATURE_DEVICEID_FAILED:
                return new Response(code, "Signature: parameter deviceId not correct");
            case SIGNATURE_SIGN_MISSED:
                return new Response(code, "Signature: parameter sign missing");
            case SIGNATURE_DEVICE_NOT_REGISTER:
                return new Response(code, "Signature: this device does not register");
            case SIGNATURE_DEVICE_NOT_BIND:
                return new Response(code, "Signature: this device does not bind");
            case SYSTEM_PARAM_ERROR:
                return new Response(code, "参数错误");
            case SYSTEM_ERROR_REQUEST:
                return new Response(code, "非法请求");
            case SYSTEM_SERVER_ERROR:
                return new Response(code, "服务器异常");
            default:
                return new Response(SYSTEM_SERVER_ERROR, "服务器异常");
        }
    }


    public static Response makeSuccess(Object obj) {
        return new Response(0, "success", obj);
    }

    public Response() {

    }


    public Response(long ret, String msg) {
        this.ret = ret;
        this.msg = msg;
    }


    public Response(long ret, String msg, Object content) {
        this.ret = ret;
        this.msg = msg;
        this.content = content;
    }


    // 消息代码
    private long ret;

    // 消息
    private String msg;

    // 消息内容
    private Object content;


    public static String toJson(long ret, String msg, Object content) {
        Response result = new Response();
        result.setRet(ret);
        result.setMsg(msg);
        result.setContent(content);
        return JSON.toJSONString(result);
    }

}
