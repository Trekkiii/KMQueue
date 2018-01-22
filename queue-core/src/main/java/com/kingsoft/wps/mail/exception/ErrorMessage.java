package com.kingsoft.wps.mail.exception;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Created by 刘春龙 on 2017/6/12.
 */
@JsonInclude(value = JsonInclude.Include.NON_NULL)
public class ErrorMessage {
    private String errorCode;//错误码
    private String errorMsg;//错误描述

    public ErrorMessage() {
    }


    public String getErrorCode() {
        return errorCode;
    }

    public ErrorMessage setErrorCode(String errorCode) {
        this.errorCode = errorCode;
        return this;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public ErrorMessage setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
        return this;
    }
}
