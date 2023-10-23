package com.emma.netty.codec.pojo;

import lombok.Data;

import java.io.Serializable;

@Data
public class SubscribeReq implements Serializable {
    /**
     * default serialize id
     */
    private static final long serialiVersionUID = 1L;

    private int subReqID;

    private String userName;

    private String productName;

    private String phoneNumber;

    private String address;

    @Override
    public String toString() {
        return "SubscribeReq [subReqID=" + subReqID + ", userName=" + userName
                + ", productName=" + productName + ", phoneNumber="
                + phoneNumber + ", address=" + address + "]";
    }
}
