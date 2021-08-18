package com.github.shoothzj.pf.consumer.action.module;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author hezhangjian
 */
@Data
@AllArgsConstructor
public class ActionMsg {

    private String content;

    public ActionMsg() {
    }
}
