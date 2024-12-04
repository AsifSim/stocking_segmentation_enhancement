package com.spriced.workflow;

import com.spriced.workflow.Exceptions.BusinessLogicException;

import java.util.Map;

public interface BusinessLogic {
    Map<String,Object> execute(Map<String,Object> data) throws BusinessLogicException;
}
