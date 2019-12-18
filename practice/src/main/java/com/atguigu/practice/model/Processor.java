package com.atguigu.practice.model;

import com.atguigu.practice.factory.OperatorFactory;
import com.atguigu.practice.store.Operator;
import com.atguigu.practice.transformer.Transformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Processor {

    private final Logger LOGGER = LoggerFactory.getLogger(Processor.class);

    private Transformer transformer;

    private Operator operator;

    private List<TimeDimension> timeDimensions;

    public Processor(String transformerClassName, String operator, String[] timeDimensions) throws ServiceException{

        try {
            this.transformer = (Transformer) Class.forName(transformerClassName).newInstance();

        } catch (Exception e) {
            LOGGER.error(ErrorCode.ERRORCODE_TRANSFORMER_CLASS_NOT_FOUND.getMsg(), e);

            throw new ServiceException(ErrorCode.ERRORCODE_TRANSFORMER_CLASS_NOT_FOUND);
        }

        this.operator = OperatorFactory.getOperator(operator);

        if(timeDimensions != null) {
            this.timeDimensions = new ArrayList<>();
            for(String time : timeDimensions) {
                this.timeDimensions.add(TimeDimension.getFromSCode(time));
            }
        }
    }

    public Transformer getTransformer() {
        return transformer;
    }

    public Operator getOperator() {
        return operator;
    }

    public List<TimeDimension> getTimeDimensions() {
        return timeDimensions;
    }

    @Override
    public String toString() {
        return "Processor:{transformer:" + transformer.getClass().getName()
                + ", operator: " + operator.getClass().getName()
                + ", timeDimensions=" + timeDimensions.toString() + "}";
    }


}