package flink.generic.db.Model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Condition {
    private String column;
    private Object value;
    private String operator;
    private String logicalOperator;

    private static final Logger logger = LoggerFactory.getLogger(Condition.class);
    Condition(String col, String val, String op, String logicalOp){
        logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
        column=col;
        value=val;
        operator=op;
        logicalOperator=logicalOp;
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
    }
}
