package flink.generic.db.DTO.Request;

import flink.generic.db.Model.Condition;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UpdateRequest {
    private String tableName;
    private Map<String,Object> values;
    private List<Condition> conditions;
    private String db_name;
}
