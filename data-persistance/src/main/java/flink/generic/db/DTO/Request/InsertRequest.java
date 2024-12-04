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
public class InsertRequest {
    private String tableName;
    private List<Map<String,Object>> values;
    private String db_name;
}
