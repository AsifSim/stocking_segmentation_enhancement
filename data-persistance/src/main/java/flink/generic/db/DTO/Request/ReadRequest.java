package flink.generic.db.DTO.Request;

import flink.generic.db.Model.Condition;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ReadRequest {
    private String tableName;
    private List<String> columns;
    private List<Condition> filters;
    private String db_name;
}
