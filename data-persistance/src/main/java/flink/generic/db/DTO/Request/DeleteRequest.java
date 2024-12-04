package flink.generic.db.DTO.Request;

import flink.generic.db.Model.Condition;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DeleteRequest {
    private String tableName;
    private List<Condition> conditions;
    private String db_name;
}
