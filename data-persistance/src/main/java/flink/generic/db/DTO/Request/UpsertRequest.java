package flink.generic.db.DTO.Request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UpsertRequest {
    private String tableName;
    private List<Map<String, Object>> valuesList;
    private String uniqueKeyColumn;
    private String db_name;
}
