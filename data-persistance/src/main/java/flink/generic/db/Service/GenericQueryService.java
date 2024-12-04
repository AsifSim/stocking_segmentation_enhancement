package flink.generic.db.Service;

import flink.generic.db.Configuration.DynamicDataSource;
import flink.generic.db.Model.Condition;
import org.jooq.*;
import org.jooq.Record;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class GenericQueryService {

    private static final Logger logger = LoggerFactory.getLogger(GenericQueryService.class);
    private final DSLContext dsl;
    public GenericQueryService(DSLContext dsl) {
        this.dsl = dsl;
    }
    
    public List<Map<String, Object>> read(String entityName, List<String> columns, List<Condition> conditions, String db, Integer... limit) {
        logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
        logger.info("Parameter(entityName = {}, columns = {}, conditions = {}, db = {}, limit = {})",entityName,columns.toString(),conditions.toString(),db,limit);
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return read(entityName, columns, conditions, db, null, null, limit);
    }

    public List<Map<String, Object>> read(String entityName, List<String> columns, List<Condition> conditions, String db, String orderByColumn, Boolean isDesc, Integer... limit)
    {
        logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
        logger.info("Parameter(entityName = {}, columns = {}, conditions = {}, db = {}, limit = {})",entityName,columns.toString(),conditions.toString(),db,limit);
        DynamicDataSource.setDataSourceKey(db);
        Table<?> table = DSL.table(DSL.unquotedName(entityName));
        SelectJoinStep<Record> query = getQuery(table,columns,conditions,orderByColumn,isDesc,limit);
        logger.info("query = {}",query.toString());
        Result<Record> result = query.fetch();
        logger.info("Going to clear the dynamic data source");
        DynamicDataSource.clearDataSourceKey();
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return result.stream().map(Record::intoMap).collect(Collectors.toList());
    }

    public SelectJoinStep<Record> getQuery(Table<?> table, List<String> columns, List<Condition> conditions, String orderByColumn, Boolean isDesc, Integer... limit){
        logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
        logger.info("Parameter(table = {}, columns = {}, conditions = {}, orderByColumn = {}, isDesc = {}, limit = {})",table,columns.toString(),conditions.toString(),orderByColumn,isDesc,limit);
        SelectJoinStep<Record> query;
        if (columns.isEmpty()) {
            query = (SelectJoinStep<Record>) dsl.selectFrom(table);
            logger.info("query without filter = {}",query.toString());}
        else {
            List<Field<?>> fields = columns.stream().map(DSL::field).collect(Collectors.toList());
            query = dsl.select(fields).from(table);
            logger.info("query with filtered attributes = {}",query.toString());}
        logger.info("query with where filters = {}",queryWithLimitAndOrder(query,conditions,orderByColumn,isDesc,limit));
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return query;
    }

    public SelectJoinStep<Record> queryWithLimitAndOrder(SelectJoinStep<Record> queryWithoutFilter, List<Condition> conditions, String orderByColumn, Boolean isDesc, Integer... limit){
        logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
        logger.info("Parameter(queryWithoutFilter = {}, conditions = {}, orderByColumn = {}, isDesc = {}, limit = {})",queryWithoutFilter,conditions.toString(),orderByColumn,isDesc,limit);
        SelectJoinStep<Record> query = (SelectJoinStep<Record>) queryWithoutFilter.where(combineCondition(conditions));
        if (orderByColumn != null && !orderByColumn.trim().isEmpty()) {
            Field<?> orderField = DSL.field(orderByColumn);
            query = Boolean.TRUE.equals(isDesc) ? (SelectJoinStep<Record>)query.orderBy(orderField.desc()) : (SelectJoinStep<Record>)query.orderBy(orderField.asc());
            logger.info("query with order by clause = {}",query.toString());
        }
        if (limit != null && limit.length > 0 && limit[0] != null && limit[0] > 0) {
            query = (SelectJoinStep<Record>) query.limit(limit[0]);
            logger.info("query with limit = {}",query.toString());
        }
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return query;
    }


    public int insert(String entityName, List<Map<String, Object>> valuesList, String db) {
        logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
        logger.info("Parameter(entityName = {}, valuesList = {}, db = {})",entityName,valuesList.toString(),db);
        DynamicDataSource.setDataSourceKey(db);
        Table<?> table = DSL.table(DSL.name(entityName));
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return dsl.batch(valuesList.stream().map(values -> dsl.insertInto(table).set(values)).toArray(org.jooq.Query[]::new)).execute().length;
    }

    public int update(String entityName, Map<String, Object> values, List<Condition> conditions, String db) {
        logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
        logger.info("Parameter(entityName = {}, values = {}, conditions = {},db = {})",entityName,values.toString(),conditions.toString(),db);
        DynamicDataSource.setDataSourceKey(db);
        Table<?> table = DSL.table(DSL.name(entityName));
        org.jooq.Condition combinedCondition =combineCondition(conditions);
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return dsl.update(table).set(values).where(combinedCondition).execute();
    }

    public int upsert(String entityName, List<Map<String, Object>> valuesList, String uniqueKeyColumn, String db) {
        logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
        logger.info("Parameter(entityName = {}, valuesList = {}, uniqueKeyColumn = {}, db = {})",entityName,valuesList.toString(),uniqueKeyColumn,db);
        DynamicDataSource.setDataSourceKey(db);
        Table<?> table = DSL.table(DSL.name(entityName));
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return dsl.batch(valuesList.stream().map(values -> dsl.insertInto(table).set(values).onConflict(DSL.field(uniqueKeyColumn)).doUpdate().set(values)).toArray(org.jooq.Query[]::new)).execute().length;
    }


    public int softDelete(String entityName, List<Condition> conditions, String db) {
        logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
        logger.info("Parameter(entityName = {}, conditions = {},db = {})",entityName,conditions.toString(),db);
        DynamicDataSource.setDataSourceKey(db);
        Table<?> table = DSL.table(DSL.name(entityName));
        org.jooq.Condition combinedCondition =combineCondition(conditions);
        combinedCondition = combinedCondition.and(DSL.field("deleted_at").isNull());
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return dsl.update(table).set(DSL.field("deleted_at"), DSL.currentTimestamp()).where(combinedCondition).execute();
    }

    public org.jooq.Condition combineCondition(List<Condition> conditions){
        logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
        logger.info("Parameter(conditions = {})",conditions.toString());
        if (conditions.isEmpty()) {
            return DSL.trueCondition();
        }
        org.jooq.Condition combinedCondition = conditions.stream()
                .map(condition -> {
                    switch (condition.getOperator()) {
                        case "=": return DSL.field(condition.getColumn()).eq(condition.getValue());
                        case "LIKE": return DSL.field(condition.getColumn()).like((String) condition.getValue());
                        case ">": return DSL.field(condition.getColumn()).gt(condition.getValue());
                        case "<": return DSL.field(condition.getColumn()).lt(condition.getValue());
                        case ">=": return DSL.field(condition.getColumn()).ge(condition.getValue());
                        case "<=": return DSL.field(condition.getColumn()).le(condition.getValue());
                        case "!=": return DSL.field(condition.getColumn()).ne(condition.getValue());
                        default: throw new IllegalArgumentException("Unsupported operator: " + condition.getOperator());
                    }
                })
                .reduce(DSL.trueCondition(), org.jooq.Condition::and);

        org.jooq.Condition jooqCondition = conditions.stream()
                .map(condition -> {
                    switch (condition.getLogicalOperator()) {
                        case "OR": return combinedCondition;
                        case "AND":
                        default: return combinedCondition;
                    }
                })
                .reduce(DSL.trueCondition(), org.jooq.Condition::and);
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return jooqCondition;
    }
}
