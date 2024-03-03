package cn.cug.dga.assess.assessor.cal;

import cn.cug.dga.assess.assessor.AssessTemplate;
import cn.cug.dga.assess.bean.GovernanceAssessDetail;
import cn.cug.dga.constant.MetaConstant;
import cn.cug.dga.ds.bean.TDsTaskInstance;
import cn.cug.dga.meta.bean.TableMetaInfo;
import cn.cug.dga.utils.AssessParam;
import cn.cug.dga.utils.ColFiled;
import cn.cug.dga.utils.MetaUtil;
import cn.cug.dga.utils.SqlParser;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonObjectFormatVisitor;
import com.google.common.collect.Sets;
import lombok.Data;
import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.codehaus.groovy.reflection.stdclasses.BigDecimalCachedClass;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

/**
 * author song
 * date 2024/3/3 11:29
 * Desc 是否是简单加工
 */
@Component("SIMPLE_PROCESS")
public class CheckSimpleProcess extends AssessTemplate {

    @Autowired
    private MetaUtil metaUtil;


    @Override
    protected void assess(AssessParam assessParam, GovernanceAssessDetail assessDetail) throws Exception {
        /**
         * sql语句没有任何join\groupby\ 非分区字段的where过滤   ，以上情况给0分，其余给10分	任务信息
         *
         * 首先一些特殊表是没有sql语句的 比如 ods层的表 以及dim_date 日期表 一年只调度一次
         * 然后拿到sql语句之后 判断sql中是否有join等复杂操作 如果有就给10分 说明不是简单加工
         * 如果没有再去判断
         *       如果说 where过滤字段中没有非分区字段 这样才说明是简单加工
         *             实现步骤就是 先拿到where过滤的字段 注意这里的字段很有可能是不同表的字段都有
         *             然后拿到表的分区字段 要注意的就是说 这里应该拿到这个sql语句中涉及到的所有表的分区字段 因为上述where过滤的分区字段可能
         *             来自不同的表
         *             将两者进行比较 （就是相当于我获取到了这个sql语句中的所有分区字段以及where中的过滤字段）
         *                 如果说二者是相同的 就说明 没有分区字段 就说明是简单加工
         *                 如果二者有不同 就说明存在分区字段  就说明不是简单加工
         *     也就是说 简单加工的判断标准就是 没有join等复杂操作 并且过滤字段中只有非分区字段
         *
         *     对于这个需求 主要有两点 就是 怎么获取sql 以及拿到sql以后怎么去解析sql 从而拿到where过滤的字段 以及判断sql中是否有join等操作
         *
         */

        String dwLevel = assessParam.getTableMetaInfo().getTableMetaInfoExtra().getDwLevel();
        String tableName = assessParam.getTableMetaInfo().getTableName();
        if (dwLevel.equals(MetaConstant.DW_LEVEL_ODS) || "dim_date".equals(tableName) ){
            return ;
        }

        // 获取sql语句 sql语句在taskinstance中的task_param中 ，所以需要编写mapper方法 来获取表的sql 为了方便起见，我们同样在
        //utils工具中封装所有的表的sql数据 ，
        String schemaName = assessParam.getTableMetaInfo().getSchemaName();
        String key = schemaName + "." + tableName;
        TDsTaskInstance tDsTaskInstance = metaUtil.getTDsTaskInstanceMap().get(key);
        String sql = tDsTaskInstance.getSql();

        // 获取完sql以后我们需要通过sql解析器 来解析sql
        //调用工具类方法 需要一个dispathcer
        myDispathcer dispathcer = new myDispathcer();
        SqlParser.parseSql(sql,dispathcer);
        //将dispatcher中收集到的数据获取到
        Set<String> complexOperator = dispathcer.getComplexOperator();
        Set<String> whereFileds = dispathcer.getWhereFileds();
        Set<String> tableNames = dispathcer.getTableNames();

        //根据判断逻辑来操作
        if (complexOperator.isEmpty()){
            //说明没有复杂操作
            //接下来判断是否where过滤字段中是否有分区字段 我们通过dispather获取到了where过滤的字段 接下来就是
            //要根据获取到的表名 查询出这些表名对应的分区字段 封装成一个集合 然后和where过滤字段进行比较
            Set<String> tablePartitionFieldNames = new HashSet<>();  //存的是分区字段信息
            tableNames.stream().forEach(
                    //根据表名来获取分区字段细信息
                    t -> {
                        TableMetaInfo tableMetaInfo = metaUtil.getTableMetaInfoMap().get(schemaName + "." + t);
                        //但是因为t很有可能是表的别名 所以tablemetainfo可能是空值 所以可以进行非空判断
                        if (tableMetaInfo != null){

                            String partitionColNameJson = tableMetaInfo.getPartitionColNameJson();
                            List<ColFiled> colFileds = JSON.parseArray(partitionColNameJson, ColFiled.class);
                            //将字段信息 只取名称即可
                            Set<String> colName = colFileds.stream().map(c -> c.getName()).collect(Collectors.toSet());
                            tablePartitionFieldNames.addAll(colName); //addall方法可以将一个集合都加入 这样的话相当于批量加入
                        }

                    }
            );
            //这样的话tablePartitionFieldNames里面存放的就是表的各个分区字段
            //将该tablePartitionFieldNames和where过滤字段的集合 做差
            whereFileds.removeAll(tablePartitionFieldNames); //如果有差值就会将差值信息放到前面那个集合中
            if (whereFileds.isEmpty()){
                //说明没有差值 就是一模一样 那么此时说明就是简单sql
                assessScore(BigDecimal.ZERO,"简单查询","",false,null,assessDetail);
            }
        }

        //在这里 不管是简单查询也好 还是复杂查询也好 我们都可以将返回的三个信息都存入数据库中 虽然说简单查询没有复杂操作
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("complexOperator",complexOperator);
        jsonObject.put("whereFileds",whereFileds);
        jsonObject.put("tableNames",tableNames);

        assessDetail.setAssessComment(JSONObject.toJSONString(jsonObject));
    }

    //声明一个内部类类实现dispathcer 完成每一个节点遍历的逻辑
    @Data
    public static class myDispathcer implements Dispatcher{

        //在遍历的节点的过程中我们需要做的事就是 如果说遍历到复杂操作时（join）等 我们就要记录下来 方便后面根据这个复杂操作来判分
        //如果说不是复杂操作 就需要在遍历的时候把where过滤的字段获取到 记录这些过滤的字段
        //因为整个sql语句中可能会比较复杂 可能涉及到多个表的分区字段过滤 所以说我们在遍历的时候获取到这个sql涉及到的表名
        //有这些表名以后 我们就可以得到他的分区字段信息 然后上层在调用的时候就能更具这个表名得到分区字段信息 然后再和where过滤字段比较
        //看是否过滤字段中包含分区字段
        //综上来开 我们需要的有复杂操作的数据、where过滤字段的数据、sql中涉及到的表名字
        //所以在这里我们可以定义三个set集合用于收集这三种信息
        //另外方便起见 我们可以先定义好时复杂操作的信息以及又因为在遍历到where时 会出现两种情况 要么是到了比较运算符 我们下面还有一层是逻辑运算符
        //而只要到了比较运算符 就说明很快就能得到where过滤的字段了

        //因为将set设为了属性 在外层需要调用的话 就需要加data注解 来获取属性值
        //收集复杂操作
        private Set<String> complexOperator = new HashSet<>();
        //收集where过滤的字段
        private Set<String> whereFileds = new HashSet<>();
        //收集查询的表名
        private Set<String> tableNames = new HashSet<>();

        //把常见的所有的复杂查询的标识符，先列出来，方便比对
        Set<Integer> complexProcessSet= Sets.newHashSet(
                HiveParser.TOK_JOIN,  //join 包含通过where 连接的情况
                HiveParser.TOK_GROUPBY,       //  group by
                HiveParser.TOK_LEFTOUTERJOIN,       //  left join
                HiveParser.TOK_RIGHTOUTERJOIN,     //   right join
                HiveParser.TOK_FULLOUTERJOIN,     // full join
                HiveParser.TOK_FUNCTION,     //count(1)
                HiveParser.TOK_FUNCTIONDI,  //count(distinct xx)
                HiveParser.TOK_FUNCTIONSTAR, // count(*)
                HiveParser.TOK_SELECTDI,  // distinct
                HiveParser.TOK_UNIONALL   // union
        );

        //为了方便判断当前节点是不是比较运算符
        Set<String> operators= Sets.newHashSet("=",">","<",">=","<=" ,"<>"  ,"like","not like"); // in / not in 属于函数计算

        @Override
        public Object dispatch(Node node, Stack<Node> stack, Object... objects) throws SemanticException {

            //获取当前节点的子节点来判断 在判断之前先强转类型方便调用方法
            ASTNode currentNode = (ASTNode) node;
            //判断当前节点是否是复杂操作类型
            // gettype 和getname 值都是一样，值都是表示标识的意思。只不过类型不一样  type 是返回int name是返回string
            //在语法树中 会为每个节点定义一个标识 数字标识
            // HiveParser.xxx 可以得到xx的类型的数字标识 标识为int类型
            //如果说当前节点标识在复杂集合中存在 就说名当前节点是复杂标识
            if (complexProcessSet.contains(currentNode.getType())){
                //将当前复杂标识记录下来
                complexOperator.add(currentNode.getText()); //gettext就能获取到当前节点的名字例如tok_xxxx
            }
            //如果当前节点是where过滤节点 我们需要记录到where过滤的字段
            if (currentNode.getType() == HiveParser.TOK_WHERE){
                //需要抽取where 过滤字段 定义方法来实现
                extraWhereFiled(currentNode);
            }

            //如果当前时表节点 就需要记录表节点下的表名称
            if (currentNode.getType() == HiveParser.TOK_TABNAME){
                //获取表节点下的子节点 如果有两个子节点 说明 时库名.表名格式 就取右边的孩子的名称即可
                ArrayList<Node> children = currentNode.getChildren();
                if (children.size() == 2 ){
                    ASTNode child = (ASTNode)currentNode.getChild(1);
                    tableNames.add(child.getName());
                }else {
                    ASTNode child = (ASTNode)currentNode.getChild(0);
                    tableNames.add(child.getName());
                }
            }

            return null;
        }

        //抽取where过滤字段
        private void extraWhereFiled(ASTNode currentNode) {
            // 在tok_where节点上的子节点会有两种情况 要么就是比较运算符 要么就是逻辑运算符 ，如果是逻辑运算符
            //逻辑运算符的下面肯定是比较运算符 ，如果不是 就需要再次递归遍历子节点 直到当前节点是比较运算符
            //然后比较运算符下的时候 要想找到字段名 也会有两种情况 因为有些字段名前面可能会有表别名 如果往下是 。就说明是有表别名的 此时只要取 。的右边节点内容就是字段名
            //如果说往下不是。 那么就说明下面是tok_table_or_col 那么此时只需要取左边的就是字段名 右边的是字段值

            //先获取当前where节点下的子节点
            ArrayList<Node> children = currentNode.getChildren();

            //因为后面涉及到递归遍历 当遍历到的节点为null 或者为空时 就需要停止递归
            //因为我这个方法一上来就是先获取子节点 如果获取不到 说明已经到最后一层了就直接返回即可
            if (children == null || children.isEmpty()){
                return ;
            }

            //对子节点进行遍历
            for (Node child : children) {
                //将node 转为astnode 更方便一点
                ASTNode astNode = (ASTNode) child;
                //如果是比较运算符
                if (operators.contains(astNode.getName())){
                    //判断他的子节点是由有 。
                    ArrayList<Node> children1 = astNode.getChildren();
                    for (Node node : children1) {
                        ASTNode node1 = (ASTNode) node;
                        //如果是。 就取右边的就行
                        if (node1.getType() == HiveParser.DOT){
                            ASTNode child1 = (ASTNode)node1.getChild(1);
                            whereFileds.add(child1.getName());
                        }else if (node1.getType() == HiveParser.TOK_TABLE_OR_COL){
                            //说明不是。而是TOK_TABLE_OR_COL 那就取左边的值就行
                            ASTNode child1 = (ASTNode)node1.getChild(0);
                            whereFileds.add(child1.getName());
                        }

                    }

                }else {
                    //如果不是比较运算符 就需要将子节点往下传进行递归遍历
                    extraWhereFiled(astNode);
                }
            }


        }
    }
}
