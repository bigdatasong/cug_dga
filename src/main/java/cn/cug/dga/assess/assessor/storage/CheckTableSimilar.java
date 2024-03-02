package cn.cug.dga.assess.assessor.storage;

import cn.cug.dga.assess.assessor.AssessTemplate;
import cn.cug.dga.assess.bean.GovernanceAssessDetail;
import cn.cug.dga.meta.bean.TableMetaInfo;
import cn.cug.dga.utils.AssessParam;
import cn.cug.dga.utils.ColFiled;
import cn.cug.dga.utils.MetaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * author song
 * date 2024/3/1 15:18
 * Desc 判断是否是相似表 同层次两个表字段重复超过{percent}%，则给0分，其余给10分
 */

public class CheckTableSimilar extends AssessTemplate {

    @Autowired
    private MetaUtil metaUtil;

    @Override
    protected void assess(AssessParam assessParam, GovernanceAssessDetail assessDetail) {
        // 何为相似 即表字段加字段注释超过某一个哔哩 就表示相似 一旦相似 就给0分 否则就10分
        //首先获取这个哔哩
        //其次 因为要和同一层次的表进行相比 所以说需要获取到其他表的元数据 但是我们目前传递过来的数据就只要自己本身的表数据
        //不妨定义一个工具类 将查询出来的所有表的元数据封装成一个特定的map ,map 的key可以用库名加表名来唯一标识,value就是tablemetainfo
        //那为啥不用其他集合呢,如果直接用set,就是说明set里面都是tablemetainfo 其实也是可以的 只不过呢 为了方便库名加表名的对比,所以封装
        //成一个map
        Integer percent = JSON.parseObject(assessParam.getGovernanceMetric().getMetricParamsJson()).getInteger("percent");

        Map<String, TableMetaInfo> tableMetaInfoMap = metaUtil.getTableMetaInfoMap();
        //获取当前表的层级以及当前表的表名加库名组成的key
        String tableName = assessParam.getTableMetaInfo().getTableName();
        String schemaName = assessParam.getTableMetaInfo().getSchemaName();
        String currentKey = schemaName  + '.' + tableName;
        String currentDwLevel = assessParam.getTableMetaInfo().getTableMetaInfoExtra().getDwLevel();

        // 接下来就是如何考虑对map进行遍历,对map的遍历方式 可以使用将其转为entryset 这样的就是一个个set set里面是一个一个的map而已
        //也可以使用foreach的方式来处理里面每一个元素
        List<JSONObject> jsonObjects = new ArrayList<>();
        tableMetaInfoMap.forEach(
                (k,v) ->{
                    //首先就是要获取到同层并且不是自己的表
                    if (!currentKey.equals(k) && v.getTableMetaInfoExtra().getDwLevel().equals(currentDwLevel)){
                        //开始字段比较
                        // 字段比较就是将当前的表以及遍历的满足的条件的没一张表来进行 表字段+表备注来比较
                        //因为一张表的字段和备注都有很多 并且字段和备注是一一对应的
                        // 可以定义一个方法 将表字段和表备注按照指定格式封装起来,又因为一张表有很多字段加备注 所以将表加字段的特定格式转为一个
                        //set集合 通过set集合之间来比较不同表中 表+字段
                        // 获取当前表的字段和备注集合 以及待比较的表的表字段备注集合
                        Set<String> currentTableColSet = extraFieldNames(assessParam.getTableMetaInfo().getColNameJson());
                        Set<String> waitTableColSet = extraFieldNames(v.getColNameJson());

                        // 相似的定义就是说表加备注的个数/总的个数的哔哩不能超过某个值
                        //集合之间可以通过retainall方法 来判断是否有交集,返回true说明有交集 并且将交集放在前一个集合中
                        //为了更加清晰 一旦超过了这个阈值,就说明和其他表有相似 那么因为当前表要和所有表比,所以可能会和多个表相似
                        //可以定义一个集合 这个集合对于每一个表都要用到 集合里面存放当前表和其他表相似的信息,集合元素用一个json来标识
                        //json中会有当前相似表的各个信息 比如记录当前相似表的表名,当前的相似比例 以及当前表的表字段加备注信息
                        if (currentTableColSet.retainAll(waitTableColSet)){
                            //说明有交集 就需要计算分数
                            BigDecimal currentPercent = BigDecimal.valueOf(currentTableColSet.size())
                                    .divide(BigDecimal.valueOf(currentTableColSet.size()), 2, RoundingMode.HALF_UP)
                                    .movePointRight(2);

                            // 判断是否超出哔哩
                            if (currentPercent.compareTo(BigDecimal.valueOf(percent)) == 1){
                                // compareto方法 如果为-1 说明前面的值小 如果为0 说明二者相等 如果为1 说明前面的更大
                                //说明超出哔哩 此时可以将当前相似表的各个信息 比如记录当前相似表的表名,当前的相似比例 以及当前表的表字段加备注信息
                                //记录下来 记载一个json上 然后加到一个list集中 方便在赋值分的时候给出具体信息
                                JSONObject jsonObject = new JSONObject();
                                jsonObject.put("compareTable",k);
                                jsonObject.put("percent",currentPercent);
                                jsonObject.put("currentTableColSet",currentTableColSet);

                                jsonObjects.add(jsonObject);
                            }

                        }
                    }
                }

        );

        // 经过这个for循环以后 来进行判分
        //如果说这个集合中有值 就说明肯定相似
        if (!jsonObjects.isEmpty()){
            //
            assessScore(BigDecimal.ZERO,"和其他表有相似字段",JSON.toJSONString(jsonObjects),false,null,assessDetail);
        }


    }

    //定义一个方法 实现将表字段加表备注 封装成集合 参数直接可以是tablemetainfo中的colnamejson
    private Set<String> extraFieldNames(String colNameJson){
        List<ColFiled> colFileds = JSON.parseArray(colNameJson, ColFiled.class);

        // 集合操作尽量用stream操作
        Set<String> collect = colFileds.stream().map(c -> c.getName() + "_" + c.getComment())
                .collect(Collectors.toSet());

        return  collect;
    }
}
