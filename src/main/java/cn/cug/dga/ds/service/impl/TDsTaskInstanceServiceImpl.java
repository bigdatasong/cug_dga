package cn.cug.dga.ds.service.impl;

import cn.cug.dga.constant.MetaConstant;
import cn.cug.dga.ds.bean.TDsTaskInstance;
import cn.cug.dga.ds.mapper.TDsTaskInstanceMapper;
import cn.cug.dga.ds.service.TDsTaskInstanceService;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author song
 * @since 2024-03-02
 */
@Service
public class TDsTaskInstanceServiceImpl extends ServiceImpl<TDsTaskInstanceMapper, TDsTaskInstance> implements TDsTaskInstanceService {


    @Override
    public List<TDsTaskInstance> getAllTdsTaskInstance(String assessDate, Set<String> schemaName_tableName) {

        //调用mapper方法根据条件来查出所有的tdstaskinstance
        QueryWrapper<TDsTaskInstance> queryWrapper = new QueryWrapper<TDsTaskInstance>()
                .eq("date(start_time)", assessDate)
                // 然后name必须也是set集合中name
                .in("name", schemaName_tableName)
                //因为一个task可能会有多个实例 即会有失败的实例 所以我们要的应该是成功的实例
                .eq("state", MetaConstant.TASK_STATE_SUCCESS);

        List<TDsTaskInstance> tDsTaskInstances = list(queryWrapper);

        //此时是没有封装sql的需要 从task_param字段中找出sql字段
        tDsTaskInstances.stream().forEach(
                t -> {
                    String taskParams = t.getTaskParams();
                    String rawScript = JSON.parseObject(taskParams).getString("rawScript");
                    //上面就是sql字段 但是 sql字段属于其中的一部分 所以需要切割出来 在这定义一个方法来实现sql片段的抽取
                    String sql = extraSqlFromrawScript(rawScript);

                    //将抽取好的sql字段封装到每一个tdstaskinstance
                    t.setSql(sql);
                }
        );


        return tDsTaskInstances;
    }

    //在rawscript中抽取sql片段
    private String extraSqlFromrawScript(String rawScript) {

        /**
         *     "rawScript": "#!/bin/bash\n\nsql=\"\ninsert overwrite table gmall.ads_order_to_pay_interval_avg\nselect * from gmall.ads_order_to_pay_interval_avg\nunion\nselect\n    '${do_date}',\n    cast(avg(to_unix_timestamp(payment_time)-to_unix_timestamp(order_time)) as bigint)\nfrom gmall.dwd_trade_trade_flow_acc\nwhere dt in ('9999-12-31','${do_date}')\nand payment_date_id='${do_date}';\n\"\n\nhive -e \"$sql\"",
         */

        //对于sql的截取应该是需要明确起始位置以及终止位置
        //起始位置的话要么就是 set xxx =xxxx ，with xxx ，要么就是没有cte语句 就是直接就是insert xxxx
        //终止位置找的话就是从起始位置开始往后找 找到分号 要是没有分号 那么就是会有” 结尾

        int start = rawScript.indexOf("with"); //起始索引 该方法 如果说没有就会返回 -1
        
        if (start == -1){
            //那就找insert开始的位置
            start = rawScript.indexOf("insert");
        }
        //开始找结尾位置
        int end = rawScript.indexOf(";", start);
        if (end == -1){
            //没有的话就找”
            end = rawScript.indexOf("\"", start);
        }

        return rawScript.substring(start,end); //默认是左闭右开


    }
}
