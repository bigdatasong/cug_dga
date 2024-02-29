package cn.cug.dga.assess.assessor.spec;

import cn.cug.dga.assess.assessor.AssessTemplate;
import cn.cug.dga.assess.bean.GovernanceAssessDetail;
import cn.cug.dga.meta.bean.TableMetaInfo;
import cn.cug.dga.meta.bean.TableMetaInfoExtra;
import cn.cug.dga.utils.AssessParam;
import cn.cug.dga.utils.ColFiled;
import com.alibaba.fastjson.JSON;
import jodd.util.StringUtil;
import org.apache.calcite.rel.core.Collect;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.stream.Collectors;

/**
 * author song
 * date 2024/2/28 17:53
 * Desc 表字段是否有备注
 */
@Component("FIELD_COMMENT")
public class CheckColumnCommentAssessor extends AssessTemplate {


    @Override
    protected void assess(AssessParam assessParam, GovernanceAssessDetail assessDetail) {
        //首先就是获取表字段,在tablemetainfo中表字段在一个json字符串数组中
        /**
         * [{"comment":"SKU_ID","name":"id","type":"string"},{"comment":"SPU_ID","name":"spu_id","type":"string"},{"comment":"价格","name":"price","type":"decimal(16,2)"},{"comment":"SKU名称","name":"sku_name","type":"string"},{"comment":"SKU规格描述","name":"sku_desc","type":"string"},{"comment":"重量","name":"weight","type":"decimal(16,2)"},{"comment":"品牌ID","name":"tm_id","type":"string"},{"comment":"三级品类ID","name":"category3_id","type":"string"},{"comment":"默认显示图片地址","name":"sku_default_img","type":"string"},{"comment":"是否在售","name":"is_sale","type":"string"},{"comment":"创建时间","name":"create_time","type":"string"},{"comment":"修改时间","name":"operate_time","type":"string"}]
         */
        //里面的每一个元素都是json字符串 可以定义个类型来封装里面的元素
        TableMetaInfo tableMetaInfo = assessParam.getTableMetaInfo();
        TableMetaInfoExtra tableMetaInfoExtra = tableMetaInfo.getTableMetaInfoExtra();
        // 使用jsonparsearray封装
        String colNameJson = tableMetaInfo.getColNameJson();
        List<ColFiled> colFileds = JSON.parseArray(colNameJson, ColFiled.class);
        //有备注字段/所有字段 *10分
        //list里面存放的就是所有字段 现在需要集合操作得到没有备注的字段 集合操作都用streamapi
        //我们之所以要获取到没有备注的字段是因为我们一方面可以得到有备注的字段数以外就是
        //还需要将没有备注的字段得到 然后封装到comment中
        List<String> noCommentName = colFileds.stream().filter(
                        c -> StringUtil.isBlank(c.getComment())
                )
                .map(c -> c.getName())
                .collect(Collectors.toList());
        //接下来就是计算分数 那么默认是有10分的 只有当nocommentname的size不为空的时候 说明确实有没有备注的字段
        //此时才需要重新计算分数
        if (!noCommentName.isEmpty()){
            // divide除法中第二个参数为保留几位小数,第三位表示的是四舍五入
            //x10 也可以用小数点右移一位movePointRight
            BigDecimal score = BigDecimal.valueOf(colFileds.size() - noCommentName.size())
                    .divide(BigDecimal.valueOf(colFileds.size()), 2, RoundingMode.HALF_UP)
                    .movePointRight(1);

            // 得到分数后赋值
            assessScore(score,"部分没有表字段备注",JSON.toJSONString(noCommentName),false,null,assessDetail);


        }
    }
}
