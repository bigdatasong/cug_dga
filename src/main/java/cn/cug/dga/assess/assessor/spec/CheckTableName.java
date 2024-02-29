package cn.cug.dga.assess.assessor.spec;

import cn.cug.dga.assess.assessor.AssessTemplate;
import cn.cug.dga.assess.bean.GovernanceAssessDetail;
import cn.cug.dga.constant.MetaConstant;
import cn.cug.dga.utils.AssessParam;
import com.sun.org.apache.bcel.internal.generic.SWITCH;
import groovyjarjarantlr.MismatchedCharException;
import jodd.util.StringUtil;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * author song
 * date 2024/2/29 20:43
 * Desc 检查表名是否符合规范
 */
@Component("TABLE_NAME_STANDARD")
public class CheckTableName extends AssessTemplate {
    @Override
    protected void assess(AssessParam assessParam, GovernanceAssessDetail assessDetail) {
        // 首先肯定是需要获取到表名
        //另外要获取到层级, 因为每一层的规范不一样
        Long tableId = assessParam.getTableMetaInfo().getId();
        String tableName = assessParam.getTableMetaInfo().getTableName();
        String schemaName = assessParam.getTableMetaInfo().getSchemaName();
        String dwLevel = assessParam.getTableMetaInfo().getTableMetaInfoExtra().getDwLevel();

        //在层级中 unset层不需要通过正则表达式来判断 就能给出分数 即 0分 那么如果说没有层级也是0分
        //另外other 层也不需要判断就能给出分数 即5分
        if (dwLevel.equals(MetaConstant.DW_LEVEL_UNSET) || StringUtil.isBlank(dwLevel)){
            assessScore(BigDecimal.ZERO,"没有设置层级","",true,tableId.toString(),assessDetail);
        }else if (dwLevel.equals(MetaConstant.DW_LEVEL_OTHER)){
            assessScore(BigDecimal.valueOf(5),"未纳入分层","",true,tableId.toString(),assessDetail);

        }else if (schemaName.equals("gmall")){
            //只有当是gmall的库下的表才进行定义的gmall的正则表达式的判断
            //根据每一层来调用判断方法
            switch (dwLevel){
                case MetaConstant.DW_LEVEL_ODS:IsMatch(tableName,MetaConstant.GMALL_ODS_REGEX,tableId,assessDetail);
                case MetaConstant.DW_LEVEL_DIM:IsMatch(tableName,MetaConstant.GMALL_DIM_REGEX,tableId,assessDetail);
                case MetaConstant.DW_LEVEL_DWD:IsMatch(tableName,MetaConstant.GMALL_DWD_REGEX,tableId,assessDetail);
                case MetaConstant.DW_LEVEL_DWS:IsMatch(tableName,MetaConstant.GMALL_DWS_REGEX,tableId,assessDetail);
                case MetaConstant.DW_LEVEL_ADS:IsMatch(tableName,MetaConstant.GMALL_ADS_REGEX,tableId,assessDetail);
                case MetaConstant.DW_LEVEL_DM:IsMatch(tableName,MetaConstant.GMALL_DM_REGEX,tableId,assessDetail);
            }
        }


    }

    // 定义一个方法 根据转来的tablename ,正则表达式 来判断是否符合该正则表达式 如果不符合就赋值0分 符合的话因为前面默认值就是满分 就不需要改动
    // 所以该方法的参数有tableName 正则表达式 以及tableid 以及detail
    private void IsMatch(String tableName,String regex,Long tableId,GovernanceAssessDetail detail){
        //创建正则表达式对象
        Pattern compile = Pattern.compile(regex);
        Matcher matcher = compile.matcher(tableName);
        if (!matcher.matches()){
            // 如果不匹配 就需要改分 根据规则改成0分
            assessScore(BigDecimal.ZERO,"表名不符合规范","", true,tableId.toString(),detail);
        }
    }
}
