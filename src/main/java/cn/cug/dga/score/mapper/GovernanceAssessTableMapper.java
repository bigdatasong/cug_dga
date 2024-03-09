package cn.cug.dga.score.mapper;

import cn.cug.dga.score.bean.GovernanceAssessTable;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.w3c.dom.stylesheets.LinkStyle;

import java.util.List;

/**
 * <p>
 * 表治理考评情况 Mapper 接口
 * </p>
 *
 * @author song
 * @since 2024-03-09
 */
@Mapper
public interface GovernanceAssessTableMapper extends BaseMapper<GovernanceAssessTable> {

    @Select("select" +
            "       null id," +
            "       #{dt} assess_date," +
            "       table_name," +
            "       schema_name," +
            "       tec_owner," +
            "       avg(if(governance_type = 'SPEC',assess_score,null)) score_spec_avg," +
            "       avg(if(governance_type = 'STORAGE',assess_score,null)) score_storage_avg," +
            "       avg(if(governance_type = 'CALC',assess_score,null)) score_calc_avg," +
            "       avg(if(governance_type = 'QUALITY',assess_score,null)) score_quality_avg," +
            "       avg(if(governance_type = 'SECURITY',assess_score,null)) score_security_avg," +
            "       null score_on_type_weight," +
            "       count(if(assess_score < 10,assess_score,null)) problem_num," +
            "       now() create_time" +
            " from governance_assess_detail" +
            " where assess_date =  #{dt} " +
            " group by table_name,schema_name,tec_owner")
    //在这里使用注解的方式 来实现从governance_assess_detail来获取封装成governance_assess_table
    //在粘sql的时候要小心from、where 、group by的空格问题
    //这个计算的每个avg 如果说最终的结果是百分制的话 应该是要乘以10的
    List<GovernanceAssessTable> calScorePerTable(@Param("dt") String assessDate);

}
