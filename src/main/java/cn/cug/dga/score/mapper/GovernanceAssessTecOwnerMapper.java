package cn.cug.dga.score.mapper;

import cn.cug.dga.score.bean.GovernanceAssessTecOwner;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * <p>
 * 技术负责人治理考评表 Mapper 接口
 * </p>
 *
 * @author song
 * @since 2024-03-09
 */
@Mapper
public interface GovernanceAssessTecOwnerMapper extends BaseMapper<GovernanceAssessTecOwner> {

    //编写一个方法 实现从GovernanceAssessTable中封装GovernanceAssessTecOwner
    @Select("select" +
            "       null id," +
            "       #{dt} assess_date," +
            "       tec_owner," +
            "       avg(score_spec_avg) score_spec," +
            "       avg(score_storage_avg) score_storage," +
            "       avg(score_calc_avg) score_calc," +
            "       avg(score_quality_avg) score_quality," +
            "       avg(score_security_avg) score_security," +
            "       avg(score_on_type_weight) score," +
            "       count(*) table_num," +
            "       sum(problem_num) problem_num," +
            "       now() create_time" +
            " from governance_assess_table" +
            " where assess_date = #{dt}" +
            " group by tec_owner,")
    List<GovernanceAssessTecOwner> calScoreByTecOwner(@Param("dt") String assessDate);

}
