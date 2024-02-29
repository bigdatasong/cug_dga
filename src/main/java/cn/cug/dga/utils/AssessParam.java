package cn.cug.dga.utils;

import cn.cug.dga.assess.bean.GovernanceMetric;
import cn.cug.dga.meta.bean.TableMetaInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * author song
 * date 2024/2/28 15:07
 * Desc 定义考评参数
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class AssessParam {

    // tablemetainfo
    private TableMetaInfo tableMetaInfo;

    // 指标信息
    private GovernanceMetric governanceMetric;

    //考评信息
    private  String assessDate;
}
