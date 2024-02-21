package cn.cug.dga.meta.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * author song
 * date 2024/2/20 20:12
 * Desc 封装表元数据信息列表查询条件参数
 *
 *schemaName=g&tableName=g&dwLevel=ODS&pageSize=20&pageNo=1
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableMetaInfoForQuery {

    private Integer pageNo;

    private Integer pageSize;

    /**
     * 表名
     */
    private String tableName;

    /**
     * 库名
     */
    private String schemaName;

    /**
     * 数仓所在层级(ODSDWDDIMDWSADS) ( 来源: 附加)
     */
    private String dwLevel;

}
