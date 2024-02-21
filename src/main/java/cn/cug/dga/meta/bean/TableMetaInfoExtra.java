package cn.cug.dga.meta.bean;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import java.io.Serializable;
import java.sql.Timestamp;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * <p>
 * 元数据表附加信息
 * </p>
 *
 * @author song
 * @since 2024-02-20
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@TableName("table_meta_info_extra")
public class TableMetaInfoExtra implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 表id
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * 表名
     */
    private String tableName;

    /**
     * 库名
     */
    private String schemaName;

    /**
     * 技术负责人   
     */
    private String tecOwnerUserName;

    /**
     * 业务负责人 
     */
    private String busiOwnerUserName;

    /**
     * 存储周期类型 
     */
    private String lifecycleType;

    /**
     * 生命周期(天) 
     */
    private Long lifecycleDays;

    /**
     * 安全级别  
     */
    private String securityLevel;

    /**
     * 数仓所在层级  (来源: 附加)
     */
    private String dwLevel;

    /**
     * 创建时间 (自动生成)
     */
    private Timestamp createTime;

    /**
     * 更新时间  (自动生成)
     */
    private Timestamp updateTime;
}
