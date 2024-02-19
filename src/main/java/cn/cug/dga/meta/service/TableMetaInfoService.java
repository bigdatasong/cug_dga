package cn.cug.dga.meta.service;

import cn.cug.dga.meta.bean.TableMetaInfo;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 * 元数据表 服务类
 * </p>
 *
 * @author song
 * @since 2024-02-07
 */
public interface TableMetaInfoService extends IService<TableMetaInfo> {

    void initMetaInfoTables(String schemaName, String assessDate) throws Exception;
}
