package cn.cug.dga.meta.service;

import cn.cug.dga.meta.bean.TableMetaInfoExtra;
import cn.cug.dga.meta.bean.TableMetaInfoForQuery;
import cn.cug.dga.meta.bean.TableMetaInfoPageVo;
import com.baomidou.mybatisplus.extension.service.IService;
import org.apache.hadoop.hive.metastore.api.MetaException;

import java.util.List;

/**
 * <p>
 * 元数据表附加信息 服务类
 * </p>
 *
 * @author song
 * @since 2024-02-20
 */
public interface TableMetaInfoExtraService extends IService<TableMetaInfoExtra> {

    //定义一个方法 实现初始化表的辅助信息 要注意就是应该是生成同一个库下的所有表的元数据的辅助信息，所以参数是库
    void initExtraMetaInfo(String db) throws MetaException;




}
