package cn.cug.dga.ds.service;

import cn.cug.dga.ds.bean.TDsTaskInstance;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;
import java.util.Set;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author song
 * @since 2024-03-02
 */
public interface TDsTaskInstanceService extends IService<TDsTaskInstance> {

    //需要封装出所有的带有sql语句字段的tdstaskinstance 需要的参数应该是当前的考评时间 以及库名和表名也是有要求的
    List<TDsTaskInstance> getAllTdsTaskInstance(String assessDate, Set<String> schemaName_tableName);

}
