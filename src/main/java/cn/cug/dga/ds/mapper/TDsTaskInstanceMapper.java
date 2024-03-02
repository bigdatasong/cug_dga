package cn.cug.dga.ds.mapper;

import cn.cug.dga.ds.bean.TDsTaskInstance;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author song
 * @since 2024-03-02
 */
@Mapper
@DS("ds")
public interface TDsTaskInstanceMapper extends BaseMapper<TDsTaskInstance> {

}
